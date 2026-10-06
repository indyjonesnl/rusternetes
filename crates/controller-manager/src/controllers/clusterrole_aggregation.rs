//! ClusterRole aggregation controller.
//!
//! Port of `pkg/controller/clusterroleaggregation/clusterroleaggregation_controller.go`
//! (release-1.35): a ClusterRole carrying an `aggregationRule` has its `rules`
//! rebuilt as the union of the rules of every other ClusterRole whose labels
//! match any of the rule's `clusterRoleSelectors`.
//!
//! Mechanism, as upstream:
//! - any ClusterRole add/update/delete enqueues *every* ClusterRole that has an
//!   `aggregationRule` (`enqueue`, :195-215: "since the set of all clusterroles
//!   is small and we don't know the dependency graph, just queue up every thing
//!   each time"), so a change to a matching child re-aggregates its parent;
//! - `syncClusterRole` (:74-121) walks the selectors in order, lists the
//!   matching ClusterRoles sorted by name, skips the parent itself, and appends
//!   each rule not already present (`ruleExists`, :158-165);
//! - an invalid selector fails the sync (:104-107) and is retried with backoff;
//! - the write is skipped when the result equals the current rules (:118-120);
//! - `Run` starts 5 workers (`cmd/kube-controller-manager/app/rbac.go`).
//!
//! Deviations (Rusternetes-only): the apply (`ClusterRoles().Apply` with field
//! manager `clusterrole-aggregation-controller`, :123-132) is a direct storage
//! `update` carrying the resourceVersion read in the sync, i.e. upstream's own
//! `updateClusterRoles` fallback (:134-142), because controllers here write the
//! store directly. A 5s periodic resync stands in for the informer's resync.

use futures::StreamExt;
use rusternetes_common::resources::{ClusterRole, PolicyRule};
use rusternetes_common::types::label_selector_as_selector;
use rusternetes_common::Result;
use rusternetes_storage::{build_key, build_prefix, Storage, WorkQueue};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Number of sync workers (`Run(ctx, 5)` in
/// `cmd/kube-controller-manager/app/rbac.go`).
const WORKERS: usize = 5;

/// Periodic resync, so a missed watch event is still repaired.
const RESYNC: Duration = Duration::from_secs(5);

pub struct ClusterRoleAggregationController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> ClusterRoleAggregationController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    /// `Run`: start the workers, then enqueue on every ClusterRole event.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting ClusterRoleAggregator controller");
        let queue = WorkQueue::new();
        for _ in 0..WORKERS {
            let worker = Arc::clone(&self);
            let queue = queue.clone();
            tokio::spawn(async move { worker.worker(queue).await });
        }

        let prefix = build_prefix("clusterroles", None);
        loop {
            self.enqueue(&queue).await;
            let mut watch = match self.storage.watch(&prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!("ClusterRoleAggregator: watch failed: {}, retrying", e);
                    tokio::time::sleep(RESYNC).await;
                    continue;
                }
            };
            let mut resync = tokio::time::interval(RESYNC);
            resync.tick().await;
            loop {
                tokio::select! {
                    event = watch.next() => match event {
                        // Add/Update/Delete all do the same thing upstream.
                        Some(Ok(_)) => self.enqueue(&queue).await,
                        Some(Err(e)) => {
                            warn!("ClusterRoleAggregator: watch error: {}, reconnecting", e);
                            break;
                        }
                        None => break,
                    },
                    _ = resync.tick() => self.enqueue(&queue).await,
                }
            }
        }
    }

    /// `enqueue` (:195-215): queue every ClusterRole that has an
    /// `aggregationRule`.
    async fn enqueue(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<ClusterRole>(&build_prefix("clusterroles", None))
            .await
        {
            Ok(all) => {
                for role in all.iter().filter(|r| r.aggregation_rule.is_some()) {
                    queue.add(role.metadata.name.clone()).await;
                }
            }
            Err(e) => error!("ClusterRoleAggregator: couldn't list all objects: {}", e),
        }
    }

    /// `runWorker` / `processNextWorkItem` (:177-193): a failed sync is
    /// requeued with rate limiting, a successful one forgotten.
    async fn worker(&self, queue: WorkQueue) {
        while let Some(name) = queue.get().await {
            match self.sync_cluster_role(&name).await {
                Ok(()) => queue.forget(&name).await,
                Err(e) => {
                    error!("ClusterRoleAggregator: {} failed with: {}", name, e);
                    queue.requeue_rate_limited(name.clone()).await;
                }
            }
            queue.done(&name).await;
        }
    }

    /// Sync every ClusterRole that has an `aggregationRule` once, in order.
    /// Used by tests and one-shot callers; the controller itself goes through
    /// the queue.
    pub async fn sync_all(&self) -> Result<()> {
        let all: Vec<ClusterRole> = self
            .storage
            .list(&build_prefix("clusterroles", None))
            .await?;
        for role in all.iter().filter(|r| r.aggregation_rule.is_some()) {
            self.sync_cluster_role(&role.metadata.name).await?;
        }
        Ok(())
    }

    /// `syncClusterRole` (:74-121).
    pub async fn sync_cluster_role(&self, name: &str) -> Result<()> {
        let key = build_key("clusterroles", None, name);
        let shared: ClusterRole = match self.storage.get(&key).await {
            Ok(r) => r,
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e),
        };
        let Some(aggregation_rule) = &shared.aggregation_rule else {
            return Ok(());
        };

        let all: Vec<ClusterRole> = self
            .storage
            .list(&build_prefix("clusterroles", None))
            .await?;

        let mut new_rules: Vec<PolicyRule> = Vec::new();
        for selector in aggregation_rule.cluster_role_selectors.iter().flatten() {
            let selector = label_selector_as_selector(Some(selector))
                .map_err(rusternetes_common::Error::InvalidResource)?;
            let mut matching: Vec<&ClusterRole> = all
                .iter()
                .filter(|r| selector.matches(r.metadata.labels.as_ref()))
                .collect();
            matching.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
            for role in matching {
                if role.metadata.name == shared.metadata.name {
                    continue;
                }
                for rule in &role.rules {
                    if !rule_exists(&new_rules, rule) {
                        new_rules.push(rule.clone());
                    }
                }
            }
        }

        if rules_equal(&new_rules, &shared.rules) {
            return Ok(());
        }

        let mut updated = shared.clone();
        updated.rules = new_rules;
        self.storage.update(&key, &updated).await?;
        debug!("ClusterRoleAggregator: updated rules of {}", name);
        Ok(())
    }
}

/// `ruleExists` (:158-165).
fn rule_exists(haystack: &[PolicyRule], needle: &PolicyRule) -> bool {
    haystack.iter().any(|curr| rule_equal(curr, needle))
}

fn rules_equal(a: &[PolicyRule], b: &[PolicyRule]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| rule_equal(x, y))
}

/// `equality.Semantic.DeepEqual` on a `PolicyRule`: a nil slice and an empty
/// slice are equal, which `Option<Vec<_>>` equality is not.
fn rule_equal(a: &PolicyRule, b: &PolicyRule) -> bool {
    fn same(a: &Option<Vec<String>>, b: &Option<Vec<String>>) -> bool {
        a.as_deref().unwrap_or_default() == b.as_deref().unwrap_or_default()
    }
    a.verbs == b.verbs
        && same(&a.api_groups, &b.api_groups)
        && same(&a.resources, &b.resources)
        && same(&a.resource_names, &b.resource_names)
        && same(&a.non_resource_urls, &b.non_resource_urls)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::rbac::AggregationRule;
    use rusternetes_common::types::{
        LabelSelector, LabelSelectorRequirement, ObjectMeta, TypeMeta,
    };
    use rusternetes_storage::MemoryStorage;
    use std::collections::HashMap;

    fn rule(verbs: &[&str], resources: &[&str]) -> PolicyRule {
        PolicyRule::new(verbs.iter().map(|s| s.to_string()).collect())
            .with_api_groups(vec!["".into()])
            .with_resources(resources.iter().map(|s| s.to_string()).collect())
    }

    fn role(name: &str, labels: &[(&str, &str)], rules: Vec<PolicyRule>) -> ClusterRole {
        ClusterRole {
            type_meta: TypeMeta {
                kind: "ClusterRole".into(),
                api_version: "rbac.authorization.k8s.io/v1".into(),
            },
            metadata: ObjectMeta {
                name: name.into(),
                labels: Some(
                    labels
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                ),
                ..Default::default()
            },
            rules,
            aggregation_rule: None,
        }
    }

    fn selector(k: &str, v: &str) -> LabelSelector {
        LabelSelector {
            match_labels: Some(HashMap::from([(k.to_string(), v.to_string())])),
            match_expressions: None,
        }
    }

    fn aggregating(name: &str, selectors: Vec<LabelSelector>) -> ClusterRole {
        let mut r = role(name, &[], vec![]);
        r.aggregation_rule = Some(AggregationRule {
            cluster_role_selectors: Some(selectors),
        });
        r
    }

    async fn put(s: &MemoryStorage, r: &ClusterRole) {
        s.create(&build_key("clusterroles", None, &r.metadata.name), r)
            .await
            .unwrap();
    }

    async fn rules_of(s: &MemoryStorage, name: &str) -> Vec<PolicyRule> {
        s.get::<ClusterRole>(&build_key("clusterroles", None, name))
            .await
            .unwrap()
            .rules
    }

    /// Matching rules are unioned in role-name order and deduplicated.
    #[tokio::test]
    async fn aggregates_matching_rules_sorted_and_deduped() {
        let s = Arc::new(MemoryStorage::new());
        put(
            &s,
            &role("b", &[("l", "x")], vec![rule(&["get"], &["pods"])]),
        )
        .await;
        put(
            &s,
            &role(
                "a",
                &[("l", "x")],
                vec![rule(&["get"], &["pods"]), rule(&["list"], &["nodes"])],
            ),
        )
        .await;
        put(
            &s,
            &role("c", &[("l", "y")], vec![rule(&["get"], &["secrets"])]),
        )
        .await;
        put(&s, &aggregating("parent", vec![selector("l", "x")])).await;

        let c = ClusterRoleAggregationController::new(s.clone());
        c.sync_cluster_role("parent").await.unwrap();
        assert_eq!(
            rules_of(&s, "parent").await,
            vec![rule(&["get"], &["pods"]), rule(&["list"], &["nodes"])]
        );
    }

    /// The reason the write-time mechanism was wrong: a matching child created
    /// after the parent must re-aggregate the parent.
    #[tokio::test]
    async fn a_new_matching_child_reaggregates_the_parent() {
        let s = Arc::new(MemoryStorage::new());
        put(&s, &aggregating("parent", vec![selector("l", "x")])).await;
        let c = ClusterRoleAggregationController::new(s.clone());
        c.sync_all().await.unwrap();
        assert!(rules_of(&s, "parent").await.is_empty());

        put(
            &s,
            &role("child", &[("l", "x")], vec![rule(&["get"], &["pods"])]),
        )
        .await;
        c.sync_all().await.unwrap();
        assert_eq!(
            rules_of(&s, "parent").await,
            vec![rule(&["get"], &["pods"])]
        );
    }

    /// The parent does not contribute to itself; other matching aggregating
    /// roles do (upstream has no skip for them).
    #[tokio::test]
    async fn skips_self_but_not_other_aggregating_roles() {
        let s = Arc::new(MemoryStorage::new());
        let mut other = aggregating("other", vec![selector("none", "none")]);
        other.metadata.labels = Some(HashMap::from([("l".into(), "x".into())]));
        other.rules = vec![rule(&["get"], &["pods"])];
        put(&s, &other).await;
        let mut parent = aggregating("parent", vec![selector("l", "x")]);
        parent.metadata.labels = Some(HashMap::from([("l".into(), "x".into())]));
        parent.rules = vec![rule(&["delete"], &["stale"])];
        put(&s, &parent).await;

        ClusterRoleAggregationController::new(s.clone())
            .sync_cluster_role("parent")
            .await
            .unwrap();
        assert_eq!(
            rules_of(&s, "parent").await,
            vec![rule(&["get"], &["pods"])]
        );
    }

    /// An invalid selector operator fails the sync (retried by the queue).
    #[tokio::test]
    async fn invalid_selector_fails_the_sync() {
        let s = Arc::new(MemoryStorage::new());
        let bad = LabelSelector {
            match_labels: None,
            match_expressions: Some(vec![LabelSelectorRequirement {
                key: "k".into(),
                operator: "Bogus".into(),
                values: None,
            }]),
        };
        put(&s, &aggregating("parent", vec![bad])).await;
        let c = ClusterRoleAggregationController::new(s.clone());
        assert!(c.sync_cluster_role("parent").await.is_err());
    }

    /// A missing or non-aggregating ClusterRole is a no-op.
    #[tokio::test]
    async fn non_aggregating_and_missing_roles_are_noops() {
        let s = Arc::new(MemoryStorage::new());
        put(&s, &role("plain", &[], vec![rule(&["get"], &["pods"])])).await;
        let c = ClusterRoleAggregationController::new(s.clone());
        c.sync_cluster_role("plain").await.unwrap();
        c.sync_cluster_role("missing").await.unwrap();
        assert_eq!(rules_of(&s, "plain").await, vec![rule(&["get"], &["pods"])]);
    }
}
