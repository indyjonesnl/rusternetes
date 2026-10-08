//! Legacy service-account token cleaner.
//!
//! Ported from `pkg/controller/serviceaccount/legacy_serviceaccount_token_cleaner.go`
//! (release-1.35) and wired as `newLegacyServiceAccountTokenCleanerController`
//! (`cmd/kube-controller-manager/app/core.go:933-963`): the controller deletes
//! auto-generated legacy service-account token Secrets that nothing has used
//! for `--legacy-service-account-token-clean-up-period` (default 365 days,
//! `pkg/controller/serviceaccount/config/v1alpha1/defaults.go:41-46`).
//! It runs every `DefaultCleanerSyncInterval` (24h, `:44`).
//!
//! The cutoff is anchored on the `since` date of the kube-system
//! `kube-apiserver-legacy-service-account-token-tracking` ConfigMap that the
//! api-server's `start-legacy-token-tracking-controller` hook maintains
//! (`pkg/controlplane/controller/legacytokentracking/controller.go:39-40`).
//!
//! Deviations (storage seam instead of client-go informers/listers):
//! - Lists are read from storage per pass instead of informer caches.
//! - The patch's `metadata.uid` and the delete's `resourceVersion`
//!   preconditions are checked by re-reading the Secret just before the write
//!   (the `Storage` trait has no preconditions), leaving a small race window.
//! - The `kubernetes.io/legacy-token-last-used` stamping on use is the
//!   api-server authenticator's half (`pkg/serviceaccount/legacy.go:185-198`);
//!   it is not part of this controller (tracked separately).

use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Utc};
use rusternetes_common::resources::{ConfigMap, Pod, Secret, ServiceAccount};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

const SECRET_TYPE_SERVICE_ACCOUNT_TOKEN: &str = "kubernetes.io/service-account-token";
const SA_NAME_ANNOTATION: &str = "kubernetes.io/service-account.name";
const SA_UID_ANNOTATION: &str = "kubernetes.io/service-account.uid";

/// `serviceaccount.LastUsedLabelKey` (pkg/serviceaccount/legacy.go:55).
pub const LAST_USED_LABEL_KEY: &str = "kubernetes.io/legacy-token-last-used";
/// `serviceaccount.InvalidSinceLabelKey` (pkg/serviceaccount/legacy.go:40).
pub const INVALID_SINCE_LABEL_KEY: &str = "kubernetes.io/legacy-token-invalid-since";
/// `legacytokentracking.ConfigMapName` (controller.go:39).
pub const TRACKING_CONFIG_MAP_NAME: &str = "kube-apiserver-legacy-service-account-token-tracking";
/// `legacytokentracking.ConfigMapDataKey` (controller.go:40).
pub const TRACKING_CONFIG_MAP_DATA_KEY: &str = "since";

/// `DefaultCleanerSyncInterval` (legacy_serviceaccount_token_cleaner.go:44).
pub const DEFAULT_CLEANER_SYNC_INTERVAL: Duration = Duration::from_secs(24 * 3600);
/// `RecommendedDefaultLegacySATokenCleanerConfiguration`: `365 * 24 * time.Hour`
/// (config/v1alpha1/defaults.go:44).
#[allow(dead_code)] // the bin takes it from the flag default; the lib wires it
pub const DEFAULT_CLEAN_UP_PERIOD: Duration = Duration::from_secs(365 * 24 * 3600);

/// `time.Parse("2006-01-02", s)` succeeds: strict, zero-padded.
fn parse_date(s: &str) -> Option<NaiveDate> {
    if s.len() != 10 {
        return None;
    }
    NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

fn format_date(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%d").to_string()
}

/// `LegacySATokenCleanerOptions` (legacy_serviceaccount_token_cleaner.go:48-52).
#[derive(Debug, Clone, Copy)]
pub struct LegacySATokenCleanerOptions {
    /// Period since last use of a legacy token before it can be deleted.
    pub clean_up_period: Duration,
    pub sync_interval: Duration,
}

pub struct LegacySATokenCleaner<S: Storage> {
    storage: Arc<S>,
    minimum_since_last_used: ChronoDuration,
    sync_interval: Duration,
}

impl<S: Storage + 'static> LegacySATokenCleaner<S> {
    /// `NewLegacySATokenCleaner` (:69-77): both durations must be positive.
    pub fn new(storage: Arc<S>, options: LegacySATokenCleanerOptions) -> Result<Self> {
        if options.clean_up_period.is_zero() {
            anyhow::bail!("invalid CleanUpPeriod: {:?}", options.clean_up_period);
        }
        if options.sync_interval.is_zero() {
            anyhow::bail!("invalid SyncInterval: {:?}", options.sync_interval);
        }
        Ok(Self {
            storage,
            minimum_since_last_used: ChronoDuration::from_std(options.clean_up_period)?,
            sync_interval: options.sync_interval,
        })
    }

    /// `Run` (:92-103): `wait.UntilWithContext(ctx, evaluateSATokens, syncInterval)`.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting legacy service account token cleaner controller");
        loop {
            self.evaluate_sa_tokens(Utc::now()).await;
            tokio::time::sleep(self.sync_interval).await;
        }
    }

    /// `latestPossibleTrackedSinceTime` (:262-279): the `since` date plus one
    /// day, so the time is 00:00 on the day after tracking started.
    async fn latest_possible_tracked_since_time(&self) -> Result<DateTime<Utc>> {
        let key = build_key("configmaps", Some("kube-system"), TRACKING_CONFIG_MAP_NAME);
        let cm: ConfigMap = self.storage.get(&key).await?;
        let since = cm
            .data
            .as_ref()
            .and_then(|d| d.get(TRACKING_CONFIG_MAP_DATA_KEY))
            .ok_or_else(|| anyhow::anyhow!("configMap does not have since label"))?;
        let date = parse_date(since)
            .ok_or_else(|| anyhow::anyhow!("error parsing trackedSince time: {since:?}"))?;
        let next = date
            .succ_opt()
            .ok_or_else(|| anyhow::anyhow!("trackedSince out of range"))?;
        Ok(next.and_hms_opt(0, 0, 0).expect("midnight").and_utc())
    }

    /// `evaluateSATokens` (:105-218). `now` stands in for `tc.clock.Now().UTC()`.
    pub async fn evaluate_sa_tokens(&self, now: DateTime<Utc>) {
        let tracked_since = match self.latest_possible_tracked_since_time().await {
            Ok(t) => t,
            Err(e) => {
                error!("Getting lastest possible tracked_since time: {e}");
                return;
            }
        };
        if now < tracked_since + self.minimum_since_last_used {
            // we haven't been tracking long enough
            return;
        }

        let preserve_created_on_or_after = now - self.minimum_since_last_used;
        let preserve_used_on_or_after = format_date(preserve_created_on_or_after);

        let secrets: Vec<Secret> = match self.storage.list(&build_prefix("secrets", None)).await {
            Ok(s) => s,
            Err(e) => {
                error!("Getting secret list: {e}");
                return;
            }
        };

        let mut namespace_to_used: HashMap<String, HashSet<String>> = HashMap::new();
        for secret in secrets {
            if secret.secret_type.as_deref() != Some(SECRET_TYPE_SERVICE_ACCOUNT_TOKEN) {
                continue;
            }
            // `!CreationTimestamp.Before(preserveCreatedOnOrAfter)`; a zero
            // timestamp is before everything.
            if secret
                .metadata
                .creation_timestamp
                .is_some_and(|c| c >= preserve_created_on_or_after)
            {
                continue;
            }
            if secret.metadata.deletion_timestamp.is_some() {
                continue;
            }

            // If LastUsedLabelKey does not exist, we think the secret has not
            // been used since the legacy token started to be tracked.
            let last_used = secret
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(LAST_USED_LABEL_KEY))
                .cloned();
            if let Some(last_used) = last_used.as_deref() {
                if parse_date(last_used).is_none() {
                    // the lastUsed value is not well-formed thus we cannot determine it
                    error!(
                        "Parsing lastUsed time: {last_used:?} (secret {}/{})",
                        namespace_of(&secret),
                        secret.metadata.name
                    );
                    continue;
                }
                if last_used >= preserve_used_on_or_after.as_str() {
                    continue;
                }
            }

            let sa = match self.get_service_account(&secret).await {
                Ok(sa) => sa,
                Err(e) => {
                    error!(
                        "Getting service account for secret {}/{}: {e}",
                        namespace_of(&secret),
                        secret.metadata.name
                    );
                    continue;
                }
            };
            let Some(sa) = sa else { continue };
            if !has_secret_reference(&sa, &secret.metadata.name) {
                // can't determine if this is an auto-generated token
                continue;
            }

            let ns = namespace_of(&secret).to_string();
            let mounted = match self.mounted_secret_names(&ns, &mut namespace_to_used).await {
                Ok(m) => m,
                Err(e) => {
                    error!(
                        "Resolving mounted secrets for {}/{}: {e}",
                        ns, secret.metadata.name
                    );
                    continue;
                }
            };
            if mounted.contains(&secret.metadata.name) {
                // still used by pods
                continue;
            }

            let invalid_since = secret
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(INVALID_SINCE_LABEL_KEY))
                .cloned()
                .unwrap_or_default();
            // If the secret has not been labeled with an invalid-since date, or
            // the label value is malformed, label it with the current date.
            if parse_date(&invalid_since).is_none() {
                let invalid_since = format_date(now);
                info!(
                    "Mark the auto-generated service account token as invalid (invalidSince {invalid_since}, secret {}/{})",
                    ns, secret.metadata.name
                );
                self.label_invalid_since(&secret, &invalid_since).await;
                continue;
            }

            if invalid_since.as_str() >= preserve_used_on_or_after.as_str() {
                continue;
            }

            info!(
                "Delete auto-generated service account token {}/{} (creationTime {:?}, lastUsed {:?}, invalidSince {invalid_since})",
                ns, secret.metadata.name, secret.metadata.creation_timestamp, last_used
            );
            self.delete_secret(&secret).await;
        }
    }

    /// The merge patch `applyv1.Secret(name, ns).WithUID(uid).WithLabels(..)`
    /// (:190-198); `metadata.uid` acts as a precondition upstream, so verify
    /// the live UID instead of writing it.
    async fn label_invalid_since(&self, secret: &Secret, invalid_since: &str) {
        let key = build_key("secrets", Some(namespace_of(secret)), &secret.metadata.name);
        match self.storage.get::<Secret>(&key).await {
            Ok(live) if live.metadata.uid == secret.metadata.uid => {}
            Ok(_) => return,
            Err(rusternetes_common::Error::NotFound(_)) => return,
            Err(e) => {
                error!("Failed to label legacy service account token secret with invalid since date: {e}");
                return;
            }
        }
        let patch = serde_json::json!({
            "metadata": { "labels": { INVALID_SINCE_LABEL_KEY: invalid_since } }
        });
        if let Err(e) = self
            .storage
            .patch_strategic_merge::<Secret>(&key, &patch)
            .await
        {
            error!(
                "Failed to label legacy service account token secret with invalid since date: {e}"
            );
        }
    }

    /// `Delete` with `Preconditions{ResourceVersion}`; Conflict and NotFound are
    /// swallowed (:208-216).
    async fn delete_secret(&self, secret: &Secret) {
        let key = build_key("secrets", Some(namespace_of(secret)), &secret.metadata.name);
        match self.storage.get::<Secret>(&key).await {
            Ok(live) if live.metadata.resource_version == secret.metadata.resource_version => {}
            Ok(_) => return, // conflict
            Err(rusternetes_common::Error::NotFound(_)) => return,
            Err(e) => {
                error!("Deleting legacy service account token: {e}");
                return;
            }
        }
        match self.storage.delete(&key).await {
            Ok(()) | Err(rusternetes_common::Error::NotFound(_)) => {}
            Err(e) => error!(
                "Deleting legacy service account token {}/{}: {e}",
                namespace_of(secret),
                secret.metadata.name
            ),
        }
    }

    /// `getMountedSecretNames` (:220-246), cached per namespace.
    async fn mounted_secret_names<'a>(
        &self,
        namespace: &str,
        cache: &'a mut HashMap<String, HashSet<String>>,
    ) -> Result<&'a HashSet<String>> {
        if !cache.contains_key(namespace) {
            let pods: Vec<Pod> = self
                .storage
                .list(&build_prefix("pods", Some(namespace)))
                .await?;
            let mut names = HashSet::new();
            for pod in &pods {
                visit_pod_secret_names(pod, &mut |n| {
                    names.insert(n.to_string());
                });
            }
            cache.insert(namespace.to_string(), names);
        }
        Ok(&cache[namespace])
    }

    /// `getServiceAccount` (:248-270).
    async fn get_service_account(&self, secret: &Secret) -> Result<Option<ServiceAccount>> {
        let ann = secret.metadata.annotations.as_ref();
        let sa_name = ann
            .and_then(|a| a.get(SA_NAME_ANNOTATION))
            .map(String::as_str)
            .unwrap_or("");
        if sa_name.is_empty() {
            return Ok(None);
        }
        let sa_uid = ann
            .and_then(|a| a.get(SA_UID_ANNOTATION))
            .map(String::as_str)
            .unwrap_or("");
        let key = build_key("serviceaccounts", Some(namespace_of(secret)), sa_name);
        let sa: ServiceAccount = match self.storage.get(&key).await {
            Ok(sa) => sa,
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        // Ensure UID matches if given
        if sa_uid.is_empty() || sa_uid == sa.metadata.uid {
            Ok(Some(sa))
        } else {
            Ok(None)
        }
    }
}

fn namespace_of(secret: &Secret) -> &str {
    secret.metadata.namespace.as_deref().unwrap_or("")
}

/// `hasSecretReference` (:281-288).
fn has_secret_reference(sa: &ServiceAccount, secret_name: &str) -> bool {
    sa.secrets
        .iter()
        .flatten()
        .any(|r| r.name.as_deref() == Some(secret_name))
}

/// `podutil.VisitPodSecretNames` (pkg/api/v1/pod/util.go:113-179,
/// `visitContainerSecretNames` :182-): every secret the pod spec references,
/// empty names skipped (`skipEmptyNames`). Only volume sources this API models
/// are visited (secret, projected, CSI nodePublishSecretRef, iSCSI).
fn visit_pod_secret_names(pod: &Pod, visit: &mut dyn FnMut(&str)) {
    let Some(spec) = pod.spec.as_ref() else {
        return;
    };
    let mut visit = |n: &str| {
        if !n.is_empty() {
            visit(n)
        }
    };
    for r in spec.image_pull_secrets.iter().flatten() {
        visit(&r.name);
    }
    let containers = spec
        .containers
        .iter()
        .chain(spec.init_containers.iter().flatten());
    for c in containers {
        for e in c.env_from.iter().flatten() {
            if let Some(s) = &e.secret_ref {
                visit(&s.name);
            }
        }
        for e in c.env.iter().flatten() {
            if let Some(s) = e
                .value_from
                .as_ref()
                .and_then(|v| v.secret_key_ref.as_ref())
            {
                visit(&s.name);
            }
        }
    }
    for c in spec.ephemeral_containers.iter().flatten() {
        for e in c.env_from.iter().flatten() {
            if let Some(s) = &e.secret_ref {
                visit(&s.name);
            }
        }
        for e in c.env.iter().flatten() {
            if let Some(s) = e
                .value_from
                .as_ref()
                .and_then(|v| v.secret_key_ref.as_ref())
            {
                visit(&s.name);
            }
        }
    }
    for v in spec.volumes.iter().flatten() {
        if let Some(p) = &v.projected {
            for s in p.sources.iter().flatten() {
                if let Some(s) = &s.secret {
                    visit(s.name.as_deref().unwrap_or(""));
                }
            }
        } else if let Some(s) = &v.secret {
            visit(s.secret_name.as_deref().unwrap_or(""));
        } else if let Some(i) = &v.iscsi {
            if let Some(r) = &i.secret_ref {
                visit(r.name.as_deref().unwrap_or(""));
            }
        } else if let Some(c) = &v.csi {
            if let Some(r) = &c.node_publish_secret_ref {
                visit(r.name.as_deref().unwrap_or(""));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_storage::memory::MemoryStorage;
    use serde_json::json;

    const CM_NS: &str = "kube-system";

    fn date(s: &str) -> DateTime<Utc> {
        NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
    }

    /// `configuredLegacyTokenCleanUpPeriod`: `time.Now() - start`.
    fn period_since(start: &str) -> Duration {
        (Utc::now() - date(start)).to_std().unwrap()
    }

    /// `configuredConfigMap`
    fn config_map(label: &str) -> serde_json::Value {
        let mut cm = json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"namespace": CM_NS, "name": TRACKING_CONFIG_MAP_NAME},
        });
        if !label.is_empty() {
            cm["data"] = json!({ TRACKING_CONFIG_MAP_DATA_KEY: label });
        }
        cm
    }

    /// `configuredServiceAccountTokenSecret`
    fn token_secret(
        last_used: &str,
        invalid_since: &str,
        created: &str,
        sa_name: &str,
        sa_uid: &str,
        deleted: bool,
    ) -> serde_json::Value {
        let mut labels = serde_json::Map::new();
        if !last_used.is_empty() {
            labels.insert(LAST_USED_LABEL_KEY.into(), json!(last_used));
        }
        if !invalid_since.is_empty() {
            labels.insert(INVALID_SINCE_LABEL_KEY.into(), json!(invalid_since));
        }
        let mut meta = json!({
            "name": "token-secret-1", "namespace": "default", "uid": "23456",
            "resourceVersion": "1", "labels": labels,
            "creationTimestamp": date(created).to_rfc3339(),
            "annotations": {SA_NAME_ANNOTATION: sa_name, SA_UID_ANNOTATION: sa_uid},
        });
        if deleted {
            meta["deletionTimestamp"] = json!(Utc::now().to_rfc3339());
        }
        json!({
            "apiVersion": "v1", "kind": "Secret", "metadata": meta,
            "type": SECRET_TYPE_SERVICE_ACCOUNT_TOKEN,
        })
    }

    fn opaque_secret() -> serde_json::Value {
        json!({
            "apiVersion": "v1", "kind": "Secret", "type": "Opaque",
            "metadata": {"name": "token-secret-1", "namespace": "default", "uid": "23456",
                         "resourceVersion": "1",
                         "creationTimestamp": date("2022-12-27").to_rfc3339()},
        })
    }

    fn service_account(refs: bool) -> serde_json::Value {
        let mut sa = json!({
            "apiVersion": "v1", "kind": "ServiceAccount",
            "metadata": {"name": "default", "namespace": "default", "uid": "12345"},
        });
        if refs {
            sa["secrets"] = json!([{"name": "token-secret-1"}]);
        }
        sa
    }

    fn pod(mount: bool) -> serde_json::Value {
        let mut spec = json!({"containers": []});
        if mount {
            spec["volumes"] = json!([{"name": "foo", "secret": {"secretName": "token-secret-1"}}]);
        }
        json!({"apiVersion": "v1", "kind": "Pod",
               "metadata": {"name": "pod-1", "namespace": "default"}, "spec": spec})
    }

    #[derive(Debug, PartialEq)]
    enum Outcome {
        Untouched,
        /// Patched with `invalid-since` = today.
        Labeled,
        Deleted,
    }

    struct Case {
        name: &'static str,
        period: Duration,
        sa: Option<serde_json::Value>,
        secret: serde_json::Value,
        pod: serde_json::Value,
        cm: Option<serde_json::Value>,
        want: Outcome,
    }

    async fn put(s: &MemoryStorage, kind: &str, ns: &str, v: &serde_json::Value) {
        let name = v["metadata"]["name"].as_str().unwrap().to_string();
        let key = build_key(kind, Some(ns), &name);
        s.create::<serde_json::Value>(&key, v).await.unwrap();
    }

    async fn run_case(c: Case) {
        let storage = Arc::new(MemoryStorage::new());
        if let Some(cm) = &c.cm {
            put(&storage, "configmaps", CM_NS, cm).await;
        }
        if let Some(sa) = &c.sa {
            put(&storage, "serviceaccounts", "default", sa).await;
        }
        put(&storage, "pods", "default", &c.pod).await;
        put(&storage, "secrets", "default", &c.secret).await;
        let cleaner = LegacySATokenCleaner::new(
            storage.clone(),
            LegacySATokenCleanerOptions {
                clean_up_period: c.period,
                sync_interval: Duration::from_secs(30),
            },
        )
        .unwrap();
        cleaner.evaluate_sa_tokens(Utc::now()).await;

        let key = build_key("secrets", Some("default"), "token-secret-1");
        let outcome = match storage.get::<serde_json::Value>(&key).await {
            Err(_) => Outcome::Deleted,
            Ok(v) => {
                let before = c.secret["metadata"]["labels"]
                    .get(INVALID_SINCE_LABEL_KEY)
                    .cloned();
                let after = v["metadata"]["labels"]
                    .get(INVALID_SINCE_LABEL_KEY)
                    .cloned();
                if after != before {
                    assert_eq!(
                        after,
                        Some(json!(format_date(Utc::now()))),
                        "{}: invalid-since must be today",
                        c.name
                    );
                    Outcome::Labeled
                } else {
                    Outcome::Untouched
                }
            }
        };
        assert_eq!(outcome, c.want, "{}", c.name);
    }

    fn case(
        name: &'static str,
        period: &str,
        cm: Option<&str>,
        secret: serde_json::Value,
        sa: Option<serde_json::Value>,
        pod_mounted: bool,
        want: Outcome,
    ) -> Case {
        Case {
            name,
            period: period_since(period),
            sa,
            secret,
            pod: pod(pod_mounted),
            cm: cm.map(config_map),
            want,
        }
    }

    // Upstream TestLegacyServiceAccountTokenCleanUp
    // (legacy_serviceaccount_token_cleaner_test.go:122-).
    #[tokio::test]
    async fn legacy_service_account_token_clean_up() {
        use Outcome::*;
        let ts = || token_secret("2022-12-27", "", "2022-12-27", "default", "12345", false);
        let sa = || Some(service_account(true));
        #[rustfmt::skip]
        let cases = vec![
            case("configmap does not exist", "2022-12-28", None, ts(), sa(), false, Untouched),
            case("configmap has no tracked-since label", "2022-12-28", Some(""), ts(), sa(), false, Untouched),
            case("time since tracked-since smaller than CleanUpPeriod", "2022-12-29", Some("2022-12-29"), ts(), sa(), false, Untouched),
            case("tracked-since cannot be parsed", "2022-12-29", Some("2022-12-27-1"), ts(), sa(), false, Untouched),
            case("secret is not SecretTypeServiceAccountToken", "2022-12-29", Some("2022-12-28"), opaque_secret(), sa(), false, Untouched),
            case("secret is not referenced by serviceaccount", "2022-12-29", Some("2022-12-27"), ts(), Some(service_account(false)), false, Untouched),
            case("auto-generated secret has a late creation time", "2022-12-29", Some("2022-12-27"), token_secret("2022-12-27", "", "2022-12-30", "default", "12345", false), sa(), false, Untouched),
            case("auto-generated secret has a deletion time", "2022-12-30", Some("2022-12-27"), token_secret("2022-12-27", "", "2022-12-27", "default", "12345", true), sa(), false, Untouched),
            case("auto-generated secret has a late last-used time", "2022-12-29", Some("2022-12-27"), token_secret("2022-12-30", "", "2022-12-27", "default", "12345", false), sa(), false, Untouched),
            case("last-used label cannot be parsed", "2022-12-29", Some("2022-12-27"), token_secret("2022-12-27-1", "", "2022-12-27", "default", "12345", false), sa(), false, Untouched),
            case("secret-referenced service account does not exist", "2022-12-29", Some("2022-12-27"), ts(), None, false, Untouched),
            case("secret-referenced service account uid does not match", "2022-12-29", Some("2022-12-27"), token_secret("2022-12-27", "", "2022-12-27", "default", "123456", false), sa(), false, Untouched),
            case("secret-referenced service account name is empty", "2022-12-29", Some("2022-12-27"), token_secret("2022-12-27", "", "2022-12-27", "", "12345", false), sa(), false, Untouched),
            case("no last-used, not marked invalid", "2022-12-30", Some("2022-12-28"), token_secret("", "", "2022-12-27", "default", "12345", false), sa(), false, Labeled),
            case("no last-used, invalid_since cannot be parsed", "2022-12-30", Some("2022-12-28"), token_secret("", "2022-12-29-1", "2022-12-27", "default", "12345", false), sa(), false, Labeled),
            case("no last-used, invalid, period since invalid less than CleanUpPeriod", "2022-12-29", Some("2022-12-28"), token_secret("", "2023-01-01", "2022-12-27", "default", "12345", false), sa(), false, Untouched),
            case("no last-used, invalid, period since invalid larger than CleanUpPeriod", "2023-01-01", Some("2022-12-28"), token_secret("", "2022-12-29", "2022-12-27", "default", "12345", false), sa(), false, Deleted),
            case("auto-generated secret is mounted by the pod", "2022-12-29", Some("2022-12-28"), ts(), sa(), true, Untouched),
            case("last-used older than CleanUpPeriod, not marked invalid", "2022-12-30", Some("2022-12-28"), ts(), sa(), false, Labeled),
            case("last-used older, invalid, period since invalid less than CleanUpPeriod", "2022-12-30", Some("2022-12-28"), token_secret("2022-12-27", "2023-05-01", "2022-12-27", "default", "12345", false), sa(), false, Untouched),
            case("last-used older, invalid, period since invalid larger than CleanUpPeriod", "2023-05-01", Some("2022-12-28"), token_secret("2022-12-27", "2023-01-05", "2022-12-27", "default", "12345", false), sa(), false, Deleted),
        ];
        for c in cases {
            run_case(c).await;
        }
    }

    #[test]
    fn rejects_non_positive_durations() {
        let s = Arc::new(MemoryStorage::new());
        let ok = Duration::from_secs(1);
        let zero = Duration::ZERO;
        let opts = |p, i| LegacySATokenCleanerOptions {
            clean_up_period: p,
            sync_interval: i,
        };
        assert!(LegacySATokenCleaner::new(s.clone(), opts(zero, ok)).is_err());
        assert!(LegacySATokenCleaner::new(s, opts(ok, zero)).is_err());
    }

    #[test]
    fn visits_secret_names_from_every_source() {
        let p: Pod = serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p"},
            "spec": {
                "imagePullSecrets": [{"name": "pull"}, {"name": ""}],
                "containers": [{"name": "c", "image": "i",
                    "envFrom": [{"secretRef": {"name": "envfrom"}}],
                    "env": [{"name": "A", "valueFrom": {"secretKeyRef": {"name": "key", "key": "k"}}}]}],
                "volumes": [
                    {"name": "a", "secret": {"secretName": "vol"}},
                    {"name": "b", "projected": {"sources": [{"secret": {"name": "proj"}}]}},
                ],
            }
        }))
        .unwrap();
        let mut got = Vec::new();
        visit_pod_secret_names(&p, &mut |n| got.push(n.to_string()));
        got.sort();
        assert_eq!(got, ["envfrom", "key", "proj", "pull", "vol"]);
    }
}
