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
//! - The delete's `resourceVersion` precondition is atomic
//!   (`Storage::delete_with_preconditions`, upstream `storage.Preconditions`),
//!   and so is the patch's `metadata.uid` precondition
//!   (`Storage::patch_strategic_merge_with_preconditions`, upstream
//!   `GuaranteedUpdate(.., preconditions, ..)`, `etcd3/store.go:463`).
//! - The `kubernetes.io/legacy-token-last-used` stamping on use is the
//!   api-server authenticator's half (`pkg/serviceaccount/legacy.go:185-198`);
//!   it is not part of this controller (tracked separately).

use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Utc};
use rusternetes_common::resources::{ConfigMap, Pod, Secret, ServiceAccount};
use rusternetes_storage::{build_key, build_prefix, Preconditions, Storage};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

pub const SECRET_TYPE_SERVICE_ACCOUNT_TOKEN: &str = "kubernetes.io/service-account-token";
pub const SA_NAME_ANNOTATION: &str = "kubernetes.io/service-account.name";
pub const SA_UID_ANNOTATION: &str = "kubernetes.io/service-account.uid";

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
pub const DEFAULT_CLEAN_UP_PERIOD: Duration = Duration::from_secs(365 * 24 * 3600);

/// `time.Parse("2006-01-02", s)` succeeds: strict, zero-padded.
fn parse_date(s: &str) -> Option<NaiveDate> {
    if s.len() != 10 {
        return None;
    }
    NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

pub fn format_date(t: DateTime<Utc>) -> String {
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
    /// (:190-198): `metadata.uid` is a storage precondition upstream, so the
    /// label lands only on the Secret generation that was inspected. Errors
    /// (including a UID-mismatch Conflict) are logged, as upstream does.
    pub async fn label_invalid_since(&self, secret: &Secret, invalid_since: &str) {
        let key = build_key("secrets", Some(namespace_of(secret)), &secret.metadata.name);
        let patch = serde_json::json!({
            "metadata": { "labels": { INVALID_SINCE_LABEL_KEY: invalid_since } }
        });
        let preconditions = Preconditions::new_uid(secret.metadata.uid.clone());
        if let Err(e) = self
            .storage
            .patch_strategic_merge_with_preconditions::<Secret>(&key, &patch, &preconditions)
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
        let preconditions = Preconditions::new_resource_version(
            secret.metadata.resource_version.clone().unwrap_or_default(),
        );
        match self
            .storage
            .delete_with_preconditions(&key, &preconditions)
            .await
        {
            Ok(())
            | Err(rusternetes_common::Error::NotFound(_))
            | Err(rusternetes_common::Error::Conflict(_)) => {}
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
pub fn visit_pod_secret_names(pod: &Pod, visit: &mut dyn FnMut(&str)) {
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
