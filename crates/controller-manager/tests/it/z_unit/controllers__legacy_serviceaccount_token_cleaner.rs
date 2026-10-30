//! Unit tests moved from `src/controllers/legacy_serviceaccount_token_cleaner.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Utc};
    pub use rusternetes_common::resources::{ConfigMap, Pod, Secret, ServiceAccount};
    pub use rusternetes_storage::{build_key, build_prefix, Preconditions, Storage};
    pub use std::collections::{HashMap, HashSet};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tracing::{error, info};
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::legacy_serviceaccount_token_cleaner::*;
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

    /// The label patch carries `WithUID(secret.UID)` (:190-198): a Secret that
    /// was deleted and recreated (new UID) after the cleaner listed it must not
    /// receive the label meant for the old generation; the live one must.
    #[tokio::test]
    async fn invalid_since_label_is_uid_guarded() {
        let storage = Arc::new(MemoryStorage::new());
        let live = token_secret("", "", "2022-12-27", "default", "12345", false);
        put(&storage, "secrets", "default", &live).await;
        let cleaner = LegacySATokenCleaner::new(
            storage.clone(),
            LegacySATokenCleanerOptions {
                clean_up_period: Duration::from_secs(60),
                sync_interval: Duration::from_secs(30),
            },
        )
        .unwrap();
        let key = build_key("secrets", Some("default"), "token-secret-1");
        let label = |v: &serde_json::Value| {
            v["metadata"]["labels"]
                .get(INVALID_SINCE_LABEL_KEY)
                .cloned()
        };

        let mut stale: Secret = serde_json::from_value(live.clone()).unwrap();
        stale.metadata.uid = "old-generation".to_string();
        cleaner.label_invalid_since(&stale, "2022-12-29").await;
        let now: serde_json::Value = storage.get(&key).await.unwrap();
        assert_eq!(label(&now), None, "stale UID must not be labeled");

        let current: Secret = serde_json::from_value(live).unwrap();
        cleaner.label_invalid_since(&current, "2022-12-29").await;
        let now: serde_json::Value = storage.get(&key).await.unwrap();
        assert_eq!(label(&now), Some(json!("2022-12-29")));
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
