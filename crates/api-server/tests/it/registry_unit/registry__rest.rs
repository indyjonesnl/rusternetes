//! Unit tests moved from `src/registry/rest.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use chrono::{Duration, Utc};
    pub use rusternetes_api_server::registry::metadata::HasMetadata;
    pub use rusternetes_common::auth::UserInfo;
    pub use rusternetes_common::deletion::{DeleteOptions, Preconditions};
    pub use rusternetes_common::types::ObjectMeta;
    pub use rusternetes_common::validation::field::{ErrorList, Path};
    pub use rusternetes_common::validation::objectmeta::{
        validate_object_meta, validate_object_meta_update, validate_path_segment_name,
    };
    pub use rusternetes_common::{Error, Result};
    pub use serde::{de::DeserializeOwned, Serialize};
    pub use std::sync::Mutex;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::rest::*;
    use rusternetes_common::resources::ConfigMap;
    use rusternetes_common::validation::field::Error as FieldError;

    /// A strategy with no opinions, to exercise the common rules alone.
    struct Plain {
        namespaced: bool,
    }
    impl NamespaceScopedStrategy for Plain {
        fn namespace_scoped(&self) -> bool {
            self.namespaced
        }
    }
    impl RestCreateStrategy<ConfigMap> for Plain {
        fn prepare_for_create(&self, _: &RequestContext, _: &mut ConfigMap) {}
        fn validate(&self, _: &RequestContext, _: &ConfigMap) -> ErrorList {
            Vec::new()
        }
        fn warnings_on_create(&self, _: &RequestContext, _: &ConfigMap) -> Vec<String> {
            vec!["a warning".to_string()]
        }
    }
    impl RestUpdateStrategy<ConfigMap> for Plain {
        fn allow_create_on_update(&self) -> bool {
            false
        }
        fn prepare_for_update(&self, _: &RequestContext, _: &mut ConfigMap, _: &ConfigMap) {}
        fn validate_update(&self, _: &RequestContext, _: &ConfigMap, _: &ConfigMap) -> ErrorList {
            Vec::new()
        }
        fn allow_unconditional_update(&self) -> bool {
            true
        }
    }

    fn cm(name: &str) -> ConfigMap {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": name},
        }))
        .unwrap()
    }

    /// `BeforeCreate` refuses an object the Store did not fill: it checks, it
    /// does not fill (create.go:102-104).
    #[tokio::test]
    async fn before_create_requires_system_metadata() {
        let mut obj = cm("a");
        let err = before_create(
            &Plain { namespaced: true },
            &RequestContext::new(Some("ns")),
            &mut obj,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("system metadata was not initialized"),
            "{err}"
        );
    }

    /// The namespace comes from the request; a conflicting one in the body is
    /// a 400 (upstream `TestBeforeCreate`-style cases in meta_test.go).
    #[tokio::test]
    async fn before_create_takes_the_request_namespace_and_rejects_a_mismatch() {
        let ctx = RequestContext::new(Some("ns"));
        let mut obj = cm("a");
        fill_object_meta_system_fields(&mut obj.metadata);
        before_create(&Plain { namespaced: true }, &ctx, &mut obj)
            .await
            .unwrap();
        assert_eq!(obj.metadata.namespace.as_deref(), Some("ns"));
        assert_eq!(ctx.warnings(), vec!["a warning".to_string()]);

        let mut other = cm("b");
        other.metadata.namespace = Some("elsewhere".to_string());
        fill_object_meta_system_fields(&mut other.metadata);
        let err = before_create(&Plain { namespaced: true }, &ctx, &mut other)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::BadRequest(_)), "{err:?}");
    }

    /// A cluster-scoped strategy clears a namespace the client sent.
    #[tokio::test]
    async fn before_create_clears_the_namespace_of_a_cluster_scoped_object() {
        let mut obj = cm("a");
        obj.metadata.namespace = Some("ns".to_string());
        fill_object_meta_system_fields(&mut obj.metadata);
        before_create(
            &Plain { namespaced: false },
            &RequestContext::new(Some("ns")),
            &mut obj,
        )
        .await
        .unwrap();
        assert_eq!(obj.metadata.namespace, None);
    }

    /// `BeforeUpdate` restores what a client may not change, generation first
    /// (update.go:127-146).
    #[tokio::test]
    async fn before_update_restores_server_owned_metadata() {
        let ctx = RequestContext::new(Some("ns"));
        let mut old = cm("a");
        old.metadata.namespace = Some("ns".to_string());
        old.metadata.uid = "uid-1".to_string();
        old.metadata.generation = Some(4);
        old.metadata.resource_version = Some("7".to_string());
        old.metadata.creation_timestamp = Some(now_seconds());
        old.metadata.deletion_timestamp = Some(now_seconds());
        old.metadata.deletion_grace_period_seconds = Some(30);

        let mut new = cm("a");
        new.metadata.resource_version = Some("7".to_string());
        new.metadata.generation = Some(1);
        before_update(&Plain { namespaced: true }, &ctx, &mut new, &old)
            .await
            .unwrap();
        assert_eq!(new.metadata.uid, "uid-1");
        assert_eq!(new.metadata.generation, Some(4));
        assert_eq!(
            new.metadata.creation_timestamp,
            old.metadata.creation_timestamp
        );
        assert_eq!(
            new.metadata.deletion_timestamp,
            old.metadata.deletion_timestamp
        );
        assert_eq!(new.metadata.deletion_grace_period_seconds, Some(30));
    }

    /// A UID that does not match is not rewritten; it fails the common
    /// metadata validation as an immutable field.
    #[tokio::test]
    async fn before_update_rejects_a_changed_uid() {
        let mut old = cm("a");
        old.metadata.namespace = Some("ns".to_string());
        old.metadata.uid = "uid-1".to_string();
        let mut new = cm("a");
        new.metadata.uid = "uid-2".to_string();
        new.metadata.resource_version = Some("1".to_string());
        let err = before_update(
            &Plain { namespaced: true },
            &RequestContext::new(Some("ns")),
            &mut new,
            &old,
        )
        .await
        .unwrap_err();
        let Error::Invalid(errs) = err else {
            panic!("{err:?}")
        };
        assert!(
            errs.iter().any(|e: &FieldError| e.field == "metadata.uid"),
            "{errs:?}"
        );
    }

    struct Graceful;
    impl RestGracefulDeleteStrategy<ConfigMap> for Graceful {
        fn check_graceful_delete(
            &self,
            _: &RequestContext,
            _: &ConfigMap,
            o: &mut DeleteOptions,
        ) -> bool {
            if o.grace_period_seconds.is_none() {
                o.grace_period_seconds = Some(30);
            }
            true
        }
    }
    struct GracefulDelete;
    impl RestDeleteStrategy<ConfigMap> for GracefulDelete {
        fn graceful(&self) -> Option<&dyn RestGracefulDeleteStrategy<ConfigMap>> {
            Some(&Graceful)
        }
    }
    struct PlainDelete;
    impl RestDeleteStrategy<ConfigMap> for PlainDelete {}

    fn gr() -> GroupResource {
        GroupResource::new("", "configmaps")
    }

    /// Upstream `TestBeforeDelete` (delete_test.go): a non-graceful strategy is
    /// never graceful, whatever the options say.
    #[test]
    fn before_delete_without_a_graceful_strategy_deletes_now() {
        let mut obj = cm("a");
        let mut opts = DeleteOptions {
            grace_period_seconds: Some(10),
            ..zero_delete_options()
        };
        let d = before_delete(
            &PlainDelete,
            &gr(),
            &RequestContext::new(Some("ns")),
            &mut obj,
            &mut opts,
        )
        .unwrap();
        assert_eq!(
            d,
            DeleteDecision {
                graceful: false,
                graceful_pending: false
            }
        );
        assert!(obj.metadata.deletion_timestamp.is_none());
    }

    /// First graceful delete stamps the timestamp and period and bumps a set
    /// generation.
    #[test]
    fn before_delete_first_graceful_delete_stamps_and_bumps_generation() {
        let mut obj = cm("a");
        obj.metadata.generation = Some(2);
        let mut opts = zero_delete_options();
        let d = before_delete(
            &GracefulDelete,
            &gr(),
            &RequestContext::new(Some("ns")),
            &mut obj,
            &mut opts,
        )
        .unwrap();
        assert!(d.graceful && !d.graceful_pending);
        assert_eq!(obj.metadata.deletion_grace_period_seconds, Some(30));
        assert!(obj.metadata.deletion_timestamp.is_some());
        assert_eq!(obj.metadata.generation, Some(3));
    }

    /// A pending graceful delete can be shortened, never lengthened.
    #[test]
    fn before_delete_only_shortens_a_pending_grace_period() {
        let start = now_seconds() + Duration::seconds(30);
        let mut obj = cm("a");
        obj.metadata.deletion_timestamp = Some(start);
        obj.metadata.deletion_grace_period_seconds = Some(30);

        let mut longer = DeleteOptions {
            grace_period_seconds: Some(60),
            ..zero_delete_options()
        };
        let d = before_delete(
            &GracefulDelete,
            &gr(),
            &RequestContext::new(Some("ns")),
            &mut obj,
            &mut longer,
        )
        .unwrap();
        assert!(!d.graceful && d.graceful_pending);
        assert_eq!(obj.metadata.deletion_timestamp, Some(start));

        let mut shorter = DeleteOptions {
            grace_period_seconds: Some(10),
            ..zero_delete_options()
        };
        let d = before_delete(
            &GracefulDelete,
            &gr(),
            &RequestContext::new(Some("ns")),
            &mut obj,
            &mut shorter,
        )
        .unwrap();
        assert!(d.graceful && !d.graceful_pending);
        assert_eq!(obj.metadata.deletion_grace_period_seconds, Some(10));
        assert_eq!(
            obj.metadata.deletion_timestamp,
            Some(start - Duration::seconds(20))
        );
    }

    /// A precondition that does not match the stored object is a Conflict,
    /// worded as upstream words it.
    #[test]
    fn before_delete_checks_preconditions() {
        let mut obj = cm("a");
        obj.metadata.uid = "real".to_string();
        let mut opts = DeleteOptions {
            preconditions: Some(Preconditions {
                uid: Some("stale".to_string()),
                resource_version: None,
            }),
            ..zero_delete_options()
        };
        let err = before_delete(
            &PlainDelete,
            &gr(),
            &RequestContext::new(Some("ns")),
            &mut obj,
            &mut opts,
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Conflict: Operation cannot be fulfilled on configmaps \"a\": the UID in the precondition (stale) does not match the UID in record (real). The object might have been deleted and then recreated"
        );
    }

    /// delete.go:81: `NewInvalid(meta.k8s.io DeleteOptions, "", errs)`.
    #[test]
    fn before_delete_invalid_options_is_new_invalid() {
        let mut obj = cm("a");
        let mut opts = DeleteOptions {
            dry_run: Some(vec!["Maybe".to_string()]),
            ..zero_delete_options()
        };
        let err = before_delete(
            &PlainDelete,
            &gr(),
            &RequestContext::new(Some("ns")),
            &mut obj,
            &mut opts,
        )
        .unwrap_err();
        let Error::Status(st) = err else {
            panic!("expected NewInvalid Status, got {err:?}");
        };
        assert!(
            st.message
                .as_deref()
                .unwrap_or("")
                .starts_with("DeleteOptions.meta.k8s.io \"\" is invalid: dryRun: "),
            "{:?}",
            st.message
        );
        let d = st.details.unwrap();
        assert_eq!(d.kind.as_deref(), Some("DeleteOptions"));
        assert_eq!(d.group.as_deref(), Some("meta.k8s.io"));
    }

    #[test]
    fn group_resource_displays_like_upstream() {
        assert_eq!(
            GroupResource::new("", "configmaps").to_string(),
            "configmaps"
        );
        assert_eq!(
            GroupResource::new("apps", "deployments").to_string(),
            "deployments.apps"
        );
    }
}
