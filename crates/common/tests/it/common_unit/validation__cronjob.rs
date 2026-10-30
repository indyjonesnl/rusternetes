//! Unit tests moved from `src/validation/cronjob.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use once_cell::sync::Lazy;
    pub use regex::Regex;
    pub use rusternetes_common::resources::{CronJob, CronJobSpec};
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::job::validate_job_template_spec;
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_nonnegative_field, validate_object_meta,
        validate_object_meta_update,
    };
}

mod time_zone_tests {
    use rusternetes_common::validation::cronjob::validate_time_zone;
    use rusternetes_common::validation::field::Path;

    fn check(tz: Option<&str>) -> Vec<String> {
        validate_time_zone(tz, &Path::new("spec").child("timeZone"))
            .into_iter()
            .map(|e| e.detail)
            .collect()
    }

    #[test]
    fn nil_and_valid_zones_pass() {
        assert!(check(None).is_empty());
        assert!(check(Some("UTC")).is_empty());
        assert!(check(Some("America/New_York")).is_empty());
        assert!(check(Some("Europe/Amsterdam")).is_empty());
    }

    #[test]
    fn empty_string_rejected() {
        assert!(!check(Some("")).is_empty());
    }

    #[test]
    fn syntactically_valid_but_unknown_zone_rejected() {
        // The naming-character rules pass, but the zone is not in the tz DB.
        let errs = check(Some("Foo/Bar"));
        assert!(
            errs.iter().any(|d| d.contains("unknown time zone Foo/Bar")),
            "{errs:?}"
        );
    }

    #[test]
    fn local_rejected_once() {
        let errs = check(Some("Local"));
        assert_eq!(
            errs.len(),
            1,
            "Local must yield exactly one error: {errs:?}"
        );
        assert!(errs[0].contains("explicit time zone"), "{errs:?}");
    }

    #[test]
    fn bad_naming_characters_rejected() {
        let errs = check(Some("../etc"));
        assert!(
            errs.iter().any(|d| d.contains("unknown time zone")),
            "{errs:?}"
        );
    }
}

mod parse_schedule_tests {
    use rusternetes_common::validation::cronjob::parse_schedule;

    #[test]
    fn an_inline_tz_prefix_is_stripped() {
        assert!(parse_schedule("CRON_TZ=UTC */5 * * * *").is_ok());
        assert!(parse_schedule("TZ=Europe/Amsterdam 0 3 * * *").is_ok());
        let err = parse_schedule("TZ=Not/AZone 0 3 * * *").unwrap_err();
        assert!(err.starts_with("provided bad location Not/AZone"), "{err}");
        assert!(parse_schedule("TZ=UTC").is_err());
    }
}

mod update_tests {
    use rusternetes_common::resources::CronJob;
    use rusternetes_common::validation::cronjob::{
        validate_cron_job_create, validate_cron_job_update,
    };

    fn cron_job(name: &str) -> CronJob {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "batch/v1", "kind": "CronJob",
            "metadata": {"name": name, "namespace": "default", "resourceVersion": "1"},
            "spec": {
                "schedule": "*/5 * * * *",
                "concurrencyPolicy": "Allow",
                "jobTemplate": {"spec": {"template": {"spec": {
                    "containers": [{"name": "c", "image": "i",
                                    "imagePullPolicy": "IfNotPresent",
                                    "terminationMessagePolicy": "File"}],
                    "restartPolicy": "OnFailure",
                    "dnsPolicy": "ClusterFirst"
                }}}}
            }
        }))
        .unwrap()
    }

    fn fields(errs: &[rusternetes_common::validation::field::Error]) -> Vec<String> {
        errs.iter()
            .map(|e| format!("{}: {}", e.field, e.detail))
            .collect()
    }

    /// validation_test.go `TestValidateCronJobSpec`: a name over 52 characters
    /// fails create, but an update of an existing one is allowed.
    #[test]
    fn the_name_cap_is_create_only() {
        let long = "a".repeat(53);
        let old = cron_job(&long);
        let errs = validate_cron_job_create(&old);
        assert!(
            fields(&errs)
                .iter()
                .any(|e| e.contains("must be no more than 52 characters")),
            "{errs:?}"
        );
        let mut new = old.clone();
        new.spec.suspend = Some(true);
        let errs = validate_cron_job_update(&new, &old);
        assert!(errs.is_empty(), "{:?}", fields(&errs));
    }

    #[test]
    fn create_validates_object_meta() {
        let mut cj = cron_job("c");
        cj.metadata.namespace = None;
        let errs = validate_cron_job_create(&cj);
        assert!(
            fields(&errs)
                .iter()
                .any(|e| e.starts_with("metadata.namespace")),
            "{errs:?}"
        );
    }

    /// `validateScheduleFormat` (validation.go:833-848): an inline TZ the old
    /// schedule already had is tolerated, unless combined with `timeZone`.
    #[test]
    fn an_inline_tz_is_grandfathered_on_update() {
        let mut old = cron_job("c");
        old.spec.schedule = "CRON_TZ=UTC 0 * * * *".into();
        assert!(!validate_cron_job_create(&old).is_empty());

        let mut new = old.clone();
        new.spec.suspend = Some(true);
        let errs = validate_cron_job_update(&new, &old);
        assert!(
            !fields(&errs).iter().any(|e| e.contains("TZ")),
            "{:?}",
            fields(&errs)
        );

        new.spec.time_zone = Some("UTC".into());
        let errs = validate_cron_job_update(&new, &old);
        assert!(
            fields(&errs)
                .iter()
                .any(|e| e.contains("cannot use both timeZone field and TZ or CRON_TZ")),
            "{:?}",
            fields(&errs)
        );

        let plain = cron_job("c");
        let mut added = plain.clone();
        added.spec.schedule = "TZ=UTC 0 * * * *".into();
        let errs = validate_cron_job_update(&added, &plain);
        assert!(
            fields(&errs)
                .iter()
                .any(|e| e.contains("use timeZone field instead")),
            "{:?}",
            fields(&errs)
        );
    }

    /// validation.go:797-799: an unchanged `timeZone` is not revalidated.
    #[test]
    fn an_unchanged_time_zone_is_not_revalidated() {
        let mut old = cron_job("c");
        old.spec.time_zone = Some("Not/AZone".into());
        let mut new = old.clone();
        new.spec.suspend = Some(true);
        assert!(validate_cron_job_update(&new, &old).is_empty());
        new.spec.time_zone = Some("Also/NotAZone".into());
        assert!(!validate_cron_job_update(&new, &old).is_empty());
    }

    /// `validateConcurrencyPolicy` (validation.go:818-831).
    #[test]
    fn a_cleared_concurrency_policy_is_required() {
        let old = cron_job("c");
        let mut new = old.clone();
        new.spec.concurrency_policy = None;
        let errs = validate_cron_job_update(&new, &old);
        assert!(
            fields(&errs)
                .iter()
                .any(|e| e.starts_with("spec.concurrencyPolicy")),
            "{errs:?}"
        );
    }
}
