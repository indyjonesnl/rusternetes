//! Job/CronJob update validation derives the pod options from the OLD
//! template (`GetValidationOptionsFromPodTemplate(new, old)`):
//! pkg/registry/batch/job/strategy.go:184 (`validationOptionsForJob`) and
//! cronjob/strategy.go:114,147, versus the create path (`..., nil`).
//! Mirrors the apps test for #3947.
use rusternetes_common::feature_gates::{with_feature, Feature};
use rusternetes_common::resources::workloads::Job;
use rusternetes_common::resources::CronJob;
use rusternetes_common::validation::cronjob::{validate_cron_job_create, validate_cron_job_update};
use rusternetes_common::validation::job::{
    validate_job, validate_job_update, JobValidationOptions,
};
use serde_json::{json, Value};

const RELAXED: &str = "my_search.example.com";

fn template(search: &str) -> Value {
    json!({"metadata": {"labels": {"app": "x"}}, "spec": {
        "restartPolicy": "Never", "dnsPolicy": "ClusterFirst",
        "containers": [{"name": "c", "image": "x", "imagePullPolicy": "IfNotPresent",
            "terminationMessagePolicy": "File"}],
        "dnsConfig": {"searches": [search]}}})
}

fn job(search: &str) -> Job {
    serde_json::from_value(json!({
        "metadata": {"name": "j", "namespace": "ns", "resourceVersion": "1"},
        "spec": {"manualSelector": true, "selector": {"matchLabels": {"app": "x"}},
            "template": template(search)}
    }))
    .unwrap()
}

fn cronjob(search: &str) -> CronJob {
    let mut t = template(search);
    t["metadata"] = json!({});
    serde_json::from_value(json!({
        "metadata": {"name": "c", "namespace": "ns", "resourceVersion": "1"},
        "spec": {"schedule": "* * * * *", "concurrencyPolicy": "Allow",
            "jobTemplate": {"spec": {"template": t}}}
    }))
    .unwrap()
}

#[test]
#[serial_test::serial]
fn job_update_derives_pod_options_from_old_template() {
    let _g = with_feature(Feature::RelaxedDNSSearchValidation, false);
    let opts = JobValidationOptions::default();
    assert!(!validate_job(&job(RELAXED), &opts).is_empty());
    let e = validate_job_update(&job(RELAXED), &job(RELAXED), &opts);
    assert!(!e.iter().any(|e| e.field.contains("dnsConfig")), "{e:?}");
    let e = validate_job_update(&job(RELAXED), &job("a.example.com"), &opts);
    assert!(e.iter().any(|e| e.field.contains("dnsConfig")), "{e:?}");
}

#[test]
#[serial_test::serial]
fn cronjob_update_derives_pod_options_from_old_template() {
    let _g = with_feature(Feature::RelaxedDNSSearchValidation, false);
    assert!(!validate_cron_job_create(&cronjob(RELAXED)).is_empty());
    let e = validate_cron_job_update(&cronjob(RELAXED), &cronjob(RELAXED));
    assert!(e.is_empty(), "{e:?}");
    assert!(!validate_cron_job_update(&cronjob(RELAXED), &cronjob("a.example.com")).is_empty());
}
