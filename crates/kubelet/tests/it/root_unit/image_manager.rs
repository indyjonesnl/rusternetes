//! Ports of `pkg/kubelet/images/image_manager_test.go` (`TestApplyDefaultImageTag`,
//! the `noFGPullerTestCases` flows of `TestParallelPuller`, `evalCRIPullErr`).
use rusternetes_common::resources::{Event, Pod, PodSpec};
use rusternetes_cri::fake::FakeCri;
use rusternetes_kubelet::credentialprovider::NodeKeyring;
use rusternetes_kubelet::image_manager::*;
use rusternetes_kubelet::lifecycle::reason_from_anyhow;
use rusternetes_storage::{Storage, StorageBackend};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn test_apply_default_image_tag() {
    let sha = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    for (input, output) in [
        ("root", "root:latest".to_string()),
        ("root:tag", "root:tag".to_string()),
        (&format!("root@{sha}") as &str, format!("root@{sha}")),
        (
            &format!("root:latest@{sha}") as &str,
            format!("root:latest@{sha}"),
        ),
        ("root:latest", "root:latest".to_string()),
        // A registry port is not a tag.
        (
            "localhost:5000/app",
            "localhost:5000/app:latest".to_string(),
        ),
    ] {
        assert_eq!(apply_default_image_tag(input).unwrap(), output, "{input}");
    }
}

#[test]
fn invalid_image_names_are_rejected() {
    for bad in ["", "repo/Upper", "a b", "img:", "img@sha256:zz", "a//b"] {
        assert!(apply_default_image_tag(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn eval_cri_pull_err_maps_cri_sentinels() {
    let (msg, kind) = eval_cri_pull_err("img", "RegistryUnavailable: boom");
    assert_eq!(kind, ImageErrorKind::RegistryUnavailable);
    assert_eq!(
        msg,
        "image pull failed for img because the registry is unavailable: boom"
    );
    let (msg, kind) = eval_cri_pull_err("img", "SignatureValidationFailed: bad");
    assert_eq!(kind, ImageErrorKind::SignatureValidationFailed);
    assert_eq!(
        msg,
        "image pull failed for img because the signature validation failed: bad"
    );
    let (msg, kind) = eval_cri_pull_err("img", "denied");
    assert_eq!((msg.as_str(), kind), ("denied", ImageErrorKind::ImagePull));
}

#[test]
fn go_duration_formatting() {
    assert_eq!(go_duration_string(Duration::from_millis(0)), "0s");
    assert_eq!(go_duration_string(Duration::from_millis(250)), "250ms");
    assert_eq!(go_duration_string(Duration::from_millis(1500)), "1.5s");
    assert_eq!(go_duration_string(Duration::from_millis(2000)), "2s");
    assert_eq!(go_duration_string(Duration::from_millis(62_500)), "1m2.5s");
}

struct Env {
    rt: rusternetes_kubelet::cri_runtime::CriContainerRuntime,
    fake: FakeCri,
    storage: Arc<StorageBackend>,
    pod: Pod,
    _tmp: tempfile::TempDir,
}

async fn env() -> Env {
    let storage = Arc::new(StorageBackend::new_memory());
    let fake = FakeCri::new();
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("fake-cri.sock");
    fake.serve(&sock);
    let rt = rusternetes_kubelet::cri_runtime::CriContainerRuntime::connect(
        &sock,
        tmp.path().to_str().unwrap(),
    )
    .await
    .unwrap()
    .with_event_recorder(Arc::clone(&storage))
    .with_node_keyring(NodeKeyring::new(vec![], vec![], Duration::from_secs(300)));
    let mut pod = Pod::new("p", PodSpec::default());
    pod.metadata.namespace = Some("ns".into());
    pod.metadata.uid = "uid".into();
    Env {
        rt,
        fake,
        storage,
        pod,
        _tmp: tmp,
    }
}

async fn events(e: &Env) -> Vec<(String, String, String)> {
    let mut v: Vec<_> = e
        .storage
        .list::<Event>("/registry/events/ns/")
        .await
        .unwrap()
        .into_iter()
        .map(|ev| (ev.reason, format!("{:?}", ev.event_type), ev.message))
        .collect();
    v.sort();
    v
}

#[tokio::test]
async fn image_missing_pulls_with_pulling_and_pulled_events() {
    let e = env().await;
    let r =
        e.rt.ensure_image_ref(
            "missing_image",
            Some("IfNotPresent"),
            Some((&e.pod, "c")),
            None,
        )
        .await
        .unwrap();
    // `GetImageRef`, `PullImage`, `GetImageSize` (image_manager_test.go:111).
    assert_eq!(e.fake.called(), ["ImageStatus", "PullImage", "ImageStatus"]);
    // The default tag is applied to the runtime image spec.
    assert_eq!(r, "missing_image:latest");
    let ev = events(&e).await;
    let reasons: Vec<_> = ev.iter().map(|x| x.0.as_str()).collect();
    assert_eq!(reasons, ["Pulled", "Pulling"]);
    let pulling = ev.iter().find(|x| x.0 == "Pulling").unwrap();
    assert_eq!(pulling.2, r#"Pulling image "missing_image""#);
    let pulled = ev.iter().find(|x| x.0 == "Pulled").unwrap();
    assert!(
        pulled
            .2
            .starts_with(r#"Successfully pulled image "missing_image" in "#)
            && pulled.2.ends_with(" bytes."),
        "{}",
        pulled.2
    );
}

#[tokio::test]
async fn image_present_does_not_pull_and_records_pulled_event() {
    let e = env().await;
    e.fake.add_image("present_image:latest");
    for _ in 0..3 {
        e.rt.ensure_image_ref(
            "present_image",
            Some("IfNotPresent"),
            Some((&e.pod, "")),
            None,
        )
        .await
        .unwrap();
    }
    assert_eq!(e.fake.called(), ["ImageStatus"; 3]);
    let ev = events(&e).await;
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].0, "Pulled");
    assert_eq!(
        ev[0].2,
        r#"Container image "present_image" already present on machine"#
    );
}

#[tokio::test]
async fn pull_always_skips_the_inspect() {
    let e = env().await;
    e.fake.add_image("present_image:latest");
    e.rt.ensure_image_ref("present_image", Some("Always"), None, None)
        .await
        .unwrap();
    assert_eq!(e.fake.called(), ["PullImage", "ImageStatus"]);
}

#[tokio::test]
async fn never_policy_missing_image_is_err_image_never_pull_with_event() {
    let e = env().await;
    let err =
        e.rt.ensure_image_ref("missing_image", Some("Never"), Some((&e.pod, "")), None)
            .await
            .unwrap_err();
    assert_eq!(reason_from_anyhow(&err), Some("ErrImageNeverPull"));
    assert_eq!(e.fake.called(), ["ImageStatus"]);
    let ev = events(&e).await;
    assert_eq!(
        ev,
        [(
            "ErrImageNeverPull".to_string(),
            "Warning".to_string(),
            r#"Container image "missing_image" is not present with pull policy of Never"#
                .to_string()
        )]
    );
}

#[tokio::test]
async fn inspect_failure_is_image_inspect_error() {
    let e = env().await;
    e.fake.fail_image_status("unknown inspectError");
    let err =
        e.rt.ensure_image_ref(
            "missing_image",
            Some("IfNotPresent"),
            Some((&e.pod, "")),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(reason_from_anyhow(&err), Some("ImageInspectError"));
    // Upstream formats the (empty) imageRef, not the image.
    assert_eq!(
        err.to_string(),
        r#"Failed to inspect image "": unknown inspectError"#
    );
    assert_eq!(e.fake.called(), ["ImageStatus"]);
    assert_eq!(events(&e).await[0].0, "InspectFailed");
}

#[tokio::test]
async fn invalid_image_name_is_invalid_image_name() {
    let e = env().await;
    let err =
        e.rt.ensure_image_ref("Bad/Name", Some("IfNotPresent"), Some((&e.pod, "")), None)
            .await
            .unwrap_err();
    assert_eq!(reason_from_anyhow(&err), Some("InvalidImageName"));
    assert!(err
        .to_string()
        .starts_with(r#"Failed to apply default image tag "Bad/Name": "#));
    assert!(e.fake.called().is_empty());
}

#[tokio::test]
async fn pull_failure_is_err_image_pull_with_failed_event() {
    let e = env().await;
    e.fake.fail_pull_image("pull denied");
    let err =
        e.rt.ensure_image_ref(
            "missing_image",
            Some("IfNotPresent"),
            Some((&e.pod, "")),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(reason_from_anyhow(&err), Some("ErrImagePull"));
    assert_eq!(err.to_string(), "pull denied");
    let ev = events(&e).await;
    let failed = ev.iter().find(|x| x.0 == "Failed").unwrap();
    assert_eq!(
        failed.2,
        r#"Failed to pull image "missing_image": pull denied"#
    );
}

#[tokio::test]
async fn registry_unavailable_pull_failure_is_mapped() {
    let e = env().await;
    e.fake.fail_pull_image("RegistryUnavailable: 503");
    let err =
        e.rt.ensure_image_ref("img:1", Some("Always"), None, None)
            .await
            .unwrap_err();
    assert_eq!(reason_from_anyhow(&err), Some("RegistryUnavailable"));
    assert_eq!(
        err.to_string(),
        "image pull failed for img:1 because the registry is unavailable: 503"
    );
}

#[test]
fn back_off_key_and_message() {
    assert_eq!(back_off_key("uid", "img:1"), "uid_img:1");
    let b = PullBackOff::default();
    let t = std::time::Instant::now();
    assert_eq!(b.check("k", "img", t), BackOffCheck::Pull);
    b.record_failure("k", ImageErrorKind::ImagePull, "boom", t);
    assert_eq!(
        b.check("k", "img", t + Duration::from_secs(5)),
        BackOffCheck::BackOff {
            event_message: r#"Back-off pulling image "img""#.into(),
            message: r#"Back-off pulling image "img": ErrImagePull: boom"#.into(),
        }
    );
    // Past the window the stored error is dropped (`delete(m.prevPullErrMsg,
    // backOffKey)`), bounding the map.
    assert_eq!(
        b.check("k", "img", t + Duration::from_secs(10)),
        BackOffCheck::Pull
    );
    assert_eq!(b.prev_errors(), 0);
}

#[tokio::test]
async fn pull_failure_then_retry_is_image_pull_back_off() {
    let e = env().await;
    e.fake.fail_pull_image("pull denied");
    let first =
        e.rt.ensure_image_ref("missing_image", None, Some((&e.pod, "c")), None)
            .await
            .unwrap_err();
    assert_eq!(reason_from_anyhow(&first), Some("ErrImagePull"));
    let pulls = |e: &Env| e.fake.called().iter().filter(|c| *c == "PullImage").count();
    assert_eq!(pulls(&e), 1);

    // Immediate retry: no PullImage, ErrImagePullBackOff carrying the last error.
    let second =
        e.rt.ensure_image_ref("missing_image", None, Some((&e.pod, "c")), None)
            .await
            .unwrap_err();
    assert_eq!(reason_from_anyhow(&second), Some("ImagePullBackOff"));
    assert_eq!(
        second.to_string(),
        r#"Back-off pulling image "missing_image": ErrImagePull: pull denied"#
    );
    assert_eq!(pulls(&e), 1);
    let ev = events(&e).await;
    let bo = ev.iter().find(|x| x.0 == "BackOff").unwrap();
    assert_eq!(bo.1, "Normal");
    assert_eq!(bo.2, r#"Back-off pulling image "missing_image""#);
    assert_eq!(ev.iter().filter(|x| x.0 == "Pulling").count(), 1);

    // A different pod UID (or image) is a different key.
    let mut other = e.pod.clone();
    other.metadata.uid = "other".into();
    let third =
        e.rt.ensure_image_ref("missing_image", None, Some((&other, "c")), None)
            .await
            .unwrap_err();
    assert_eq!(reason_from_anyhow(&third), Some("ErrImagePull"));
    assert_eq!(pulls(&e), 2);
}

#[tokio::test]
async fn successful_pull_runs_gc() {
    let e = env().await;
    e.rt.ensure_image_ref("ok_image", None, Some((&e.pod, "c")), None)
        .await
        .unwrap();
    assert_eq!(e.rt.pull_back_off.tracked(), 0);
    assert_eq!(e.rt.pull_back_off.prev_errors(), 0);
}
