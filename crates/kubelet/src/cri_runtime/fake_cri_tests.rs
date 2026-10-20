//! CRI-less tests: `CriContainerRuntime` against an in-process fake
//! `RuntimeService` (`rusternetes_cri::fake`), covering the stale-incarnation
//! paths of issue #1115 without containerd.
//!
//! Upstream parity: `computePodActions` (`pkg/kubelet/kuberuntime/
//! kuberuntime_manager.go`) decides from the runtime's per-pod container
//! statuses, which are fetched by pod UID. A same-name pod with a NEW uid must
//! not see the old incarnation's containers.

use super::translate::labels;
use super::CriContainerRuntime;
use rusternetes_cri::fake::FakeCri;
use rusternetes_cri::v1::ContainerState;

async fn runtime_on(fake: &FakeCri) -> (CriContainerRuntime, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("fake-cri.sock");
    fake.serve(&sock);
    let rt = CriContainerRuntime::connect(&sock, tmp.path().to_str().unwrap())
        .await
        .unwrap();
    (rt, tmp)
}

#[tokio::test]
async fn container_exists_is_scoped_to_pod_uid_not_name() {
    // StatefulSet replacement: web-0 (uid-old) still has an exited "app"
    // container; web-0 (uid-new) has none yet.
    let fake = FakeCri::new();
    fake.add_container(
        "c-old",
        &[
            (labels::POD_UID, "uid-old"),
            (labels::CONTAINER_NAME, "app"),
        ],
        ContainerState::ContainerExited,
    );
    let (rt, _tmp) = runtime_on(&fake).await;

    assert!(rt.container_exists("uid-old", "app").await);
    assert!(
        !rt.container_exists("uid-new", "app").await,
        "new incarnation must not see the old incarnation's container"
    );
    assert!(
        !rt.container_exists("uid-old", "sidecar").await,
        "container name must also scope the match"
    );
}

#[tokio::test]
async fn is_container_running_counts_created_not_exited_and_scopes_by_uid() {
    let fake = FakeCri::new();
    let l = |uid: &'static str, name: &'static str| {
        [(labels::POD_UID, uid), (labels::CONTAINER_NAME, name)]
    };
    fake.add_container("a", &l("uid-old", "app"), ContainerState::ContainerRunning);
    fake.add_container("b", &l("uid-new", "init"), ContainerState::ContainerCreated);
    fake.add_container("c", &l("uid-new", "done"), ContainerState::ContainerExited);
    let (rt, _tmp) = runtime_on(&fake).await;

    // Old incarnation's running container is invisible to the new uid.
    assert!(rt.is_container_running("uid-old", "app").await.unwrap());
    assert!(!rt.is_container_running("uid-new", "app").await.unwrap());
    // CREATED counts as alive (create->running window; avoids a duplicate start).
    assert!(rt.is_container_running("uid-new", "init").await.unwrap());
    // EXITED is restartable, i.e. not alive.
    assert!(!rt.is_container_running("uid-new", "done").await.unwrap());
}

#[tokio::test]
async fn has_terminated_containers_ignores_other_incarnations() {
    let fake = FakeCri::new();
    fake.add_container(
        "c-old",
        &[
            (labels::POD_UID, "uid-old"),
            (labels::CONTAINER_NAME, "app"),
        ],
        ContainerState::ContainerExited,
    );
    fake.add_container(
        "c-new",
        &[
            (labels::POD_UID, "uid-new"),
            (labels::CONTAINER_NAME, "app"),
        ],
        ContainerState::ContainerRunning,
    );
    let (rt, _tmp) = runtime_on(&fake).await;

    let mut pod_new = rusternetes_common::resources::Pod::new("web-0", Default::default());
    pod_new.metadata.uid = "uid-new".to_string();
    assert!(
        !rt.has_terminated_containers(&pod_new).await,
        "old incarnation's exited container must not mark the new pod terminated"
    );
    let mut pod_old = pod_new.clone();
    pod_old.metadata.uid = "uid-old".to_string();
    assert!(rt.has_terminated_containers(&pod_old).await);
}

// ---- Sandbox/container lifecycle against the fake's RPCs (#1115, #3884) ----
//
// Upstream parity: the fake mirrors `FakeRuntimeService`
// (`staging/src/k8s.io/cri-api/pkg/apis/testing/fake_runtime_service.go`), and
// the teardown sequence under test is `killPodWithSyncResult` ->
// `StopPodSandbox` then `RemovePodSandbox` (`kuberuntime_manager.go`) with
// containers stopped first (`killContainersWithSyncResult`).

use super::translate::sandbox_config;
use rusternetes_cri::v1;
use rusternetes_cri::CriClient;

fn pod(ns: &str, name: &str, uid: &str) -> rusternetes_common::resources::Pod {
    let mut p = rusternetes_common::resources::Pod::new(name, Default::default());
    p.metadata.namespace = Some(ns.to_string());
    p.metadata.uid = uid.to_string();
    p
}

/// A second raw client on the fake's socket, used to set up runtime state the
/// way `start_pod` would (RunPodSandbox + CreateContainer + StartContainer).
async fn setup_client(fake: &FakeCri) -> (CriClient, CriContainerRuntime, tempfile::TempDir) {
    let (rt, tmp) = runtime_on(fake).await;
    let cri = CriClient::connect(tmp.path().join("fake-cri.sock"))
        .await
        .unwrap();
    (cri, rt, tmp)
}

async fn run_pod_with_container(cri: &mut CriClient, p: &rusternetes_common::resources::Pod) {
    let sb_cfg = sandbox_config(p, "/tmp/fake-logs");
    let sb = cri.run_pod_sandbox(sb_cfg.clone(), "").await.unwrap();
    let cfg = v1::ContainerConfig {
        metadata: Some(v1::ContainerMetadata {
            name: "app".into(),
            attempt: 0,
        }),
        labels: [
            (labels::POD_UID.to_string(), p.metadata.uid.clone()),
            (labels::CONTAINER_NAME.to_string(), "app".to_string()),
        ]
        .into(),
        ..Default::default()
    };
    let cid = cri.create_container(&sb, cfg, sb_cfg).await.unwrap();
    cri.start_container(&cid).await.unwrap();
}

#[tokio::test]
async fn duplicate_container_name_is_rejected_which_exists_check_guards() {
    // containerd reserves (name, sandbox, attempt): a second create for the
    // same container collides. `container_exists` is what keeps
    // `computePodActions` from issuing it (#1112).
    let fake = FakeCri::new();
    let (mut cri, rt, _tmp) = setup_client(&fake).await;
    let p = pod("ns", "web-0", "uid-1");
    run_pod_with_container(&mut cri, &p).await;
    assert!(rt.container_exists("uid-1", "app").await);

    let sb = cri.list_pod_sandbox(None).await.unwrap().remove(0);
    let cfg = v1::ContainerConfig {
        metadata: Some(v1::ContainerMetadata {
            name: "app".into(),
            attempt: 0,
        }),
        ..Default::default()
    };
    let err = cri
        .create_container(&sb.id, cfg, sandbox_config(&p, "/tmp/fake-logs"))
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("AlreadyExists"), "{err:?}");
}

#[tokio::test]
async fn stop_and_remove_pod_is_namespace_scoped() {
    let fake = FakeCri::new();
    let (mut cri, rt, _tmp) = setup_client(&fake).await;
    run_pod_with_container(&mut cri, &pod("ns-a", "srv", "uid-a")).await;
    run_pod_with_container(&mut cri, &pod("ns-b", "srv", "uid-b")).await;

    rt.stop_and_remove_pod("ns-a", "srv").await.unwrap();

    // Only ns-b's sandbox and container survive.
    assert_eq!(fake.sandboxes().len(), 1);
    assert!(rt.container_exists("uid-b", "app").await);
    assert!(!rt.container_exists("uid-a", "app").await);
}

#[tokio::test]
async fn stop_and_remove_pod_for_removes_only_that_incarnation() {
    // Same name + namespace, different UID (StatefulSet replacement): tearing
    // down the old incarnation must leave the new one running.
    let fake = FakeCri::new();
    let (mut cri, rt, _tmp) = setup_client(&fake).await;
    let old = pod("ns", "web-0", "uid-old");
    let mut new = old.clone();
    new.metadata.uid = "uid-new".into();
    run_pod_with_container(&mut cri, &old).await;
    run_pod_with_container(&mut cri, &new).await;

    rt.stop_and_remove_pod_for(&old).await.unwrap();

    assert!(!rt.container_exists("uid-old", "app").await);
    assert!(rt.is_container_running("uid-new", "app").await.unwrap());
    assert_eq!(fake.sandboxes().len(), 1);
}

#[tokio::test]
async fn stop_pod_with_grace_period_stops_containers_before_sandbox() {
    let fake = FakeCri::new();
    let (mut cri, rt, _tmp) = setup_client(&fake).await;
    run_pod_with_container(&mut cri, &pod("ns", "web-0", "uid-1")).await;

    rt.stop_pod_with_grace_period("ns", "web-0", 5)
        .await
        .unwrap();

    let called = fake.called();
    let pos = |n: &str| called.iter().rposition(|c| c == n).unwrap();
    assert!(pos("StopContainer") < pos("StopPodSandbox"), "{called:?}");
    assert!(
        pos("StopPodSandbox") < pos("RemovePodSandbox"),
        "{called:?}"
    );
    assert!(fake.sandboxes().is_empty() && fake.containers().is_empty());
}

#[tokio::test]
async fn garbage_collect_containers_removes_only_undesired_sandboxes() {
    let fake = FakeCri::new();
    let (mut cri, rt, _tmp) = setup_client(&fake).await;
    run_pod_with_container(&mut cri, &pod("ns", "keep", "uid-keep")).await;
    run_pod_with_container(&mut cri, &pod("ns", "gone", "uid-gone")).await;

    let existing = std::collections::HashSet::from(["keep".to_string()]);
    assert_eq!(rt.garbage_collect_containers(&existing).await.unwrap(), 1);

    assert!(rt.container_exists("uid-keep", "app").await);
    assert!(!rt.container_exists("uid-gone", "app").await);
}
