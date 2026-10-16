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
