//! In-process fake CRI `RuntimeService` backed by an in-memory container and
//! sandbox store, served over a unix socket. Lets the kubelet's CRI-facing
//! paths (stale-incarnation exists-checks, same-name sync races) run in a unit
//! test with no containerd (#1115).
//!
//! Implemented: the `ImageService` (`ListImages`, `ImageStatus`, `PullImage`,
//! `RemoveImage`, `ImageFsInfo`), the list RPCs plus the sandbox/container lifecycle RPCs
//! (`RunPodSandbox`, `StopPodSandbox`, `RemovePodSandbox`, `CreateContainer`,
//! `StartContainer`, `StopContainer`, `RemoveContainer`). Every other RPC
//! answers `UNIMPLEMENTED`, like a runtime that lacks the feature.
//!
//! Ported from upstream's `FakeRuntimeService`
//! (`staging/src/k8s.io/cri-api/pkg/apis/testing/fake_runtime_service.go`:
//! `RunPodSandbox`, `StopPodSandbox`, `RemovePodSandbox`, `CreateContainer`,
//! `StartContainer`, `StopContainer`, `RemoveContainer`) and served the way
//! `cri-client/pkg/fake/fake_runtime.go` (`RemoteRuntime`) serves it. One
//! deliberate deviation: upstream derives the ID from the metadata, so a
//! duplicate silently overwrites; here IDs are unique and a duplicate name is
//! rejected with `AlreadyExists`, which is what containerd's CRI plugin does
//! (`containerNameIndex.Reserve` / `sandboxNameIndex.Reserve`) and is the
//! collision `computePodActions` must avoid (#1112). List filtering
//! follows the CRI proto's `label_selector` contract (`proto/runtime/v1/api.proto`,
//! `ContainerFilter`): "Only api.MatchLabels is supported for now and the
//! requirements are ANDed" — i.e. exact key/value match, all pairs required.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use tonic::{Request, Response, Status};

use crate::v1;
use crate::v1::image_service_server::{ImageService, ImageServiceServer};
use crate::v1::runtime_service_server::{RuntimeService, RuntimeServiceServer};

/// Shared, mutable runtime state a test can seed and inspect.
#[derive(Default)]
struct FakeState {
    containers: Vec<v1::Container>,
    sandboxes: Vec<v1::PodSandbox>,
    next_id: u64,
    /// RPC names in call order (upstream `FakeRuntimeService.Called`).
    called: Vec<String>,
    /// Image store (upstream `FakeImageService.Images`), keyed by image name.
    images: HashMap<String, v1::Image>,
}

impl FakeState {
    fn record(&mut self, rpc: &str) {
        self.called.push(rpc.to_string());
    }

    fn new_id(&mut self, prefix: &str) -> String {
        self.next_id += 1;
        format!("{prefix}-{}", self.next_id)
    }
}

/// Handle to the fake runtime: seed state, then point a `CriClient` at the
/// socket passed to [`FakeCri::serve`].
#[derive(Clone, Default)]
pub struct FakeCri {
    state: Arc<Mutex<FakeState>>,
}

fn labels_match(have: &HashMap<String, String>, want: &HashMap<String, String>) -> bool {
    want.iter().all(|(k, v)| have.get(k) == Some(v))
}

impl FakeCri {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed a container with the given labels.
    pub fn add_container(&self, id: &str, labels: &[(&str, &str)], state: v1::ContainerState) {
        self.state.lock().unwrap().containers.push(v1::Container {
            id: id.to_string(),
            state: state as i32,
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..Default::default()
        });
    }

    /// Seed a pod sandbox with the given labels.
    pub fn add_sandbox(&self, id: &str, labels: &[(&str, &str)]) {
        self.state.lock().unwrap().sandboxes.push(v1::PodSandbox {
            id: id.to_string(),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..Default::default()
        });
    }

    /// RPC names received so far, in order (upstream `Called`).
    pub fn called(&self) -> Vec<String> {
        self.state.lock().unwrap().called.clone()
    }

    /// `(id, state)` of every container currently in the store.
    pub fn containers(&self) -> Vec<(String, v1::ContainerState)> {
        let st = self.state.lock().unwrap();
        st.containers
            .iter()
            .map(|c| (c.id.clone(), v1::ContainerState::try_from(c.state).unwrap()))
            .collect()
    }

    /// `(id, state)` of every sandbox currently in the store.
    pub fn sandboxes(&self) -> Vec<(String, v1::PodSandboxState)> {
        let st = self.state.lock().unwrap();
        st.sandboxes
            .iter()
            .map(|s| {
                (
                    s.id.clone(),
                    v1::PodSandboxState::try_from(s.state).unwrap(),
                )
            })
            .collect()
    }

    /// Bind `socket` and serve the fake until the returned task is aborted.
    pub fn serve(&self, socket: &Path) -> tokio::task::JoinHandle<()> {
        let listener = tokio::net::UnixListener::bind(socket).expect("bind fake CRI socket");
        let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
        let svc = RuntimeServiceServer::new(self.clone());
        let img = ImageServiceServer::new(self.clone());
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(svc)
                .add_service(img)
                .serve_with_incoming(incoming)
                .await;
        })
    }
}

/// `#[async_trait]` cannot see through a macro invoked inside the impl, so the
/// whole impl is generated by one macro: the real handlers are passed in
/// verbatim and the remaining RPCs are stamped out as `UNIMPLEMENTED`.
macro_rules! fake_runtime_service {
    (
        handlers { $($handlers:tt)* }
        stubs { $($name:ident: $req:ident => $resp:ident),* $(,)? }
    ) => {
        #[tonic::async_trait]
        impl RuntimeService for FakeCri {
            $($handlers)*
            $(
                async fn $name(
                    &self,
                    _request: Request<v1::$req>,
                ) -> Result<Response<v1::$resp>, Status> {
                    Err(Status::unimplemented(stringify!($name)))
                }
            )*
        }
    };
}

fake_runtime_service! {
    handlers {
    async fn list_containers(
        &self,
        request: Request<v1::ListContainersRequest>,
    ) -> Result<Response<v1::ListContainersResponse>, Status> {
        let filter = request.into_inner().filter.unwrap_or_default();
        let st = self.state.lock().unwrap();
        let containers = st
            .containers
            .iter()
            .filter(|c| filter.id.is_empty() || c.id == filter.id)
            .filter(|c| {
                filter.pod_sandbox_id.is_empty() || c.pod_sandbox_id == filter.pod_sandbox_id
            })
            .filter(|c| labels_match(&c.labels, &filter.label_selector))
            .cloned()
            .collect();
        Ok(Response::new(v1::ListContainersResponse { containers }))
    }

    async fn list_pod_sandbox(
        &self,
        request: Request<v1::ListPodSandboxRequest>,
    ) -> Result<Response<v1::ListPodSandboxResponse>, Status> {
        let filter = request.into_inner().filter.unwrap_or_default();
        let st = self.state.lock().unwrap();
        let items = st
            .sandboxes
            .iter()
            .filter(|s| filter.id.is_empty() || s.id == filter.id)
            .filter(|s| filter.state.as_ref().is_none_or(|v| s.state == v.state))
            .filter(|s| labels_match(&s.labels, &filter.label_selector))
            .cloned()
            .collect();
        Ok(Response::new(v1::ListPodSandboxResponse { items }))
    }

    /// `RunPodSandbox`: new sandbox in `SANDBOX_READY`. A second sandbox with
    /// the same (name, namespace, uid, attempt) is `AlreadyExists`, as with
    /// containerd's `sandboxNameIndex.Reserve`.
    async fn run_pod_sandbox(
        &self,
        request: Request<v1::RunPodSandboxRequest>,
    ) -> Result<Response<v1::RunPodSandboxResponse>, Status> {
        let config = request.into_inner().config.unwrap_or_default();
        let meta = config.metadata.clone().unwrap_or_default();
        let mut st = self.state.lock().unwrap();
        st.record("RunPodSandbox");
        if st.sandboxes.iter().any(|s| s.metadata.as_ref() == Some(&meta)) {
            return Err(Status::already_exists(format!(
                "failed to reserve sandbox name \"{}_{}_{}_{}\"",
                meta.name, meta.namespace, meta.uid, meta.attempt
            )));
        }
        let id = st.new_id("sandbox");
        st.sandboxes.push(v1::PodSandbox {
            id: id.clone(),
            metadata: Some(meta),
            state: v1::PodSandboxState::SandboxReady as i32,
            labels: config.labels,
            annotations: config.annotations,
            ..Default::default()
        });
        Ok(Response::new(v1::RunPodSandboxResponse { pod_sandbox_id: id }))
    }

    /// `StopPodSandbox`: NOTREADY, and the sandbox's containers exit.
    async fn stop_pod_sandbox(
        &self,
        request: Request<v1::StopPodSandboxRequest>,
    ) -> Result<Response<v1::StopPodSandboxResponse>, Status> {
        let id = request.into_inner().pod_sandbox_id;
        let mut st = self.state.lock().unwrap();
        st.record("StopPodSandbox");
        let Some(sb) = st.sandboxes.iter_mut().find(|s| s.id == id) else {
            return Err(Status::not_found(format!("pod sandbox {id} not found")));
        };
        sb.state = v1::PodSandboxState::SandboxNotready as i32;
        for c in st.containers.iter_mut().filter(|c| c.pod_sandbox_id == id) {
            c.state = v1::ContainerState::ContainerExited as i32;
        }
        Ok(Response::new(v1::StopPodSandboxResponse {}))
    }

    /// `RemovePodSandbox`: drops the sandbox and its containers. Removing an
    /// unknown sandbox succeeds (CRI: no such sandbox is a no-op; upstream's
    /// fake `delete`s unconditionally).
    async fn remove_pod_sandbox(
        &self,
        request: Request<v1::RemovePodSandboxRequest>,
    ) -> Result<Response<v1::RemovePodSandboxResponse>, Status> {
        let id = request.into_inner().pod_sandbox_id;
        let mut st = self.state.lock().unwrap();
        st.record("RemovePodSandbox");
        st.sandboxes.retain(|s| s.id != id);
        st.containers.retain(|c| c.pod_sandbox_id != id);
        Ok(Response::new(v1::RemovePodSandboxResponse {}))
    }

    /// `CreateContainer`: new container in `CONTAINER_CREATED`. A duplicate
    /// (sandbox, name, attempt) is `AlreadyExists` like containerd's
    /// `containerNameIndex.Reserve`.
    async fn create_container(
        &self,
        request: Request<v1::CreateContainerRequest>,
    ) -> Result<Response<v1::CreateContainerResponse>, Status> {
        let req = request.into_inner();
        let config = req.config.unwrap_or_default();
        let meta = config.metadata.clone().unwrap_or_default();
        let mut st = self.state.lock().unwrap();
        st.record("CreateContainer");
        if !st.sandboxes.iter().any(|s| s.id == req.pod_sandbox_id) {
            return Err(Status::not_found(format!(
                "pod sandbox {} not found",
                req.pod_sandbox_id
            )));
        }
        if let Some(existing) = st
            .containers
            .iter()
            .find(|c| c.pod_sandbox_id == req.pod_sandbox_id && c.metadata.as_ref() == Some(&meta))
        {
            return Err(Status::already_exists(format!(
                "failed to reserve container name \"{}_{}\": name is reserved for \"{}\"",
                meta.name, meta.attempt, existing.id
            )));
        }
        let id = st.new_id("container");
        st.containers.push(v1::Container {
            id: id.clone(),
            pod_sandbox_id: req.pod_sandbox_id,
            metadata: Some(meta),
            image: config.image,
            state: v1::ContainerState::ContainerCreated as i32,
            labels: config.labels,
            annotations: config.annotations,
            ..Default::default()
        });
        Ok(Response::new(v1::CreateContainerResponse { container_id: id }))
    }

    async fn start_container(
        &self,
        request: Request<v1::StartContainerRequest>,
    ) -> Result<Response<v1::StartContainerResponse>, Status> {
        let id = request.into_inner().container_id;
        let mut st = self.state.lock().unwrap();
        st.record("StartContainer");
        let Some(c) = st.containers.iter_mut().find(|c| c.id == id) else {
            return Err(Status::not_found(format!("container {id} not found")));
        };
        c.state = v1::ContainerState::ContainerRunning as i32;
        Ok(Response::new(v1::StartContainerResponse {}))
    }

    async fn stop_container(
        &self,
        request: Request<v1::StopContainerRequest>,
    ) -> Result<Response<v1::StopContainerResponse>, Status> {
        let id = request.into_inner().container_id;
        let mut st = self.state.lock().unwrap();
        st.record("StopContainer");
        let Some(c) = st.containers.iter_mut().find(|c| c.id == id) else {
            return Err(Status::not_found(format!("container {id} not found")));
        };
        c.state = v1::ContainerState::ContainerExited as i32;
        Ok(Response::new(v1::StopContainerResponse {}))
    }

    async fn remove_container(
        &self,
        request: Request<v1::RemoveContainerRequest>,
    ) -> Result<Response<v1::RemoveContainerResponse>, Status> {
        let id = request.into_inner().container_id;
        let mut st = self.state.lock().unwrap();
        st.record("RemoveContainer");
        st.containers.retain(|c| c.id != id);
        Ok(Response::new(v1::RemoveContainerResponse {}))
    }

        type GetContainerEventsStream =
            tokio_stream::Empty<Result<v1::ContainerEventResponse, Status>>;

        async fn get_container_events(
            &self,
            _request: Request<v1::GetEventsRequest>,
        ) -> Result<Response<Self::GetContainerEventsStream>, Status> {
            Err(Status::unimplemented("get_container_events"))
        }
    }
    stubs {
        version: VersionRequest => VersionResponse,
        pod_sandbox_status: PodSandboxStatusRequest => PodSandboxStatusResponse,
        container_status: ContainerStatusRequest => ContainerStatusResponse,
        update_container_resources: UpdateContainerResourcesRequest => UpdateContainerResourcesResponse,
        reopen_container_log: ReopenContainerLogRequest => ReopenContainerLogResponse,
        exec_sync: ExecSyncRequest => ExecSyncResponse,
        exec: ExecRequest => ExecResponse,
        attach: AttachRequest => AttachResponse,
        port_forward: PortForwardRequest => PortForwardResponse,
        container_stats: ContainerStatsRequest => ContainerStatsResponse,
        list_container_stats: ListContainerStatsRequest => ListContainerStatsResponse,
        pod_sandbox_stats: PodSandboxStatsRequest => PodSandboxStatsResponse,
        list_pod_sandbox_stats: ListPodSandboxStatsRequest => ListPodSandboxStatsResponse,
        update_runtime_config: UpdateRuntimeConfigRequest => UpdateRuntimeConfigResponse,
        status: StatusRequest => StatusResponse,
        checkpoint_container: CheckpointContainerRequest => CheckpointContainerResponse,
        list_metric_descriptors: ListMetricDescriptorsRequest => ListMetricDescriptorsResponse,
        list_pod_sandbox_metrics: ListPodSandboxMetricsRequest => ListPodSandboxMetricsResponse,
        runtime_config: RuntimeConfigRequest => RuntimeConfigResponse,
        update_pod_sandbox_resources: UpdatePodSandboxResourcesRequest => UpdatePodSandboxResourcesResponse,
    }
}

/// Image service, ported from upstream `FakeImageService`
/// (`staging/src/k8s.io/cri-api/pkg/apis/testing/fake_image_service.go`:
/// `ListImages`, `ImageStatus`, `PullImage`, `RemoveImage`, `ImageFsInfo`).
/// The image ID is the image name, as in `PullImage`/`makeFakeImage`; an
/// absent image yields an `ImageStatusResponse` with no image.
#[tonic::async_trait]
impl ImageService for FakeCri {
    async fn list_images(
        &self,
        request: Request<v1::ListImagesRequest>,
    ) -> Result<Response<v1::ListImagesResponse>, Status> {
        let filter = request.into_inner().filter.and_then(|f| f.image);
        let mut st = self.state.lock().unwrap();
        st.record("ListImages");
        let images = st
            .images
            .values()
            .filter(|i| {
                filter
                    .as_ref()
                    .is_none_or(|f| i.repo_tags.contains(&f.image))
            })
            .cloned()
            .collect();
        Ok(Response::new(v1::ListImagesResponse { images }))
    }

    async fn image_status(
        &self,
        request: Request<v1::ImageStatusRequest>,
    ) -> Result<Response<v1::ImageStatusResponse>, Status> {
        let spec = request.into_inner().image.unwrap_or_default();
        let mut st = self.state.lock().unwrap();
        st.record("ImageStatus");
        Ok(Response::new(v1::ImageStatusResponse {
            image: st.images.get(&spec.image).cloned(),
            ..Default::default()
        }))
    }

    async fn pull_image(
        &self,
        request: Request<v1::PullImageRequest>,
    ) -> Result<Response<v1::PullImageResponse>, Status> {
        let spec = request.into_inner().image.unwrap_or_default();
        let mut st = self.state.lock().unwrap();
        st.record("PullImage");
        let id = spec.image.clone();
        st.images.entry(id.clone()).or_insert_with(|| v1::Image {
            id: id.clone(),
            repo_tags: vec![spec.image.clone()],
            spec: Some(spec),
            ..Default::default()
        });
        Ok(Response::new(v1::PullImageResponse { image_ref: id }))
    }

    async fn remove_image(
        &self,
        request: Request<v1::RemoveImageRequest>,
    ) -> Result<Response<v1::RemoveImageResponse>, Status> {
        let spec = request.into_inner().image.unwrap_or_default();
        let mut st = self.state.lock().unwrap();
        st.record("RemoveImage");
        st.images.remove(&spec.image);
        Ok(Response::new(v1::RemoveImageResponse {}))
    }

    async fn image_fs_info(
        &self,
        _request: Request<v1::ImageFsInfoRequest>,
    ) -> Result<Response<v1::ImageFsInfoResponse>, Status> {
        self.state.lock().unwrap().record("ImageFsInfo");
        Ok(Response::new(v1::ImageFsInfoResponse::default()))
    }
}
