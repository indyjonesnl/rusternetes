//! A fake CSI driver: a real gRPC `csi.v1.Node` server on a unix socket, in
//! the role upstream's `fake.NewIdentityClient`/`fake.NewNodeClient`
//! (`pkg/volume/csi/fake/fake_client.go`) play — but over the wire, so the
//! client's request construction and error mapping are exercised too.

use rusternetes_kubelet::volume_plugins::csi_client::proto::node_server::{Node, NodeServer};
use rusternetes_kubelet::volume_plugins::csi_client::proto::*;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

#[derive(Default)]
pub struct Calls {
    pub stage: Vec<NodeStageVolumeRequest>,
    pub unstage: Vec<NodeUnstageVolumeRequest>,
    pub publish: Vec<NodePublishVolumeRequest>,
    pub unpublish: Vec<NodeUnpublishVolumeRequest>,
    pub expand: Vec<NodeExpandVolumeRequest>,
    pub capability_calls: usize,
    /// Stage/publish call order, for NodeStage-before-NodePublish checks.
    pub order: Vec<&'static str>,
    pub node_get_info_calls: usize,
    pub volume_stats: Vec<NodeGetVolumeStatsRequest>,
}

#[derive(Clone, Default)]
pub struct FakeDriver {
    pub calls: Arc<Mutex<Calls>>,
    pub capabilities: Arc<Mutex<Vec<i32>>>,
    /// When set, `NodeStageVolume` fails with this code.
    pub stage_error: Arc<Mutex<Option<tonic::Code>>>,
    /// When set, `NodePublishVolume` fails with this code.
    pub publish_error: Arc<Mutex<Option<tonic::Code>>>,
    /// When set, `NodeExpandVolume` fails with this code.
    pub expand_error: Arc<Mutex<Option<tonic::Code>>>,
    /// `NodeGetInfo`'s answer: `Err` makes the RPC fail.
    pub node_info: Arc<Mutex<Option<Result<NodeGetInfoResponse, tonic::Code>>>>,
    /// `NodeGetVolumeStats`'s answer (`fake.NodeClient.SetNodeVolumeStatsResp`,
    /// `fake_client.go`): `None` answers an empty response.
    pub volume_stats_resp: Arc<Mutex<Option<NodeGetVolumeStatsResponse>>>,
}

impl FakeDriver {
    pub fn with_capabilities(caps: &[node_service_capability::rpc::Type]) -> Self {
        let d = Self::default();
        *d.capabilities.lock().unwrap() = caps.iter().map(|c| *c as i32).collect();
        d
    }
}

#[tonic::async_trait]
impl Node for FakeDriver {
    async fn node_stage_volume(
        &self,
        r: Request<NodeStageVolumeRequest>,
    ) -> Result<Response<NodeStageVolumeResponse>, Status> {
        {
            let mut calls = self.calls.lock().unwrap();
            calls.stage.push(r.into_inner());
            calls.order.push("stage");
        }
        if let Some(code) = *self.stage_error.lock().unwrap() {
            return Err(Status::new(code, "fake stage failure"));
        }
        Ok(Response::new(NodeStageVolumeResponse {}))
    }
    async fn node_unstage_volume(
        &self,
        r: Request<NodeUnstageVolumeRequest>,
    ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        self.calls.lock().unwrap().unstage.push(r.into_inner());
        Ok(Response::new(NodeUnstageVolumeResponse {}))
    }
    async fn node_publish_volume(
        &self,
        r: Request<NodePublishVolumeRequest>,
    ) -> Result<Response<NodePublishVolumeResponse>, Status> {
        let req = r.into_inner();
        // A conformant driver creates the target path; do the same so the
        // kubelet-side post-publish steps see a directory.
        let _ = std::fs::create_dir_all(&req.target_path);
        {
            let mut calls = self.calls.lock().unwrap();
            calls.publish.push(req);
            calls.order.push("publish");
        }
        if let Some(code) = *self.publish_error.lock().unwrap() {
            return Err(Status::new(code, "fake publish failure"));
        }
        Ok(Response::new(NodePublishVolumeResponse {}))
    }
    async fn node_unpublish_volume(
        &self,
        r: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        self.calls.lock().unwrap().unpublish.push(r.into_inner());
        Ok(Response::new(NodeUnpublishVolumeResponse {}))
    }
    /// `fake.NodeClient.NodeExpandVolume` (`pkg/volume/csi/fake/fake_client.go:319-343`).
    async fn node_expand_volume(
        &self,
        r: Request<NodeExpandVolumeRequest>,
    ) -> Result<Response<NodeExpandVolumeResponse>, Status> {
        let req = r.into_inner();
        if let Some(code) = *self.expand_error.lock().unwrap() {
            return Err(Status::new(code, "fake expand failure"));
        }
        let required = req.capacity_range.as_ref().map_or(0, |c| c.required_bytes);
        self.calls.lock().unwrap().expand.push(req);
        Ok(Response::new(NodeExpandVolumeResponse {
            capacity_bytes: required,
        }))
    }
    async fn node_get_volume_stats(
        &self,
        r: Request<NodeGetVolumeStatsRequest>,
    ) -> Result<Response<NodeGetVolumeStatsResponse>, Status> {
        self.calls.lock().unwrap().volume_stats.push(r.into_inner());
        Ok(Response::new(
            self.volume_stats_resp
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_default(),
        ))
    }
    async fn node_get_info(
        &self,
        _r: Request<NodeGetInfoRequest>,
    ) -> Result<Response<NodeGetInfoResponse>, Status> {
        self.calls.lock().unwrap().node_get_info_calls += 1;
        match self.node_info.lock().unwrap().clone() {
            Some(Ok(r)) => Ok(Response::new(r)),
            Some(Err(code)) => Err(Status::new(code, "fake node info failure")),
            None => Ok(Response::new(NodeGetInfoResponse::default())),
        }
    }
    async fn node_get_capabilities(
        &self,
        _r: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        self.calls.lock().unwrap().capability_calls += 1;
        let capabilities = self
            .capabilities
            .lock()
            .unwrap()
            .iter()
            .map(|t| NodeServiceCapability {
                r#type: Some(node_service_capability::Type::Rpc(
                    node_service_capability::Rpc { r#type: *t },
                )),
            })
            .collect();
        Ok(Response::new(NodeGetCapabilitiesResponse { capabilities }))
    }
}

/// Serve `driver` on `socket` until the returned handle is dropped.
pub fn serve(driver: FakeDriver, socket: &std::path::Path) -> tokio::task::JoinHandle<()> {
    let listener = tokio::net::UnixListener::bind(socket).unwrap();
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(NodeServer::new(driver))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    })
}
