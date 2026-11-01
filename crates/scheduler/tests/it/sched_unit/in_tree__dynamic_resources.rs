//! DynamicResources PreFilter/Filter through the plugin object
//! (`dynamicresources.go:402-`, `:631-`).
use async_trait::async_trait;
use rusternetes_common::resources::{Node, Pod, ResourceClaim, ResourceSlice};
use rusternetes_scheduler::fwk::{
    Code, CycleState, FilterPlugin, Handle, NoHandle, NodeInfo, PreFilterPlugin,
};
use rusternetes_scheduler::in_tree::DynamicResources;
use serde_json::{json, Value};

#[derive(Default)]
struct Dra {
    claims: Vec<ResourceClaim>,
    slices: Vec<ResourceSlice>,
}

#[async_trait]
impl Handle for Dra {
    async fn resource_claim(&self, _ns: &str, name: &str) -> Result<ResourceClaim, String> {
        self.claims
            .iter()
            .find(|c| c.metadata.name == name)
            .cloned()
            .ok_or_else(|| format!("resourceclaim {name:?} not found"))
    }
    async fn resource_slices(&self) -> Result<Vec<ResourceSlice>, String> {
        Ok(self.slices.clone())
    }
}

fn pod(claim: Option<&str>) -> Pod {
    let claims = claim
        .map(|c| json!([{"name": "r", "resourceClaimName": c}]))
        .unwrap_or(Value::Null);
    serde_json::from_value(json!({
        "metadata": {"name": "p", "namespace": "default"},
        "spec": {"containers": [{"name": "c"}], "resourceClaims": claims}
    }))
    .unwrap()
}

fn claim(name: &str, status: Value) -> ResourceClaim {
    serde_json::from_value(json!({
        "metadata": {"name": name, "namespace": "default"},
        "spec": {"devices": {}},
        "status": status
    }))
    .unwrap()
}

fn node(name: &str, zone: &str) -> Node {
    serde_json::from_value(json!({"metadata": {"name": name, "labels": {"zone": zone}}})).unwrap()
}

fn slice(node: &str) -> ResourceSlice {
    serde_json::from_value(json!({
        "metadata": {"name": format!("s-{node}")},
        "spec": {"driver": "d", "pool": {"name": "pool", "generation": 1, "resourceSliceCount": 1},
                 "nodeName": node, "devices": [{"name": "dev0"}]}
    }))
    .unwrap()
}

async fn filter(handle: &Dra, p: &Pod, n: &Node) -> (Code, Vec<String>) {
    let mut state = CycleState::new();
    let (_, s) = DynamicResources
        .pre_filter(&mut state, p, &[], &[], handle)
        .await;
    assert!(s.is_success(), "{s:?}");
    let pods: Vec<Pod> = vec![];
    let s = DynamicResources
        .filter(&state, p, &NodeInfo::new(n, &pods))
        .await;
    (s.code(), s.reasons())
}

#[tokio::test]
async fn pod_without_claims_is_skipped() {
    let (r, s) = DynamicResources
        .pre_filter(&mut CycleState::new(), &pod(None), &[], &[], &NoHandle)
        .await;
    assert!(r.is_none() && s.is_skip());
}

#[tokio::test]
async fn missing_claim_is_unresolvable() {
    let (_, s) = DynamicResources
        .pre_filter(
            &mut CycleState::new(),
            &pod(Some("c")),
            &[],
            &[],
            &Dra::default(),
        )
        .await;
    assert_eq!(s.code(), Code::UnschedulableAndUnresolvable);
}

#[tokio::test]
async fn unallocated_claim_rejects_every_node() {
    let h = Dra {
        claims: vec![claim("c", json!({}))],
        ..Default::default()
    };
    let (code, reasons) = filter(&h, &pod(Some("c")), &node("n", "a")).await;
    assert_eq!(code, Code::Unschedulable);
    assert_eq!(reasons, vec!["cannot allocate all claims"]);
}

#[tokio::test]
async fn allocated_claim_needs_the_node_selector_and_device() {
    let status = json!({"allocation": {
        "devices": {"results": [{"request": "r", "driver": "d", "pool": "pool", "device": "dev0"}]},
        "nodeSelector": {"nodeSelectorTerms": [{"matchExpressions": [
            {"key": "zone", "operator": "In", "values": ["a"]}]}]}
    }});
    let h = Dra {
        claims: vec![claim("c", status)],
        slices: vec![slice("n1")],
    };
    let p = pod(Some("c"));
    assert_eq!(filter(&h, &p, &node("n1", "a")).await.0, Code::Success);
    // Wrong zone.
    assert_eq!(
        filter(&h, &p, &node("n1", "b")).await.0,
        Code::UnschedulableAndUnresolvable
    );
    // Right zone but the device is not advertised for this node.
    assert_eq!(
        filter(&h, &p, &node("n2", "a")).await.0,
        Code::UnschedulableAndUnresolvable
    );
}
