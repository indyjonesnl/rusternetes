//! A `fwk::Handle` over an empty cluster: no Services, controllers or
//! namespaces, as the listers return for a cluster with none.
use async_trait::async_trait;
use k3rs_common::resources::{Namespace, Pod};
use k3rs_scheduler::fwk::Handle;
use k3rs_scheduler::pod_topology_spread_score::DefaultSelectorInputs;

pub struct EmptyListers;

#[async_trait]
impl Handle for EmptyListers {
    async fn default_selector_inputs(&self, _pod: &Pod) -> Result<DefaultSelectorInputs, String> {
        Ok(DefaultSelectorInputs {
            service_selectors: Vec::new(),
            owner: None,
        })
    }

    async fn list_namespaces(&self) -> Result<Vec<Namespace>, String> {
        Ok(Vec::new())
    }
}
