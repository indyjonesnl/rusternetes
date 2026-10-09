//! Stub (red-test commit): the CSI external-provisioner loop (#2882).

use rusternetes_common::resources::PersistentVolumeClaim;
use rusternetes_csi::controller_client::CsiControllerClient;
use rusternetes_storage::Storage;
use std::sync::Arc;

pub struct CsiProvisioner<S: Storage> {
    _storage: Arc<S>,
}

impl<S: Storage + 'static> CsiProvisioner<S> {
    pub fn new(
        storage: Arc<S>,
        _driver_name: impl Into<String>,
        _client: CsiControllerClient,
    ) -> Self {
        Self { _storage: storage }
    }

    pub async fn sync_claim(&self, _claim: &PersistentVolumeClaim) -> anyhow::Result<()> {
        unimplemented!("CSI provisioner")
    }
}
