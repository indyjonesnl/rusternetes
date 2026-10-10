//! The CSI external-attacher (#1460). Skeleton: `reconcile_all` is a no-op
//! until the implementation lands.
use anyhow::Result;
use rusternetes_csi::controller_client::CsiControllerClient;
use rusternetes_storage::Storage;
use std::sync::Arc;

pub struct CsiAttacher<S: Storage> {
    #[allow(dead_code)]
    storage: Arc<S>,
    #[allow(dead_code)]
    driver_name: String,
    #[allow(dead_code)]
    client: CsiControllerClient,
}

impl<S: Storage + 'static> CsiAttacher<S> {
    pub fn new(
        storage: Arc<S>,
        driver_name: impl Into<String>,
        client: CsiControllerClient,
    ) -> Self {
        Self {
            storage,
            driver_name: driver_name.into(),
            client,
        }
    }

    pub async fn reconcile_all(&self) -> Result<()> {
        Ok(())
    }
}
