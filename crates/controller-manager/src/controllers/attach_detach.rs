//! stub
use anyhow::Result;
use rusternetes_storage::Storage;
use std::sync::Arc;

pub fn attachment_name(_volume_handle: &str, _driver: &str, _node: &str) -> String {
    String::new()
}

pub struct AttachDetachController<S: Storage> {
    #[allow(dead_code)]
    storage: Arc<S>,
}

impl<S: Storage + 'static> AttachDetachController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
    pub async fn reconcile_all(&self) -> Result<()> {
        Ok(())
    }
}
