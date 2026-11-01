//! `RunBindPlugins` (`runtime/framework.go:1514-1550`, `TestBindPlugins`) and
//! `DefaultBinder.Bind` (`plugins/defaultbinder/default_binder.go`).
use async_trait::async_trait;
use rusternetes_common::resources::Pod;
use rusternetes_scheduler::framework_runtime::Framework;
use rusternetes_scheduler::fwk::{BindPlugin, Code, CycleState, Handle, NoHandle, Plugin, Status};
use rusternetes_scheduler::in_tree::DefaultBinder;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

fn pod() -> Pod {
    serde_json::from_value(serde_json::json!({
        "metadata": {"name": "p", "namespace": "default"},
        "spec": {"containers": []}
    }))
    .unwrap()
}

/// A Bind plugin that returns `code` and counts its calls.
struct Fake {
    name: &'static str,
    code: Code,
    calls: Arc<AtomicUsize>,
}

impl Plugin for Fake {
    fn name(&self) -> &'static str {
        self.name
    }
}

#[async_trait]
impl BindPlugin for Fake {
    async fn bind(&self, _: &CycleState, _: &Pod, _: &str, _: &dyn Handle) -> Status {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.code {
            Code::Success => Status::success(),
            Code::Error => Status::from_error("boom".to_string()),
            c => Status::new(c, vec!["why".to_string()]),
        }
    }
}

fn fake(name: &'static str, code: Code) -> (Arc<dyn BindPlugin>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    (
        Arc::new(Fake {
            name,
            code,
            calls: calls.clone(),
        }),
        calls,
    )
}

async fn run(plugins: Vec<Arc<dyn BindPlugin>>) -> Status {
    Framework::default()
        .with_bind_plugins(plugins)
        .run_bind_plugins(&CycleState::new(), &pod(), "n1", &NoHandle)
        .await
}

#[tokio::test]
async fn no_plugins_is_skip() {
    assert_eq!(run(vec![]).await.code(), Code::Skip);
}

#[tokio::test]
async fn skip_moves_to_the_next_plugin_and_first_success_wins() {
    let (a, ca) = fake("a", Code::Skip);
    let (b, cb) = fake("b", Code::Success);
    let (c, cc) = fake("c", Code::Success);
    assert!(run(vec![a, b, c]).await.is_success());
    assert_eq!(ca.load(Ordering::SeqCst), 1);
    assert_eq!(cb.load(Ordering::SeqCst), 1);
    assert_eq!(cc.load(Ordering::SeqCst), 0, "later plugins do not run");
}

#[tokio::test]
async fn every_plugin_skipping_is_skip() {
    let (a, _) = fake("a", Code::Skip);
    let (b, cb) = fake("b", Code::Skip);
    assert_eq!(run(vec![a, b]).await.code(), Code::Skip);
    assert_eq!(cb.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn rejection_is_returned_with_the_plugin_and_stops() {
    let (a, _) = fake("a", Code::Unschedulable);
    let (b, cb) = fake("b", Code::Success);
    let status = run(vec![a, b]).await;
    assert_eq!(status.code(), Code::Unschedulable);
    assert_eq!(status.plugin(), "a");
    assert_eq!(cb.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn error_is_wrapped_with_the_plugin_name() {
    let (a, _) = fake("a", Code::Error);
    let status = run(vec![a]).await;
    assert_eq!(status.code(), Code::Error);
    assert_eq!(status.message(), "running Bind plugin \"a\": boom");
}

/// A Handle that records the binds `DefaultBinder` asks for.
struct Binds(Mutex<Vec<(String, String)>>, Result<(), String>);

#[async_trait]
impl Handle for Binds {
    async fn bind_pod(&self, pod: &Pod, node_name: &str) -> Result<(), String> {
        self.0
            .lock()
            .unwrap()
            .push((pod.metadata.name.clone(), node_name.to_string()));
        self.1.clone()
    }
}

#[tokio::test]
async fn default_binder_binds_through_the_handle() {
    let h = Binds(Mutex::new(Vec::new()), Ok(()));
    let status = DefaultBinder
        .bind(&CycleState::new(), &pod(), "n1", &h)
        .await;
    assert!(status.is_success());
    assert_eq!(*h.0.lock().unwrap(), [("p".to_string(), "n1".to_string())]);

    let h = Binds(Mutex::new(Vec::new()), Err("conflict".to_string()));
    let status = DefaultBinder
        .bind(&CycleState::new(), &pod(), "n1", &h)
        .await;
    assert_eq!(status.code(), Code::Error);
    assert_eq!(status.message(), "conflict");
}

#[test]
fn default_profile_has_the_default_binder() {
    use rusternetes_scheduler::profile::Profile;
    // Bound through the framework the scheduler builds: the DefaultBinder
    // object is the only Bind plugin, so a handle without cluster access
    // fails the bind rather than skipping it.
    let fw = Profile::named("default-scheduler").framework;
    let status = futures::executor::block_on(fw.run_bind_plugins(
        &CycleState::new(),
        &pod(),
        "n1",
        &NoHandle,
    ));
    assert_eq!(status.code(), Code::Error);
    assert_eq!(
        status.message(),
        "running Bind plugin \"DefaultBinder\": no cluster access"
    );
}
