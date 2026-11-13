//! Ports of
//! `staging/src/k8s.io/cloud-provider/controllers/nodelifecycle/node_lifecycle_controller_test.go`
//! (`Test_NodesDeleted`, `Test_NodesShutdown`, `Test_GetProviderID`) and
//! `staging/src/k8s.io/cloud-provider/node/helpers/taints_test.go`
//! (`TestTaintExists`, `TestRemoveTaint`, `TestDeleteTaint`), run against the
//! fake cloud provider (`fake/fake.go`) and `MemoryStorage` in place of
//! `fake.NewSimpleClientset`.

use chrono::{TimeZone, Utc};
use rusternetes_common::resources::{Event, Node, NodeCondition, NodeSpec, NodeStatus, Taint};
use rusternetes_controller_manager::cloud_provider::fake::Cloud;
use rusternetes_controller_manager::cloud_provider::node_helpers::{
    delete_taint, remove_taint, taint_exists,
};
use rusternetes_controller_manager::cloud_provider::{CloudError, Interface};
use rusternetes_controller_manager::controllers::cloud_node_lifecycle::{
    shutdown_taint, CloudNodeLifecycleController, DELETE_NODE_EVENT,
};
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

fn node(name: &str, provider_id: Option<&str>, ready: Option<&str>, taints: Vec<Taint>) -> Node {
    let mut n = Node::new(name);
    n.spec = Some(NodeSpec {
        pod_cidr: None,
        pod_cidrs: None,
        provider_id: provider_id.map(str::to_string),
        unschedulable: None,
        taints: if taints.is_empty() {
            None
        } else {
            Some(taints)
        },
    });
    if let Some(status) = ready {
        let t = Utc.with_ymd_and_hms(2015, 1, 1, 12, 0, 0).unwrap();
        n.status = Some(NodeStatus {
            conditions: Some(vec![NodeCondition {
                condition_type: "Ready".into(),
                status: status.into(),
                last_heartbeat_time: Some(t),
                last_transition_time: Some(t),
                reason: None,
                message: None,
            }]),
            ..Default::default()
        });
    }
    n
}

fn errs(s: &str) -> Option<CloudError> {
    Some(CloudError::other(s))
}

/// Run `MonitorNodes` once over `existing`; returns the node as stored
/// afterwards (None when deleted) and the storage.
async fn monitor(cloud: Cloud, existing: &Node) -> (Option<Node>, Arc<MemoryStorage>) {
    let storage = Arc::new(MemoryStorage::new());
    storage
        .create(&build_key("nodes", None, &existing.metadata.name), existing)
        .await
        .unwrap();
    let cloud: Arc<dyn Interface> = Arc::new(cloud);
    let c = CloudNodeLifecycleController::new(
        Arc::clone(&storage),
        Some(cloud),
        Duration::from_secs(1),
    )
    .unwrap();
    c.monitor_nodes().await;
    let got = storage
        .get::<Node>(&build_key("nodes", None, &existing.metadata.name))
        .await
        .ok();
    (got, storage)
}

fn same(name: &str, updated: &Node, expected: &Node) {
    assert_eq!(updated.metadata.name, expected.metadata.name, "{name}");
    assert_eq!(
        serde_json::to_value(&updated.spec).unwrap(),
        serde_json::to_value(&expected.spec).unwrap(),
        "{name}: spec"
    );
    assert_eq!(
        serde_json::to_value(&updated.status).unwrap(),
        serde_json::to_value(&expected.status).unwrap(),
        "{name}: status"
    );
}

struct Case {
    name: &'static str,
    cloud: Cloud,
    existing: Node,
    /// `expectedNode`; `None` is `expectedDeleted`.
    expected: Option<Node>,
}

async fn run(cases: Vec<Case>) {
    for c in cases {
        let (got, _) = monitor(c.cloud, &c.existing).await;
        match (&got, &c.expected) {
            (None, None) => {}
            (Some(g), Some(e)) => same(c.name, g, e),
            _ => panic!(
                "{}: deleted={} expected_deleted={}",
                c.name,
                got.is_none(),
                c.expected.is_none()
            ),
        }
    }
}

fn kept(n: &Node) -> Option<Node> {
    Some(n.clone())
}

/// `Test_NodesDeleted`, once with `Instances` and once with `InstancesV2`
/// (upstream's `[instancev2]` cases mirror the plain ones).
#[tokio::test]
async fn nodes_deleted() {
    let mut cases = Vec::new();
    for v2 in [false, true] {
        let base = |c: Cloud| Cloud {
            enable_instances_v2: v2,
            ..c
        };

        cases.push(Case {
            name: "node is not ready and does not exist",
            cloud: base(Cloud {
                exists_by_provider_id: false,
                ..Default::default()
            }),
            existing: node("node0", None, Some("False"), vec![]),
            expected: None,
        });
        let n = node("node0", Some("node0"), Some("False"), vec![]);
        cases.push(Case {
            name: "node is not ready and provider returns err",
            cloud: base(Cloud {
                exists_by_provider_id: false,
                err_by_provider_id: errs("err!"),
                ..Default::default()
            }),
            expected: kept(&n),
            existing: n,
        });
        let n = node("node0", Some("node0"), Some("False"), vec![]);
        cases.push(Case {
            name: "node is not ready but still exists",
            cloud: base(Cloud {
                exists_by_provider_id: true,
                ..Default::default()
            }),
            expected: kept(&n),
            existing: n,
        });
        cases.push(Case {
            name: "node ready condition is unknown, node doesn't exist",
            cloud: base(Cloud {
                exists_by_provider_id: false,
                ..Default::default()
            }),
            existing: node("node0", None, Some("Unknown"), vec![]),
            expected: None,
        });
        let n = node("node0", None, Some("Unknown"), vec![]);
        cases.push(Case {
            name: "node ready condition is unknown, node exists",
            cloud: base(Cloud {
                node_shutdown: false,
                exists_by_provider_id: true,
                ext_id: HashMap::from([("node0".to_string(), "foo://12345".to_string())]),
                ..Default::default()
            }),
            expected: kept(&n),
            existing: n,
        });
        let n = node("node0", Some("node0"), Some("True"), vec![]);
        cases.push(Case {
            name: "node is ready, but provider said it is deleted (maybe a bug in provider)",
            cloud: base(Cloud {
                exists_by_provider_id: false,
                ..Default::default()
            }),
            expected: kept(&n),
            existing: n,
        });
    }
    run(cases).await;
}

/// `Test_NodesShutdown`.
#[tokio::test]
async fn nodes_shutdown() {
    let cases = vec![
        Case {
            name: "node is not ready and was shutdown, but exists",
            cloud: Cloud {
                node_shutdown: true,
                exists_by_provider_id: true,
                ..Default::default()
            },
            expected: Some(node(
                "node0",
                Some("node0"),
                Some("False"),
                vec![shutdown_taint()],
            )),
            existing: node("node0", Some("node0"), Some("False"), vec![]),
        },
        Case {
            name: "node with empty spec providerID is not ready and was shutdown, but exists",
            cloud: Cloud {
                node_shutdown: true,
                exists_by_provider_id: true,
                ext_id: HashMap::from([("node0".to_string(), "foo://12345".to_string())]),
                ..Default::default()
            },
            expected: Some(node(
                "node0",
                Some(""),
                Some("False"),
                vec![shutdown_taint()],
            )),
            existing: node("node0", Some(""), Some("False"), vec![]),
        },
        Case {
            name: "node with non-existing providerID (missing in cloud provider) gets deleted",
            cloud: Cloud {
                ext_id: HashMap::from([("node0".to_string(), String::new())]),
                ..Default::default()
            },
            existing: node("node0", Some(""), Some("False"), vec![]),
            expected: None,
        },
        {
            let n = node("node0", Some(""), Some("False"), vec![]);
            Case {
                name: "node with error when getting providerID does not have shutdown taint",
                cloud: Cloud {
                    ext_id_err: HashMap::from([("node0".to_string(), CloudError::other("err!"))]),
                    ..Default::default()
                },
                expected: kept(&n),
                existing: n,
            }
        },
        Case {
            name: "node with InstanceID returning InstanceNotFound gets deleted",
            cloud: Cloud {
                ext_id_err: HashMap::from([("node0".to_string(), CloudError::InstanceNotFound)]),
                ..Default::default()
            },
            existing: node("node0", Some(""), Some("False"), vec![]),
            expected: None,
        },
        Case {
            name: "node is not ready, but there is error checking if node is shutdown",
            cloud: Cloud {
                node_shutdown: false,
                err_shutdown_by_provider_id: errs("err!"),
                ..Default::default()
            },
            existing: node("node0", None, Some("False"), vec![]),
            expected: None,
        },
        Case {
            name: "node is not ready and is not shutdown",
            cloud: Cloud::default(),
            existing: node("node0", None, Some("False"), vec![]),
            expected: None,
        },
        {
            let n = node("node0", None, Some("True"), vec![]);
            Case {
                name: "node is ready but provider says it's shutdown (maybe a bug by provider)",
                cloud: Cloud {
                    node_shutdown: true,
                    ..Default::default()
                },
                expected: kept(&n),
                existing: n,
            }
        },
        Case {
            name: "node is shutdown but provider says it does not exist",
            cloud: Cloud {
                node_shutdown: true,
                exists_by_provider_id: false,
                ..Default::default()
            },
            existing: node("node0", None, Some("Unknown"), vec![]),
            expected: None,
        },
    ];
    run(cases).await;
}

/// A node that became Ready again loses the shutdown taint
/// (`RemoveTaintOffNode` on the `status == ConditionTrue` branch).
#[tokio::test]
async fn ready_node_has_shutdown_taint_removed() {
    let existing = node("node0", Some("node0"), Some("True"), vec![shutdown_taint()]);
    let (got, _) = monitor(Cloud::default(), &existing).await;
    let got = got.unwrap();
    assert!(got.spec.unwrap().taints.is_none());
}

/// Deleting a node records a `DeletingNode` Normal event whose involvedObject
/// carries `apiVersion` (the assertion in `Test_NodesShutdown`'s event watcher).
#[tokio::test]
async fn delete_records_event_with_api_version() {
    let existing = node("node0", None, Some("False"), vec![]);
    let (got, storage) = monitor(Cloud::default(), &existing).await;
    assert!(got.is_none());
    let events: Vec<Event> = storage.list(&build_prefix("events", None)).await.unwrap();
    let ev = events
        .iter()
        .find(|e| e.reason == DELETE_NODE_EVENT)
        .expect("DeletingNode event");
    assert_eq!(ev.involved_object.api_version.as_deref(), Some("v1"));
    assert_eq!(
        ev.message,
        "Deleting node node0 because it does not exist in the cloud provider"
    );
}

/// `NewCloudNodeLifecycleController`'s validation errors.
#[test]
fn constructor_errors() {
    let storage = Arc::new(MemoryStorage::new());
    let e = CloudNodeLifecycleController::new(Arc::clone(&storage), None, Duration::from_secs(1));
    assert_eq!(e.err().unwrap(), "no cloud provider provided");
    let cloud: Arc<dyn Interface> = Arc::new(Cloud {
        disable_instances: true,
        ..Default::default()
    });
    let e = CloudNodeLifecycleController::new(storage, Some(cloud), Duration::from_secs(1));
    assert_eq!(
        e.err().unwrap(),
        "cloud provider does not support instances"
    );
}

/// `Test_GetProviderID`.
#[tokio::test]
async fn get_provider_id() {
    struct T {
        name: &'static str,
        cloud: Cloud,
        node: Node,
        expected: &'static str,
        err: Option<&'static str>,
    }
    let n0 = |pid: Option<&str>| node("node0", pid, None, vec![]);
    let cases = vec![
        T {
            name: "node initialized with provider ID",
            cloud: Cloud::default(),
            node: n0(Some("fake://12345")),
            expected: "fake://12345",
            err: None,
        },
        T {
            name: "node initialized with provider ID with InstancesV2",
            cloud: Cloud {
                enable_instances_v2: true,
                ..Default::default()
            },
            node: n0(Some("fake://12345")),
            expected: "fake://12345",
            err: None,
        },
        T {
            name: "cloud implemented with Instances",
            cloud: Cloud {
                ext_id: HashMap::from([("node0".to_string(), "12345".to_string())]),
                ..Default::default()
            },
            node: n0(None),
            expected: "fake://12345",
            err: None,
        },
        T {
            name: "cloud implemented with InstancesV2",
            cloud: Cloud {
                enable_instances_v2: true,
                provider_id: HashMap::from([("node0".to_string(), "fake://12345".to_string())]),
                ..Default::default()
            },
            node: n0(None),
            expected: "fake://12345",
            err: None,
        },
        T {
            name: "cloud implemented with InstancesV2 (without providerID)",
            cloud: Cloud {
                enable_instances_v2: true,
                ..Default::default()
            },
            node: n0(None),
            expected: "",
            err: None,
        },
        T {
            name: "cloud implemented with Instances with instance missing",
            cloud: Cloud {
                ext_id_err: HashMap::from([("node0".to_string(), CloudError::InstanceNotFound)]),
                ..Default::default()
            },
            node: n0(None),
            expected: "",
            err: Some("instance not found"),
        },
        T {
            name: "cloud implemented with Instances with unknown error",
            cloud: Cloud {
                ext_id_err: HashMap::from([(
                    "node0".to_string(),
                    CloudError::other("unknown error"),
                )]),
                ..Default::default()
            },
            node: n0(None),
            expected: "",
            err: Some("failed to get instance ID from cloud provider: unknown error"),
        },
    ];
    for t in cases {
        let c = CloudNodeLifecycleController::new(
            Arc::new(MemoryStorage::new()),
            Some(Arc::new(t.cloud) as Arc<dyn Interface>),
            Duration::from_secs(1),
        )
        .unwrap();
        let r = c.get_provider_id(&t.node).await;
        match (r, t.err) {
            (Ok(id), None) => assert_eq!(id, t.expected, "{}", t.name),
            (Err(e), Some(m)) => assert_eq!(e.to_string(), m, "{}", t.name),
            (r, e) => panic!("{}: got {r:?}, want err {e:?}", t.name),
        }
    }
}

fn taint(key: &str, value: &str, effect: &str) -> Taint {
    Taint {
        key: key.into(),
        value: if value.is_empty() {
            None
        } else {
            Some(value.into())
        },
        effect: effect.into(),
        time_added: None,
    }
}

/// `TestTaintExists`.
#[test]
fn taint_exists_cases() {
    let testing = vec![
        taint("foo_1", "bar_1", "NoExecute"),
        taint("foo_2", "bar_2", "NoSchedule"),
    ];
    assert!(taint_exists(
        &testing,
        &taint("foo_1", "bar_1", "NoExecute")
    ));
    assert!(!taint_exists(
        &testing,
        &taint("no_such_key", "bar_1", "NoExecute")
    ));
    assert!(!taint_exists(
        &testing,
        &taint("foo_1", "bar_1", "NoSchedule")
    ));
}

/// `TestRemoveTaint`.
#[test]
fn remove_taint_cases() {
    let mk = |ts: Vec<Taint>| node("n", None, None, ts);
    let (n, r) = remove_taint(
        &mk(vec![taint("foo", "", "NoSchedule")]),
        &taint("foo_1", "", "NoSchedule"),
    );
    assert!(!r);
    assert_eq!(n.spec.unwrap().taints.unwrap().len(), 1);
    let (n, r) = remove_taint(
        &mk(vec![taint("foo", "", "NoSchedule")]),
        &taint("foo", "", "NoSchedule"),
    );
    assert!(r);
    assert!(n.spec.unwrap().taints.is_none());
    let (n, r) = remove_taint(&mk(vec![]), &taint("foo", "", "NoSchedule"));
    assert!(!r);
    assert!(n.spec.unwrap().taints.is_none());
}

/// `TestDeleteTaint`.
#[test]
fn delete_taint_cases() {
    let ts = vec![taint("foo", "", "NoSchedule")];
    let (out, deleted) = delete_taint(&ts, &taint("foo_1", "", "NoSchedule"));
    assert!(!deleted);
    assert_eq!(out.len(), 1);
    let (out, deleted) = delete_taint(&ts, &taint("foo", "", "NoExecute"));
    assert!(!deleted);
    assert_eq!(out.len(), 1);
    let (out, deleted) = delete_taint(&ts, &taint("foo", "", "NoSchedule"));
    assert!(deleted);
    assert!(out.is_empty());
}
