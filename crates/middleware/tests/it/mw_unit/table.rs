//! Unit tests moved from `src/table.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::types::ObjectMeta;
    pub use serde::{Deserialize, Serialize};
}

mod tests {
    use super::__parent::*;
    use rusternetes_middleware::table::*;
    use serde_json::json;

    #[test]
    fn test_table_creation() {
        let table = Table::new()
            .add_column("NAME", "string", "name", "Resource name", 0)
            .add_column("AGE", "string", "", "Resource age", 0);

        assert_eq!(table.kind, "Table");
        assert_eq!(table.api_version, "meta.k8s.io/v1");
        assert_eq!(table.column_definitions.len(), 2);
    }

    #[test]
    fn test_wants_table() {
        assert!(wants_table(Some("application/json;as=Table")));
        assert!(wants_table(Some("application/json;as=Table;v=v1")));
        assert!(!wants_table(Some("application/json")));
        assert!(!wants_table(None));
    }

    #[test]
    fn test_normalize_kind_strips_list() {
        assert_eq!(normalize_kind("PodList"), "Pod");
        assert_eq!(normalize_kind("Pod"), "Pod");
        assert_eq!(normalize_kind("NodeList"), "Node");
    }

    #[test]
    fn pod_columns_include_wide_ip_and_node() {
        let cols = printer_columns("Pod").expect("pods have a printer");
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "NAME",
                "READY",
                "STATUS",
                "RESTARTS",
                "AGE",
                "IP",
                "NODE",
                "NOMINATED NODE",
                "READINESS GATES"
            ]
        );
        // IP and NODE must be wide (priority 1) so plain `get` hides them
        // but `-o wide` reveals them.
        let by_name = |n: &str| cols.iter().find(|c| c.name == n).unwrap();
        assert_eq!(by_name("STATUS").priority, 0);
        assert_eq!(by_name("IP").priority, 1);
        assert_eq!(by_name("NODE").priority, 1);
        // PodList maps to the same columns.
        assert_eq!(printer_columns("PodList").unwrap().len(), cols.len());
    }

    #[test]
    fn pod_row_cells_extract_ip_and_node() {
        let pod = json!({
            "metadata": {"name": "smoke", "creationTimestamp": "2026-06-01T00:00:00Z"},
            "spec": {"nodeName": "node-1"},
            "status": {
                "phase": "Running",
                "podIP": "172.18.0.9",
                "containerStatuses": [
                    {"ready": true, "restartCount": 2},
                    {"ready": false, "restartCount": 1}
                ]
            }
        });
        let cells = printer_row_cells("Pod", &pod).expect("pod cells");
        assert_eq!(cells[0], json!("smoke")); // NAME
        assert_eq!(cells[1], json!("1/2")); // READY
        assert_eq!(cells[2], json!("Running")); // STATUS
        assert_eq!(cells[3], json!(3)); // RESTARTS (2+1)
        assert_eq!(cells[5], json!("172.18.0.9")); // IP (wide)
        assert_eq!(cells[6], json!("node-1")); // NODE (wide)
        assert_eq!(cells[7], json!("<none>")); // NOMINATED NODE
    }

    #[test]
    fn pod_row_cells_tolerate_missing_status() {
        let pod = json!({"metadata": {"name": "fresh"}, "spec": {}});
        let cells = printer_row_cells("Pod", &pod).expect("pod cells");
        assert_eq!(cells[0], json!("fresh"));
        assert_eq!(cells[1], json!("0/0")); // READY
        assert_eq!(cells[3], json!(0)); // RESTARTS
        assert_eq!(cells[5], json!("<none>")); // IP
        assert_eq!(cells[6], json!("<none>")); // NODE
    }

    #[test]
    fn node_row_cells_status_roles_and_wide() {
        let node = json!({
            "metadata": {
                "name": "node-1",
                "creationTimestamp": "2026-06-01T00:00:00Z",
                "labels": {
                    "node-role.kubernetes.io/control-plane": "",
                    "kubernetes.io/os": "linux"
                }
            },
            "spec": {"unschedulable": true},
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
                "addresses": [
                    {"type": "InternalIP", "address": "172.18.0.8"},
                    {"type": "Hostname", "address": "node-1"}
                ],
                "nodeInfo": {
                    "kubeletVersion": "v1.35.0-rusternetes",
                    "osImage": "Rusternetes OS",
                    "kernelVersion": "6.1.0",
                    "containerRuntimeVersion": "containerd://1.7.0"
                }
            }
        });
        let cells = printer_row_cells("Node", &node).expect("node cells");
        assert_eq!(cells[0], json!("node-1")); // NAME
        assert_eq!(cells[1], json!("Ready,SchedulingDisabled")); // STATUS
        assert_eq!(cells[2], json!("control-plane")); // ROLES
        assert_eq!(cells[4], json!("v1.35.0-rusternetes")); // VERSION
        assert_eq!(cells[5], json!("172.18.0.8")); // INTERNAL-IP (wide)
        assert_eq!(cells[6], json!("<none>")); // EXTERNAL-IP (wide)
        assert_eq!(cells[7], json!("Rusternetes OS")); // OS-IMAGE (wide)
    }

    #[test]
    fn node_not_ready_when_no_ready_condition() {
        let node = json!({
            "metadata": {"name": "n", "labels": {}},
            "status": {"conditions": [{"type": "MemoryPressure", "status": "False"}]}
        });
        let cells = printer_row_cells("Node", &node).unwrap();
        assert_eq!(cells[1], json!("NotReady"));
        assert_eq!(cells[2], json!("<none>")); // ROLES
    }

    #[test]
    fn namespace_row_cells_show_phase() {
        let ns = json!({
            "metadata": {"name": "default", "creationTimestamp": "2026-06-01T00:00:00Z"},
            "status": {"phase": "Active"}
        });
        let cells = printer_row_cells("Namespace", &ns).expect("ns cells");
        assert_eq!(cells[0], json!("default"));
        assert_eq!(cells[1], json!("Active"));
        let cols = printer_columns("Namespace").unwrap();
        assert_eq!(cells.len(), cols.len());
    }

    #[test]
    fn unknown_kind_has_no_printer() {
        assert!(printer_columns("ConfigMap").is_none());
        assert!(printer_row_cells("ConfigMap", &json!({})).is_none());
    }

    #[test]
    fn pods_table_matches_printer_columns() {
        use rusternetes_common::resources::Pod;
        let pod: Pod = serde_json::from_value(json!({
            "metadata": {"name": "smoke", "namespace": "default"},
            "spec": {"containers": [{"name": "c", "image": "nginx"}], "nodeName": "node-1"},
            "status": {"phase": "Running", "podIP": "10.0.0.5"}
        }))
        .expect("valid pod");
        let table = pods_table(vec![pod], Some("42".to_string()));
        let expected: Vec<String> = printer_columns("Pod")
            .unwrap()
            .iter()
            .map(|c| c.name.clone())
            .collect();
        let got: Vec<String> = table
            .column_definitions
            .iter()
            .map(|c| c.name.clone())
            .collect();
        assert_eq!(got, expected);
        assert_eq!(table.rows.len(), 1);
        // Cell count must match column count.
        assert_eq!(table.rows[0].cells.len(), table.column_definitions.len());
    }

    // Port of kube-aggregator pkg/registry/apiservice/etcd/etcd.go:82-120
    // (`REST.ConvertToTable`).
    fn api_service_cells(obj: serde_json::Value) -> Vec<serde_json::Value> {
        printer_row_cells("APIService", &obj).expect("APIService has a printer")
    }

    #[test]
    fn api_service_columns_match_upstream() {
        let cols = printer_columns("APIServiceList").expect("APIService has a printer");
        let got: Vec<(&str, &str, &str, i32)> = cols
            .iter()
            .map(|c| {
                (
                    c.name.as_str(),
                    c.column_type.as_str(),
                    c.format.as_str(),
                    c.priority,
                )
            })
            .collect();
        assert_eq!(
            got,
            vec![
                ("Name", "string", "name", 0),
                ("Service", "string", "", 0),
                ("Available", "string", "", 0),
                ("Age", "string", "", 0),
            ]
        );
        assert_eq!(
            cols[1].description,
            "The reference to the service that hosts this API endpoint."
        );
        assert_eq!(cols[2].description, "Whether this service is available.");
    }

    #[test]
    fn api_service_local_without_condition_is_unknown() {
        let cells = api_service_cells(json!({
            "metadata": {"name": "v1.", "creationTimestamp": "2020-01-01T00:00:00Z"},
            "spec": {}
        }));
        assert_eq!(cells[0], json!("v1."));
        assert_eq!(cells[1], json!("Local"));
        assert_eq!(cells[2], json!("Unknown"));
    }

    #[test]
    fn api_service_remote_renders_namespace_slash_name() {
        let cells = api_service_cells(json!({
            "metadata": {"name": "v1beta1.metrics.k8s.io"},
            "spec": {"service": {"namespace": "kube-system", "name": "metrics-server"}}
        }));
        assert_eq!(cells[1], json!("kube-system/metrics-server"));
    }

    #[test]
    fn api_service_available_condition_rendering() {
        let with = |status: &str, reason: Option<&str>| {
            let mut c = json!({"type": "Available", "status": status});
            if let Some(r) = reason {
                c["reason"] = json!(r);
            }
            api_service_cells(json!({
                "metadata": {"name": "x"},
                "spec": {},
                "status": {"conditions": [{"type": "Other", "status": "False", "reason": "No"}, c]}
            }))[2]
                .clone()
        };
        // etcd.go:105-110: True is bare even with a reason.
        assert_eq!(with("True", Some("Passed")), json!("True"));
        // etcd.go:107-108: non-True with a reason is "status (reason)".
        assert_eq!(
            with("False", Some("FailedDiscoveryCheck")),
            json!("False (FailedDiscoveryCheck)")
        );
        // etcd.go:109-110: no reason is the bare status.
        assert_eq!(with("False", None), json!("False"));
        assert_eq!(with("False", Some("")), json!("False"));
    }

    // printers.go:426-436 + 2356-2381.
    #[test]
    fn pod_certificate_request_columns_and_cells_match_upstream() {
        let cols = printer_columns("PodCertificateRequestList").expect("has a printer");
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "Name",
                "PodName",
                "ServiceAccountName",
                "NodeName",
                "SignerName",
                "State",
                "UnverifiedUserAnnotations"
            ]
        );
        assert_eq!(cols[0].format, "name");
        assert_eq!(
            cols[5].description,
            "Is the request Pending, Issued, Denied, or Failed?"
        );
        let cells =
            |obj: serde_json::Value| printer_row_cells("PodCertificateRequest", &obj).unwrap();
        let pending = cells(json!({
            "metadata": {"name": "r"},
            "spec": {"podName": "p", "serviceAccountName": "sa", "nodeName": "n", "signerName": "x.io/s"}
        }));
        assert_eq!(
            pending,
            vec![
                json!("r"),
                json!("p"),
                json!("sa"),
                json!("n"),
                json!("x.io/s"),
                json!("Pending"),
                json!("<none>")
            ]
        );
        assert_eq!(pending.len(), cols.len());
        for t in ["Issued", "Denied", "Failed"] {
            let c = cells(json!({
                "metadata": {"name": "r"},
                "spec": {"unverifiedUserAnnotations": {"b.io/k": "2", "a.io/k": "1"}},
                "status": {"conditions": [{"type": t, "status": "True"}]}
            }));
            assert_eq!(c[5], json!(t));
            assert_eq!(c[6], json!("a.io/k=1,b.io/k=2"));
        }
    }

    // printers.go:419-424 (`clusterTrustBundleColumnDefinitions`) + 2332-2342
    // (`printClusterTrustBundle`): signerName shows `<none>` when empty.
    #[test]
    fn cluster_trust_bundle_columns_and_cells_match_upstream() {
        let cols = printer_columns("ClusterTrustBundleList").expect("has a printer");
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["Name", "SignerName"]);
        assert_eq!(cols[0].format, "name");
        assert!(cols[1]
            .description
            .starts_with("signerName indicates the associated signer, if any."));
        let cells = |obj: serde_json::Value| printer_row_cells("ClusterTrustBundle", &obj).unwrap();
        assert_eq!(
            cells(json!({"metadata": {"name": "b"}, "spec": {}})),
            vec![json!("b"), json!("<none>")]
        );
        assert_eq!(
            cells(json!({"metadata": {"name": "x.io:s:a"}, "spec": {"signerName": "x.io/s"}})),
            vec![json!("x.io:s:a"), json!("x.io/s")]
        );
    }

    // printers.go:700-713 (`deviceTaintColumnDefinitions`) + 3255-3267
    // (`printDeviceTaint`): the rule list prints the taint, not the filter.
    #[test]
    fn device_taint_rule_columns_and_cells_match_upstream() {
        let cols = printer_columns("DeviceTaintRuleList").expect("has a printer");
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["Name", "Key", "Value", "Effect", "TimeAdded", "Age"]
        );
        assert_eq!(cols[0].format, "name");
        assert!(cols[1]
            .description
            .starts_with("The taint key to be applied to a device."));
        let cells = |obj: serde_json::Value| printer_row_cells("DeviceTaintRule", &obj).unwrap();
        // No timeAdded is an empty cell.
        let c = cells(json!({
            "metadata": {"name": "r", "creationTimestamp": "2020-01-01T00:00:00Z"},
            "spec": {"taint": {"key": "k", "value": "v", "effect": "NoExecute"}}
        }));
        assert_eq!(
            &c[..5],
            &[
                json!("r"),
                json!("k"),
                json!("v"),
                json!("NoExecute"),
                json!("")
            ]
        );
        // A set timeAdded is rendered as a duration since.
        let c = cells(json!({
            "metadata": {"name": "r"},
            "spec": {"taint": {"key": "k", "effect": "None", "timeAdded": "2020-01-01T00:00:00Z"}}
        }));
        assert_ne!(c[4], json!(""));
        assert_ne!(c[4], json!("<unknown>"));
        assert_eq!(c[5], json!("<unknown>"));
    }

    // duration_test.go TestHumanDuration + TestHumanDurationBoundaries.
    #[test]
    fn human_duration_matches_upstream_cases() {
        use chrono::Duration as D;
        let h = |n: i64| D::hours(n);
        let ms = D::milliseconds;
        let cases: Vec<(D, &str)> = vec![
            (D::seconds(1), "1s"),
            (D::seconds(70), "70s"),
            (D::seconds(190), "3m10s"),
            (D::minutes(70), "70m"),
            (h(47), "47h"),
            (h(49), "2d1h"),
            (h(8 * 24 + 2), "8d"),
            (h(367 * 24), "367d"),
            (h(365 * 2 * 24 + 25), "2y1d"),
            (h(365 * 8 * 24 + 2), "8y"),
            (D::seconds(-2), "<invalid>"),
            (ms(-2000 + 1), "0s"),
            (D::zero(), "0s"),
            (ms(999), "0s"),
            (D::minutes(2) - ms(1), "119s"),
            (D::minutes(2), "2m"),
            (D::minutes(2) + D::seconds(1), "2m1s"),
            (D::minutes(10) - ms(1), "9m59s"),
            (D::minutes(10) + D::seconds(1), "10m"),
            (h(3) - ms(1), "179m"),
            (h(3), "3h"),
            (h(3) + D::minutes(1), "3h1m"),
            (h(8) - ms(1), "7h59m"),
            (h(8) + D::minutes(59), "8h"),
            (h(48) - ms(1), "47h"),
            (h(48), "2d"),
            (h(49), "2d1h"),
            (h(8 * 24) - ms(1), "7d23h"),
            (h(8 * 24 + 23), "8d"),
            (h(2 * 365 * 24) - ms(1), "729d"),
            (h(2 * 365 * 24), "2y"),
            (h(2 * 365 * 24 + 24), "2y1d"),
            (h(7 * 365 * 24), "7y"),
            (h(8 * 365 * 24) - ms(1), "7y364d"),
            (h(8 * 365 * 24 + 364 * 24), "8y"),
        ];
        for (d, want) in cases {
            assert_eq!(human_duration(d), want, "{d:?}");
        }
    }

    #[test]
    fn table_age_cell_is_two_unit() {
        let ts =
            chrono::Utc::now() - chrono::Duration::hours(24 * 5 + 3) - chrono::Duration::minutes(1);
        assert_eq!(humanize_age(Some(ts)), "5d3h");
    }
}
