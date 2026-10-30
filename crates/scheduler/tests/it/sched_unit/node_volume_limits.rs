//! Unit tests moved from `src/node_volume_limits.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::csi_translation::CsiTranslator;
    pub use rusternetes_common::resources::{
        CSIDriver, CSINode, Node, PersistentVolume, PersistentVolumeClaim, Pod, StorageClass,
        Volume, VolumeAttachment,
    };
    pub use rusternetes_scheduler::volume_binding::{
        claim_class, ephemeral_volume_is_for_pod, pod_volumes, volume_pvc_name,
    };
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::node_volume_limits::*;
    use serde_json::{json, Value};

    const EBS: &str = "ebs.csi.aws.com";
    const GCE: &str = "pd.csi.storage.gke.io";
    const AWS_IN_TREE: &str = "kubernetes.io/aws-ebs";
    const HOSTPATH: &str = "kubernetes.io/hostpath";
    const SC: &str = "csi-sc";
    const NODE: &str = "node-for-max-pd-test-1";

    fn pod_json(meta: Value, volumes: Value) -> Pod {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod", "metadata": meta, "spec": {"volumes": volumes}
        }))
        .unwrap()
    }
    fn pvc_pod(claims: &[&str]) -> Pod {
        let vols: Vec<Value> = claims
            .iter()
            .enumerate()
            .map(
                |(i, c)| json!({"name": format!("v{i}"), "persistentVolumeClaim": {"claimName": c}}),
            )
            .collect();
        pod_json(json!({"name": "p"}), Value::Array(vols))
    }
    fn node() -> Node {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Node", "metadata": {"name": NODE}, "spec": {}
        }))
        .unwrap()
    }
    /// `getFakeCSIPVLister`.
    fn pvs(volume_name: &str, drivers: &[&str]) -> Vec<PersistentVolume> {
        let mut out = vec![];
        for d in drivers {
            for j in 0..4 {
                let h = format!("{volume_name}-{d}-{j}");
                let spec = match *d {
                    AWS_IN_TREE => json!({"awsElasticBlockStore": {"volumeID": h}}),
                    HOSTPATH => json!({"hostPath": {"path": "/tmp"}}),
                    _ => json!({"csi": {"driver": d, "volumeHandle": h}}),
                };
                out.push(
                    serde_json::from_value(json!({
                        "apiVersion": "v1", "kind": "PersistentVolume",
                        "metadata": {"name": h}, "spec": spec
                    }))
                    .unwrap(),
                );
            }
        }
        out
    }
    /// `getFakeCSIPVCLister`.
    fn pvcs(volume_name: &str, drivers: &[&str]) -> Vec<PersistentVolumeClaim> {
        let mk = |name: String, spec: Value| -> PersistentVolumeClaim {
            serde_json::from_value(json!({
                "apiVersion": "v1", "kind": "PersistentVolumeClaim",
                "metadata": {"name": name}, "spec": spec
            }))
            .unwrap()
        };
        let mut out = vec![];
        for d in drivers {
            for j in 0..4 {
                let v = format!("{volume_name}-{d}-{j}");
                out.push(mk(v.clone(), json!({"volumeName": v})));
            }
        }
        out.push(mk(
            format!("{volume_name}-4"),
            json!({"storageClassName": SC}),
        ));
        out.push(mk(format!("{volume_name}-5"), json!({})));
        out.push(mk(
            format!("{volume_name}-6"),
            json!({"storageClassName": SC, "volumeName": "missing-in-action"}),
        ));
        out
    }
    fn class(provisioner: &str) -> StorageClass {
        serde_json::from_value(json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
            "metadata": {"name": SC}, "provisioner": provisioner
        }))
        .unwrap()
    }
    /// `getFakeVolumeAttachmentLister`.
    fn vas(count: usize, drivers: &[&str]) -> Vec<VolumeAttachment> {
        let mut out = vec![];
        for d in drivers {
            for j in 0..count {
                out.push(
                    serde_json::from_value(json!({
                        "apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachment",
                        "metadata": {"name": format!("va-{d}-{j}")},
                        "spec": {"nodeName": NODE, "attacher": d,
                                 "source": {"persistentVolumeName": format!("csi-{d}-{j}")}}
                    }))
                    .unwrap(),
                );
            }
        }
        out
    }
    /// `getNodeWithPodAndVolumeLimits` + `enableMigrationOnNode`.
    fn csi_node(
        limit_source: &str,
        limit: i32,
        drivers: &[&str],
        migrate: bool,
    ) -> Option<CSINode> {
        let add = |with_limits: bool| -> Vec<Value> {
            drivers
                .iter()
                .map(|d| {
                    let mut v = json!({"name": d, "nodeID": NODE});
                    if with_limits {
                        v["allocatable"] = json!({"count": limit});
                    }
                    v
                })
                .collect()
        };
        let drivers_json = match limit_source {
            "csinode" => add(true),
            "csinode-with-no-limit" => add(false),
            "no-csi-driver" => vec![],
            _ => return None,
        };
        let mut meta = json!({"name": NODE});
        if migrate {
            meta["annotations"] = json!({MIGRATED_PLUGINS_ANNOTATION_KEY: AWS_IN_TREE});
        }
        Some(
            serde_json::from_value(json!({
                "apiVersion": "storage.k8s.io/v1", "kind": "CSINode",
                "metadata": meta, "spec": {"drivers": drivers_json}
            }))
            .unwrap(),
        )
    }

    struct Case {
        new_pod: Pod,
        existing: Vec<Pod>,
        extra_claims: Vec<PersistentVolumeClaim>,
        max_vols: i32,
        va_count: usize,
        drivers: Vec<&'static str>,
        migration: bool,
        limit_source: &'static str,
        want: FilterStatus,
        want_skip: bool,
    }
    impl Case {
        fn new(new_pod: Pod, drivers: &[&'static str], limit_source: &'static str) -> Self {
            Self {
                new_pod,
                existing: vec![],
                extra_claims: vec![],
                max_vols: 0,
                va_count: 0,
                drivers: drivers.to_vec(),
                migration: false,
                limit_source,
                want: FilterStatus::Success,
                want_skip: false,
            }
        }
        fn run(self, name: &str) {
            let mut claims = pvcs("csi", &self.drivers);
            claims.extend(self.extra_claims);
            let snap = NodeVolumeLimitsSnapshot {
                pvcs: claims,
                pvs: pvs("csi", &self.drivers),
                classes: vec![class(self.drivers[0])],
                csi_drivers: vec![],
                csi_nodes: csi_node(
                    self.limit_source,
                    self.max_vols,
                    &self.drivers,
                    self.migration,
                )
                .into_iter()
                .collect(),
                volume_attachments: vas(self.va_count, &self.drivers),
            };
            let mut p = CsiLimits::new(&snap);
            p.enable_volume_limit_scaling = false;
            let skip = !CsiLimits::pre_filter(&self.new_pod);
            assert_eq!(skip, self.want_skip, "{name}: PreFilter skip");
            if skip {
                return;
            }
            let existing: Vec<&Pod> = self.existing.iter().collect();
            assert_eq!(
                p.filter(&self.new_pod, &node(), &existing),
                self.want,
                "{name}"
            );
        }
    }

    fn unsched() -> FilterStatus {
        FilterStatus::Unschedulable(ERR_REASON_MAX_VOLUME_COUNT_EXCEEDED.to_string())
    }
    fn in_tree_inline(volume_id: &str) -> Pod {
        pod_json(
            json!({"name": "p"}),
            json!([{"name": "i", "awsElasticBlockStore": {"volumeID": volume_id}}]),
        )
    }
    fn cm_secret_plus(extra: Option<Value>) -> Pod {
        let mut vols = vec![
            json!({"name": "c", "configMap": {"name": "x"}}),
            json!({"name": "s", "secret": {"secretName": "x"}}),
        ];
        vols.extend(extra);
        pod_json(
            json!({"name": "abc", "namespace": "test"}),
            Value::Array(vols),
        )
    }

    #[test]
    fn va_counted_with_no_pods() {
        let mut c = Case::new(pvc_pod(&["csi-ebs.csi.aws.com-0"]), &[EBS], "csinode");
        c.max_vols = 2;
        c.va_count = 2;
        c.want = unsched();
        c.run("should count VolumeAttachments towards volume limit when no pods exist");
        let mut c = Case::new(pvc_pod(&["csi-ebs.csi.aws.com-0"]), &[EBS], "csinode");
        c.max_vols = 2;
        c.va_count = 1;
        c.run("should schedule pod when VolumeAttachments count does not exceed limit");
    }

    #[test]
    fn csi_limit_rows() {
        let running = || pvc_pod(&["csi-ebs.csi.aws.com-3"]);
        let ebs_two = || pvc_pod(&["csi-ebs.csi.aws.com-1", "csi-ebs.csi.aws.com-2"]);
        let ebs_one = || pvc_pod(&["csi-ebs.csi.aws.com-0"]);
        let pending = || pvc_pod(&["csi-4"]);
        let gce_two = || pvc_pod(&["csi-pd.csi.storage.gke.io-1", "csi-pd.csi.storage.gke.io-2"]);

        let mut c = Case::new(ebs_one(), &[EBS], "csinode");
        c.existing = vec![running(), ebs_two()];
        c.max_vols = 4;
        c.run("fits when node volume limit >= new pods CSI volume");

        let mut c = Case::new(ebs_one(), &[EBS], "csinode");
        c.existing = vec![running(), ebs_two()];
        c.max_vols = 2;
        c.want = unsched();
        c.run("doesn't when node volume limit <= pods CSI volume");

        let mut c = Case::new(ebs_one(), &[EBS], "csinode-with-no-limit");
        c.existing = vec![running(), ebs_two()];
        c.max_vols = 2;
        c.run("should when driver does not support volume limits");

        let mut c = Case::new(ebs_one(), &[EBS], "csinode");
        c.existing = vec![pending(), ebs_two()];
        c.max_vols = 2;
        c.want = unsched();
        c.run("count pending PVCs towards volume limit <= pods CSI volume");

        let mut c = Case::new(ebs_one(), &[EBS], "csinode");
        c.existing = vec![pending(), pvc_pod(&["csi-4"]), ebs_two()];
        c.max_vols = 4;
        c.run("count multiple pending pvcs towards volume limit >= pods CSI volume");

        let mut c = Case::new(ebs_one(), &[EBS], "csinode");
        c.existing = vec![pvc_pod(&["csi-6"]), ebs_two()];
        c.max_vols = 2;
        c.want = unsched();
        c.run("should count PVCs with invalid PV name but valid SC");

        let mut c = Case::new(ebs_one(), &[EBS], "csinode");
        c.existing = vec![running(), pvc_pod(&["csi-5"])];
        c.max_vols = 2;
        c.run("don't count pvcs with missing SC towards volume limit");

        let mut c = Case::new(ebs_one(), &[EBS, GCE], "csinode");
        c.existing = vec![gce_two(), ebs_two()];
        c.max_vols = 2;
        c.want = unsched();
        c.run("count pvcs with the same type towards volume limit");

        let mut c = Case::new(gce_two(), &[EBS, GCE], "csinode");
        c.existing = vec![ebs_two(), running()];
        c.max_vols = 2;
        c.run("don't count pvcs with different type towards volume limit");
    }

    #[test]
    fn migration_rows() {
        let in_tree_one = || pvc_pod(&["csi-kubernetes.io/aws-ebs-0"]);
        let in_tree_two =
            || pvc_pod(&["csi-kubernetes.io/aws-ebs-1", "csi-kubernetes.io/aws-ebs-2"]);
        let ebs_two = || pvc_pod(&["csi-ebs.csi.aws.com-1", "csi-ebs.csi.aws.com-2"]);
        let drivers = [AWS_IN_TREE, EBS];

        let mut c = Case::new(in_tree_one(), &drivers, "csinode");
        c.existing = vec![in_tree_two()];
        c.max_vols = 2;
        c.migration = true;
        c.want = unsched();
        c.run("should count in-tree volumes if migration is enabled");

        let mut c = Case::new(in_tree_inline("aws-inline1"), &drivers, "nil");
        c.existing = vec![in_tree_two()];
        c.migration = true;
        c.run("nil csi node");

        let mut c = Case::new(pvc_pod(&["csi-4"]), &drivers, "csinode");
        c.existing = vec![in_tree_two()];
        c.max_vols = 2;
        c.migration = true;
        c.want = unsched();
        c.run("should count unbound in-tree volumes if migration is enabled");

        let mut c = Case::new(in_tree_one(), &drivers, "csinode-with-no-limit");
        c.existing = vec![in_tree_two()];
        c.max_vols = 2;
        c.migration = true;
        c.run("should not limit pod if volume used does not report limits");

        let mut c = Case::new(
            pvc_pod(&["csi-kubernetes.io/hostpath-0"]),
            &[HOSTPATH, EBS],
            "csinode",
        );
        c.existing = vec![ebs_two()];
        c.max_vols = 2;
        c.migration = true;
        c.run("should not count non-migratable in-tree volumes");

        let mut c = Case::new(in_tree_inline("aws-inline1"), &drivers, "csinode");
        c.existing = vec![in_tree_two()];
        c.max_vols = 2;
        c.migration = true;
        c.want = unsched();
        c.run("should count in-tree inline volumes if migration is enabled");

        let mut c = Case::new(in_tree_one(), &drivers, "csinode");
        c.existing = vec![ebs_two()];
        c.max_vols = 2;
        c.migration = true;
        c.want = unsched();
        c.run("should count in-tree and csi volumes (scheduling in-tree volumes)");

        let mut c = Case::new(in_tree_inline("aws-inline1"), &drivers, "csinode");
        c.existing = vec![ebs_two(), in_tree_one()];
        c.max_vols = 3;
        c.migration = true;
        c.want = unsched();
        c.run("should count in-tree, inline and csi volumes");

        let mut c = Case::new(in_tree_inline("csi-ebs.csi.aws.com-1"), &drivers, "csinode");
        c.existing = vec![ebs_two(), in_tree_one()];
        c.max_vols = 3;
        c.migration = true;
        c.run("should not count in-tree, inline and csi volumes with the same volume id");

        let mut c = Case::new(pvc_pod(&["csi-ebs.csi.aws.com-0"]), &drivers, "csinode");
        c.existing = vec![in_tree_two()];
        c.max_vols = 2;
        c.migration = true;
        c.want = unsched();
        c.run("should count in-tree and csi volumes (scheduling csi volumes)");

        let mut c = Case::new(
            cm_secret_plus(Some(
                json!({"name": "i", "awsElasticBlockStore": {"volumeID": "aws-inline1"}}),
            )),
            &drivers,
            "csinode",
        );
        c.existing = vec![in_tree_two()];
        c.max_vols = 2;
        c.migration = true;
        c.want = unsched();
        c.run("don't skip Filter when the pod has inline migratable volumes");
    }

    fn ephemeral_pod(name: &str, uid: &str, vols: &[&str]) -> Pod {
        let v: Vec<Value> = vols
            .iter()
            .map(|n| json!({"name": n, "ephemeral": {}}))
            .collect();
        pod_json(
            json!({"name": name, "namespace": "test", "uid": uid}),
            Value::Array(v),
        )
    }
    fn eph_claim(pod: &str, vol: &str, uid: Option<&str>) -> PersistentVolumeClaim {
        let mut meta = json!({"name": format!("{pod}-{vol}"), "namespace": "test"});
        if let Some(u) = uid {
            meta["ownerReferences"] = json!([{"apiVersion": "v1", "kind": "Pod", "name": pod,
                "uid": u, "controller": true}]);
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": meta, "spec": {"storageClassName": SC}
        }))
        .unwrap()
    }

    #[test]
    fn ephemeral_rows() {
        let eph = || ephemeral_pod("abc", "12345", &["xyz"]);
        let running = || pvc_pod(&["csi-ebs.csi.aws.com-3"]);
        let ebs_two = || pvc_pod(&["csi-ebs.csi.aws.com-1", "csi-ebs.csi.aws.com-2"]);
        let missing = || {
            FilterStatus::UnschedulableAndUnresolvable(
                "looking up PVC test/abc-xyz: persistentvolumeclaims \"abc-xyz\" not found".into(),
            )
        };

        let mut c = Case::new(eph(), &[EBS], "csinode-with-no-limit");
        c.want = missing();
        c.run("ephemeral volume missing");

        let mut c = Case::new(eph(), &[EBS], "csinode-with-no-limit");
        c.extra_claims = vec![eph_claim("abc", "xyz", None)];
        c.want = FilterStatus::Error(
            "PVC test/abc-xyz was not created for pod test/abc (pod is not owner)".into(),
        );
        c.run("ephemeral volume not owned");

        let mut c = Case::new(eph(), &[EBS], "csinode-with-no-limit");
        c.extra_claims = vec![eph_claim("abc", "xyz", Some("12345"))];
        c.run("ephemeral volume unbound");

        let mut c = Case::new(eph(), &[EBS], "csinode");
        c.extra_claims = vec![eph_claim("abc", "xyz", Some("12345"))];
        c.existing = vec![running(), ebs_two()];
        c.max_vols = 2;
        c.want = unsched();
        c.run("ephemeral doesn't when node volume limit <= pods CSI volume");

        let two = || ephemeral_pod("abc", "12345II", &["x", "y"]);
        let mut c = Case::new(pvc_pod(&["csi-ebs.csi.aws.com-0"]), &[EBS], "csinode");
        c.extra_claims = vec![
            eph_claim("abc", "x", Some("12345II")),
            eph_claim("abc", "y", Some("12345II")),
        ];
        c.existing = vec![running(), two()];
        c.max_vols = 2;
        c.want = unsched();
        c.run("ephemeral doesn't when node volume limit <= pods ephemeral CSI volume");

        let mut c = Case::new(pvc_pod(&["csi-ebs.csi.aws.com-0"]), &[EBS], "csinode");
        c.extra_claims = vec![eph_claim("abc", "xyz", Some("12345"))];
        c.existing = vec![running(), eph(), ebs_two()];
        c.max_vols = 3;
        c.want = unsched();
        c.run("persistent doesn't when node volume limit <= ephemeral + persistent");

        let mut c = Case::new(pvc_pod(&["csi-ebs.csi.aws.com-0"]), &[EBS], "csinode");
        c.extra_claims = vec![eph_claim("abc", "xyz", Some("12345"))];
        c.existing = vec![running(), eph(), ebs_two()];
        c.max_vols = 5;
        c.run("persistent okay when node volume limit > ephemeral + persistent");

        let mut c = Case::new(
            cm_secret_plus(Some(json!({"name": "xyz", "ephemeral": {}}))),
            &[EBS],
            "csinode-with-no-limit",
        );
        c.want = missing();
        c.run("don't skip Filter when the pod has ephemeral volumes");
    }

    #[test]
    fn prefilter_rows() {
        let mut c = Case::new(cm_secret_plus(None), &[EBS], "csinode");
        c.max_vols = 2;
        c.want_skip = true;
        c.run("skip Filter when the pod only uses secrets and configmaps");

        // pvcPodWithConfigmapAndSecret has no namespace upstream.
        let pod = pod_json(
            json!({"name": "p"}),
            json!([
                {"name": "c", "configMap": {"name": "x"}},
                {"name": "s", "secret": {"secretName": "x"}},
                {"name": "p", "persistentVolumeClaim": {"claimName": "csi-ebs.csi.aws.com-0"}}
            ]),
        );
        let mut c = Case::new(pod, &[EBS], "csinode");
        c.max_vols = 2;
        c.run("don't skip Filter when the pod has pvcs");
    }

    #[test]
    fn volume_limit_scaling_gate() {
        let new_pod = pvc_pod(&["csi-ebs.csi.aws.com-0"]);
        let not_installed =
            FilterStatus::Unschedulable(format!("{EBS} CSI driver is not installed on the node"));
        for (name, enabled, source, limit, driver_present, want) in [
            (
                "gate enabled - fail when driver not installed and CSIDriver exists",
                true,
                "no-csi-driver",
                0,
                true,
                not_installed,
            ),
            (
                "gate disabled - skip driver presence check",
                false,
                "no-csi-driver",
                0,
                true,
                FilterStatus::Success,
            ),
            (
                "gate enabled - driver installed within limit",
                true,
                "csinode",
                2,
                true,
                FilterStatus::Success,
            ),
            (
                "gate enabled - allow scheduling when CSIDriver object missing",
                true,
                "no-csi-driver",
                0,
                false,
                FilterStatus::Success,
            ),
        ] {
            let snap = NodeVolumeLimitsSnapshot {
                pvcs: pvcs("csi", &[EBS]),
                pvs: pvs("csi", &[EBS]),
                classes: vec![class(EBS)],
                csi_drivers: if driver_present {
                    vec![serde_json::from_value(json!({
                        "apiVersion": "storage.k8s.io/v1", "kind": "CSIDriver",
                        "metadata": {"name": EBS}, "spec": {}
                    }))
                    .unwrap()]
                } else {
                    vec![]
                },
                csi_nodes: csi_node(source, limit, &[EBS], false).into_iter().collect(),
                volume_attachments: vas(0, &[EBS]),
            };
            let mut p = CsiLimits::new(&snap);
            p.enable_volume_limit_scaling = enabled;
            assert!(
                CsiLimits::pre_filter(&new_pod),
                "{name}: unexpected PreFilter Skip"
            );
            assert_eq!(p.filter(&new_pod, &node(), &[]), want, "{name}");
        }
    }
}
