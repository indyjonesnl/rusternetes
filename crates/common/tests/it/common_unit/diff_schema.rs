//! Unit tests moved from `src/diff_schema.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::diff::GoJson;
    pub use rusternetes_common::quantity::Quantity;
    pub use serde_json::Value;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::diff_schema::*;
    use serde_json::json;

    /// Nested internal types render with Go field names, `null` for nil
    /// pointers/slices, canonical quantities and sorted resource keys.
    #[test]
    fn renders_internal_ephemeral_container_shape() {
        let c = json!({
            "name": "d", "image": "busybox", "tty": true,
            "env": [{"name": "A", "value": "1"}],
            "resources": {"requests": {"memory": "1024Mi", "cpu": "1"}},
            "securityContext": {"runAsUser": 0}
        });
        let out = render_struct(EPHEMERAL_CONTAINER, Some(&c)).marshal_indent();
        for want in [
            " \"Env\": [\n  {\n   \"Name\": \"A\",\n   \"Value\": \"1\",\n   \"ValueFrom\": null\n  }\n ],",
            " \"Resources\": {\n  \"Limits\": null,\n  \"Requests\": {\n   \"cpu\": \"1\",\n   \"memory\": \"1Gi\"\n  },\n  \"Claims\": null\n },",
            "  \"RunAsUser\": 0,",
            " \"TTY\": true,",
        ] {
            assert!(out.contains(want), "missing {want:?} in\n{out}");
        }
    }

    /// Probe/Lifecycle are modelled (no longer raw): inlined `ProbeHandler`,
    /// `IntOrString` port, nested handlers.
    #[test]
    fn renders_probe_and_lifecycle() {
        let c = json!({
            "name": "d",
            "livenessProbe": {"httpGet": {"path": "/h", "port": "http"}, "periodSeconds": 5},
            "lifecycle": {"preStop": {"exec": {"command": ["sh"]}}}
        });
        let out = render_struct(EPHEMERAL_CONTAINER, Some(&c)).marshal_indent();
        for want in [
            " \"LivenessProbe\": {\n  \"Exec\": null,\n  \"HTTPGet\": {\n   \"Path\": \"/h\",\n   \"Port\": \"http\",\n   \"Host\": \"\",\n   \"Scheme\": \"\",\n   \"HTTPHeaders\": null\n  },\n  \"TCPSocket\": null,\n  \"GRPC\": null,\n  \"InitialDelaySeconds\": 0,\n  \"TimeoutSeconds\": 0,\n  \"PeriodSeconds\": 5,",
            " \"ReadinessProbe\": null,",
            " \"Lifecycle\": {\n  \"PostStart\": null,\n  \"PreStop\": {\n   \"Exec\": {\n    \"Command\": [\n     \"sh\"\n    ]\n   },",
            "\"StopSignal\": null",
        ] {
            assert!(out.contains(want), "missing {want:?} in\n{out}");
        }
    }

    /// v1->internal: host namespace fields move into `SecurityContext`, which
    /// is always an object; `serviceAccount` fills an empty `ServiceAccountName`
    /// (conversion.go:349-372).
    #[test]
    fn pod_spec_applies_v1_conversion() {
        let spec = json!({"containers": [], "hostNetwork": true, "hostUsers": false,
                          "serviceAccount": "legacy"});
        let out = render_struct(POD_SPEC, Some(&internal_pod_spec(&spec))).marshal_indent();
        for want in [
            " \"ServiceAccountName\": \"legacy\",",
            " \"SecurityContext\": {\n  \"HostNetwork\": true,\n  \"HostPID\": false,\n  \"HostIPC\": false,\n  \"ShareProcessNamespace\": null,\n  \"HostUsers\": false,",
        ] {
            assert!(out.contains(want), "missing {want:?} in\n{out}");
        }
    }

    /// `metav1` types are json-tagged: tag names as keys, `omitempty` drops
    /// empty values (and a zero `creationTimestamp`), struct values stay.
    #[test]
    fn renders_tagged_metav1_with_omitempty() {
        let vol = json!({"volumeClaimTemplate": {
            "name": "x", "labels": {"b": "2", "a": "1"},
            "spec": {"selector": {"matchLabels": {"k": "v"}},
                     "resources": {"requests": {"storage": "1024Mi"}}}}});
        let out = render(Kind::PtrStruct(EPHEMERAL_VOLUME_SOURCE), Some(&vol)).marshal_indent();
        let want = concat!(
            "{\n \"VolumeClaimTemplate\": {\n  \"name\": \"x\",\n  \"labels\": {\n   \"a\": \"1\",\n   \"b\": \"2\"\n  },\n",
            "  \"Spec\": {\n   \"AccessModes\": null,\n   \"Selector\": {\n    \"matchLabels\": {\n     \"k\": \"v\"\n    }\n   },\n",
            "   \"Resources\": {\n    \"Limits\": null,\n    \"Requests\": {\n     \"storage\": \"1Gi\"\n    }\n   },",
        );
        assert!(out.starts_with(want), "{out}");
    }

    /// `FieldsV1` renders in `emitContentsV1` order, not serde's sorted-key
    /// order: `.`, field names, keys, values, then numeric indexes.
    #[test]
    fn fields_v1_keys_follow_fieldpath_order() {
        let v = json!({"fieldsV1": {
            "i:10": {}, "i:2": {}, "v:3": {}, "k:{\"name\":\"b\"}": {},
            "k:{\"name\":\"a\"}": {}, "f:z": {}, "f:a": {}, ".": {}}});
        let out = render(Kind::Struct(MANAGED_FIELDS_ENTRY), Some(&v)).marshal_indent();
        let keys: Vec<&str> = out
            .lines()
            .filter(|l| l.starts_with("  \""))
            .map(|l| l.trim().split("\": ").next().unwrap())
            .collect();
        assert_eq!(
            keys,
            [
                "\".",
                "\"f:a",
                "\"f:z",
                "\"k:{\\\"name\\\":\\\"a\\\"}",
                "\"k:{\\\"name\\\":\\\"b\\\"}",
                "\"v:3",
                "\"i:2",
                "\"i:10"
            ],
            "{out}"
        );
    }

    /// An empty-but-present `*FieldsV1` (`{}`) is not omitted.
    #[test]
    fn empty_fields_v1_not_omitted() {
        let out = render(
            Kind::Struct(MANAGED_FIELDS_ENTRY),
            Some(&json!({"fieldsV1": {}})),
        )
        .marshal_indent();
        assert!(out.contains("\"fieldsV1\": {}"), "{out}");
    }

    /// Every one of the 22 `PersistentVolumeSource` members renders, in
    /// declaration order, `null` when unset.
    #[test]
    fn renders_persistent_volume_source_members() {
        let out = render_struct(PERSISTENT_VOLUME_SOURCE, Some(&json!({}))).marshal_indent();
        let keys: Vec<&str> = out
            .lines()
            .map(|l| l.trim().split('"').nth(1).unwrap_or(""))
            .collect();
        assert_eq!(
            keys[1..23].to_vec(),
            [
                "GCEPersistentDisk",
                "AWSElasticBlockStore",
                "HostPath",
                "Glusterfs",
                "NFS",
                "RBD",
                "Quobyte",
                "ISCSI",
                "FlexVolume",
                "Cinder",
                "CephFS",
                "FC",
                "Flocker",
                "AzureFile",
                "VsphereVolume",
                "AzureDisk",
                "PhotonPersistentDisk",
                "PortworxVolume",
                "ScaleIO",
                "Local",
                "StorageOS",
                "CSI"
            ]
        );
        let csi = render_struct(
            PERSISTENT_VOLUME_SOURCE,
            Some(&json!({"csi": {"driver": "d", "volumeHandle": "h",
                "controllerExpandSecretRef": {"name": "s", "namespace": "n"}}})),
        )
        .marshal_indent();
        assert!(
            csi.contains("  \"ControllerExpandSecretRef\": {\n   \"Name\": \"s\",\n   \"Namespace\": \"n\"\n  },\n  \"NodeExpandSecretRef\": null"),
            "{csi}"
        );
    }
}
