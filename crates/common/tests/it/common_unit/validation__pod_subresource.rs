//! Unit tests moved from `src/validation/pod_subresource.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::equality::semantic_equal;
    pub use rusternetes_common::feature_gates::{enabled, Feature};
    pub use rusternetes_common::qos::{compute_pod_qos, get_pod_qos};
    pub use rusternetes_common::resources::pod::{EphemeralContainer, PodSpec};
    pub use rusternetes_common::resources::{Container, Pod};
    pub use rusternetes_common::types::ResourceRequirements;
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
    };
    pub use rusternetes_common::validation::pod::{
        validate_container, validate_pod_create_with_options,
    };
    pub use rusternetes_common::validation::pod_options::{
        get_validation_options_from_pod_spec_and_meta, PodValidationOptions,
    };
    pub use rusternetes_common::validation::pod_status::validate_pod_metadata_annotations_with_opts;
    pub use std::collections::{HashMap, HashSet};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::PodStatus;
    use rusternetes_common::validation::pod_subresource::*;
    use serde_json::json;

    fn pod(value: serde_json::Value) -> Pod {
        serde_json::from_value(value).unwrap()
    }

    fn running_status(resources: Option<serde_json::Value>) -> PodStatus {
        let mut status = serde_json::json!({"containerStatuses": [{
            "name": "c", "ready": true, "restartCount": 0, "image": "i", "imageID": "i",
            "state": {"running": {}}
        }]});
        if let Some(r) = resources {
            status["containerStatuses"][0]["resources"] = r;
        }
        serde_json::from_value(status).unwrap()
    }

    /// `isPodResizeRequestSupported` (validation.go:6479): a running container
    /// whose status carries no `resources` marks a node without resize support.
    #[test]
    fn resize_support_is_read_from_the_first_running_container_status() {
        let mut p = pod(serde_json::json!({"metadata": {"name": "p"}}));
        assert!(is_pod_resize_request_supported(&p));
        p.status = Some(running_status(None));
        assert!(!is_pod_resize_request_supported(&p));
        p.status = Some(running_status(Some(
            serde_json::json!({"requests": {"cpu": "1"}}),
        )));
        assert!(is_pod_resize_request_supported(&p));
    }

    /// `resourcesRemoved` (validation.go:6513): fewer entries, or a missing
    /// name, is a removal; adding an entry is not.
    #[test]
    fn resources_removed_matches_upstream() {
        let list = |kv: &[(&str, &str)]| {
            Some(
                kv.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect::<HashMap<_, _>>(),
            )
        };
        assert!(resources_removed(&None, &list(&[("cpu", "1")])));
        assert!(resources_removed(
            &list(&[("memory", "1")]),
            &list(&[("cpu", "1")])
        ));
        assert!(!resources_removed(
            &list(&[("cpu", "2"), ("memory", "1")]),
            &list(&[("cpu", "1")])
        ));
        assert!(!resources_removed(&list(&[("cpu", "1")]), &None));
    }

    /// `dropCPUMemoryUpdates` (validation.go:6414): cpu and memory come from
    /// the old list, everything else from the new one.
    #[test]
    fn drop_cpu_memory_updates_keeps_old_cpu_and_memory() {
        let new = Some(HashMap::from([
            ("cpu".to_string(), "2".to_string()),
            ("memory".to_string(), "2Gi".to_string()),
            ("ephemeral-storage".to_string(), "2Gi".to_string()),
        ]));
        let old = Some(HashMap::from([("cpu".to_string(), "1".to_string())]));
        let munged = drop_cpu_memory_updates(&new, &old).unwrap();
        assert_eq!(munged.get("cpu").map(String::as_str), Some("1"));
        assert!(!munged.contains_key("memory"));
        assert_eq!(
            munged.get("ephemeral-storage").map(String::as_str),
            Some("2Gi")
        );
        assert_eq!(drop_cpu_memory_updates(&None, &None), None);
    }

    /// `ValidatePodResize` with pod-level resources and the
    /// `InPlacePodLevelResourcesVerticalScaling` gate off (validation.go:6247).
    #[test]
    #[serial_test::serial]
    fn pod_level_resources_cannot_be_resized_with_the_gate_off() {
        let old = pod(serde_json::json!({
            "metadata": {"name": "p"},
            "spec": {
                "containers": [{"name": "c", "image": "busybox"}],
                "resources": {"requests": {"cpu": "1"}}
            }
        }));
        let errs = validate_pod_resize(&old.clone(), &old);
        assert!(
            errs.iter()
                .any(|e| e.detail.contains("pod-level resources is forbidden")),
            "{errs:?}"
        );
    }

    /// A pod as `TestValidatePodResize`'s `mkPod`: one container with the
    /// given requests/limits, plus optional extra spec JSON merged in.
    fn mk(req: serde_json::Value, lim: serde_json::Value, extra: serde_json::Value) -> Pod {
        let mut spec = serde_json::json!({"containers": [{
            "name": "container", "image": "busybox",
            "resources": {"requests": req, "limits": lim}
        }]});
        if let Some(m) = extra.as_object() {
            for (k, v) in m {
                spec[k] = v.clone();
            }
        }
        pod(
            serde_json::json!({"metadata": {"name": "pod", "namespace": "ns", "resourceVersion": "1"}, "spec": spec}),
        )
    }

    fn resize_errs(old: &Pod, new: &Pod) -> Vec<String> {
        validate_pod_resize(new, old)
            .iter()
            .map(|e| format!("{}: {}", e.field, e.detail))
            .collect()
    }

    /// TestValidatePodResize: "Pod QoS change, *" cases (validation_test.go:27935).
    #[test]
    fn resize_refuses_qos_changes_and_allows_unchanged_classes() {
        use serde_json::json;
        let none = json!({});
        let guaranteed = |c: &str| json!({"cpu": c, "memory": "100Mi"});
        let g1 = mk(guaranteed("100m"), guaranteed("100m"), none.clone());
        let g2 = mk(guaranteed("200m"), guaranteed("200m"), none.clone());
        assert!(resize_errs(&g1, &g2).is_empty());
        let burstable = mk(json!({"cpu": "100m"}), none.clone(), none.clone());
        let burstable2 = mk(json!({"cpu": "200m"}), none.clone(), none.clone());
        assert!(resize_errs(&burstable, &burstable2).is_empty());
        let besteffort = mk(none.clone(), none.clone(), none.clone());
        for (old, new) in [
            (&g1, &burstable),
            (&burstable, &g1),
            (&besteffort, &burstable),
            (&burstable, &besteffort),
        ] {
            let errs = resize_errs(old, new);
            assert!(
                errs.iter()
                    .any(|e| e.contains("Pod QOS Class may not change as a result of resizing")),
                "{errs:?}"
            );
        }
    }

    /// "storage {limit,request} change" and the sidecar / non-sidecar init
    /// container cases.
    #[test]
    fn resize_mutates_only_cpu_and_memory_of_containers_and_sidecars() {
        use serde_json::json;
        let none = json!({});
        let old = mk(
            json!({"cpu": "1", "ephemeral-storage": "1Gi"}),
            none.clone(),
            none.clone(),
        );
        let new = mk(
            json!({"cpu": "1", "ephemeral-storage": "2Gi"}),
            none.clone(),
            none.clone(),
        );
        assert_eq!(
            resize_errs(&old, &new),
            vec!["spec: only cpu and memory resources are mutable"]
        );

        let init = |restart: Option<&str>, req: serde_json::Value| {
            let mut c = json!({"name": "init", "image": "busybox", "resources": {"requests": req}});
            if let Some(r) = restart {
                c["restartPolicy"] = json!(r);
            }
            mk(
                json!({"cpu": "1"}),
                none.clone(),
                json!({"initContainers": [c]}),
            )
        };
        // sidecar: cpu is mutable, storage is not.
        let old = init(
            Some("Always"),
            json!({"cpu": "1", "ephemeral-storage": "1Gi"}),
        );
        assert!(resize_errs(
            &old,
            &init(
                Some("Always"),
                json!({"cpu": "2", "ephemeral-storage": "1Gi"})
            )
        )
        .is_empty());
        assert_eq!(
            resize_errs(
                &old,
                &init(
                    Some("Always"),
                    json!({"cpu": "1", "ephemeral-storage": "2Gi"})
                )
            ),
            vec!["spec: only cpu and memory resources for sidecar containers are mutable"]
        );
        // a plain init container's resources are immutable.
        let old = init(None, json!({"cpu": "1"}));
        assert_eq!(
            resize_errs(&old, &init(None, json!({"cpu": "2"}))),
            vec!["spec: resources for non-sidecar init containers are immutable"]
        );
        // removing a sidecar request is refused.
        let old = init(Some("Always"), json!({"cpu": "1", "memory": "1Gi"}));
        let errs = resize_errs(&old, &init(Some("Always"), json!({"cpu": "1"})));
        assert!(
            errs.contains(
                &"spec.initContainers[0].resources.requests: resource requests cannot be removed"
                    .to_string()
            ),
            "{errs:?}"
        );
    }

    /// "pod container addition / removal / reorder / rename" and "change resize
    /// restart policy".
    #[test]
    fn resize_refuses_structural_changes() {
        use serde_json::json;
        let two = |a: &str, b: &str| {
            pod(
                json!({"metadata": {"name": "pod", "namespace": "ns", "resourceVersion": "1"}, "spec": {"containers": [
                    {"name": a, "image": "busybox"}, {"name": b, "image": "busybox"}
                ]}}),
            )
        };
        let one = pod(
            json!({"metadata": {"name": "pod", "namespace": "ns", "resourceVersion": "1"}, "spec": {"containers": [
                {"name": "a", "image": "busybox"}
            ]}}),
        );
        assert_eq!(
            resize_errs(&one, &two("a", "b")),
            vec!["spec.containers: containers may not be added or removed on resize"]
        );
        assert_eq!(
            resize_errs(&two("a", "b"), &one),
            vec!["spec.containers: containers may not be added or removed on resize"]
        );
        assert_eq!(
            resize_errs(&two("a", "b"), &two("b", "a")),
            vec![
                "spec.containers[0].name: containers may not be renamed or reordered on resize",
                "spec.containers[1].name: containers may not be renamed or reordered on resize",
            ]
        );

        let none = json!({});
        let old = mk(json!({"cpu": "1"}), none.clone(), none.clone());
        let mut new = old.clone();
        new.spec.as_mut().unwrap().containers[0].resize_policy = serde_json::from_value(
            json!([{"resourceName": "cpu", "restartPolicy": "RestartContainer"}]),
        )
        .unwrap();
        assert_eq!(
            resize_errs(&old, &new),
            vec!["spec: only cpu and memory resources are mutable"]
        );
    }

    /// Windows pods: refused with or without a resource change.
    #[test]
    fn resize_refuses_windows_pods() {
        use serde_json::json;
        let os = json!({"os": {"name": "windows"}});
        let old = mk(json!({"cpu": "1"}), json!({}), os.clone());
        let new = mk(json!({"cpu": "2"}), json!({}), os);
        assert_eq!(
            resize_errs(&old, &new),
            vec![": windows pods cannot be resized"]
        );
        assert_eq!(
            resize_errs(&old, &old),
            vec![": windows pods cannot be resized"]
        );
    }

    /// validation.go:6205-6207: the "may not be changed" error appends
    /// `diff.Diff(old, *new)` — a unified diff (context 3) of
    /// `json.MarshalIndent` of the INTERNAL `core.EphemeralContainer`, which has
    /// no json tags (Go field names, declaration order, `EphemeralContainerCommon`
    /// inlined, nil slices/pointers as `null`, `Resources` always an object).
    #[test]
    fn ephemeral_update_changed_error_carries_diff_text() {
        use serde_json::json;
        let with = |image: &str| {
            pod(json!({
                "metadata": {"name": "pod", "namespace": "ns", "resourceVersion": "1"},
                "spec": {"containers": [{"name": "cnt", "image": "busybox"}],
                         "restartPolicy": "OnFailure",
                         "ephemeralContainers": [{"name": "a", "image": image}]}
            }))
        };
        let errs = validate_pod_ephemeral_containers_update(&with("other"), &with("busybox"));
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert_eq!(
            errs[0].detail,
            concat!(
                "existing ephemeral containers \"a\" may not be changed\n",
                "@@ -1,6 +1,6 @@\n",
                " {\n",
                "  \"Name\": \"a\",\n",
                "- \"Image\": \"busybox\",\n",
                "+ \"Image\": \"other\",\n",
                "  \"Command\": null,\n",
                "  \"Args\": null,\n",
                "  \"WorkingDir\": \"\",\n",
            )
        );
    }

    /// TestValidatePodEphemeralContainersUpdate: removal and change refused,
    /// addition and reordering allowed (validation_test.go:16059).
    #[test]
    fn ephemeral_update_allows_add_and_reorder_only() {
        use serde_json::json;
        let with = |ecs: serde_json::Value| {
            pod(json!({
                "metadata": {"name": "pod", "namespace": "ns", "resourceVersion": "1"},
                "spec": {"containers": [{"name": "cnt", "image": "busybox"}],
                         "restartPolicy": "OnFailure", "ephemeralContainers": ecs}
            }))
        };
        let a = json!({"name": "a", "image": "busybox"});
        let b = json!({"name": "b", "image": "busybox"});
        let msgs = |new: &Pod, old: &Pod| -> Vec<String> {
            validate_pod_ephemeral_containers_update(new, old)
                .into_iter()
                .map(|e| e.detail)
                .collect()
        };
        // add
        assert!(msgs(&with(json!([a, b])), &with(json!([a]))).is_empty());
        // reorder
        assert!(msgs(&with(json!([b, a])), &with(json!([a, b]))).is_empty());
        // remove
        let errs = msgs(&with(json!([b])), &with(json!([a, b])));
        assert_eq!(
            errs,
            vec!["existing ephemeral containers \"a\" may not be removed\n"]
        );
        // change
        let a2 = json!({"name": "a", "image": "other"});
        let errs = msgs(&with(json!([a2])), &with(json!([a])));
        assert!(errs[0].starts_with("existing ephemeral containers \"a\" may not be changed\n"));
        // an unknown targetContainerName is NotFound
        let t = json!({"name": "t", "image": "busybox", "targetContainerName": "nope"});
        let errs = validate_pod_ephemeral_containers_update(&with(json!([t])), &with(json!([])));
        assert!(
            errs.iter()
                .any(|e| e.field == "spec.ephemeralContainers[0].targetContainerName"),
            "{errs:?}"
        );
        // forbidden fields
        let r = json!({"name": "r", "image": "busybox", "resources": {"limits": {"cpu": "1"}}});
        let errs = validate_pod_ephemeral_containers_update(&with(json!([r])), &with(json!([])));
        assert!(
            errs.iter()
                .any(|e| e.field == "spec.ephemeralContainers[0].resources"
                    && e.detail == "cannot be set for an Ephemeral Container"),
            "{errs:?}"
        );
    }

    /// Static pods take neither subresource (validation.go:6190, :6225).
    #[test]
    fn mirror_pods_refuse_both_subresources() {
        let old = pod(serde_json::json!({
            "metadata": {"name": "p", "annotations": {MIRROR_POD_ANNOTATION_KEY: "x"}},
            "spec": {"containers": [{"name": "c", "image": "busybox"}]}
        }));
        let errs = validate_pod_resize(&old.clone(), &old);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert_eq!(errs[0].detail, "static pods cannot be resized");
        let errs = validate_pod_ephemeral_containers_update(&old.clone(), &old);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert_eq!(
            errs[0].detail,
            "static pods do not support ephemeral containers"
        );
    }

    /// A pod with pod-level resources (`mkPodWithPodLevelResources`-style).
    fn mk_pl(pod_res: serde_json::Value) -> Pod {
        pod(serde_json::json!({
            "metadata": {"name": "pod", "namespace": "ns", "resourceVersion": "1"},
            "spec": {
                "containers": [{"name": "c", "image": "busybox"}],
                "resources": pod_res
            }
        }))
    }

    /// `validatePodLevelResourcesResize` (validation.go:6384-6412) with the
    /// `InPlacePodLevelResourcesVerticalScaling` gate on.
    #[test]
    #[serial_test::serial]
    fn pod_level_resize_with_the_gate_on() {
        let _g = rusternetes_common::feature_gates::with_feature(
            Feature::InPlacePodLevelResourcesVerticalScaling,
            true,
        );
        let old = mk_pl(json!({"requests": {"cpu": "1"}, "limits": {"cpu": "1"}}));
        // cpu/memory are mutable.
        let new = mk_pl(json!({"requests": {"cpu": "1"}, "limits": {"cpu": "2"}}));
        assert_eq!(resize_errs(&old, &new), Vec::<String>::new());
        // removing the pod-level resources block.
        let mut removed = new.clone();
        removed.spec.as_mut().unwrap().resources = None;
        assert!(resize_errs(&old, &removed)
            .contains(&"spec.resources: pod-level resources cannot be removed".to_string()));
        // removing requests.
        let no_req = mk_pl(json!({"limits": {"cpu": "1"}}));
        assert!(resize_errs(&old, &no_req).contains(
            &"spec.resources.requests: pod-level resource requests cannot be removed".to_string()
        ));
        // only cpu and memory are mutable.
        let storage_old = mk_pl(json!({"requests": {"cpu": "1"}}));
        let storage_new = mk_pl(json!({"requests": {"cpu": "1", "ephemeral-storage": "1Gi"}}));
        assert!(resize_errs(&storage_old, &storage_new)
            .contains(&"spec: only cpu and memory resources are mutable at pod-level".to_string()));
    }

    /// `ValidatePodResize` -> `validatePodMetadataAndSpec(newPod, opts)`: opts
    /// come from the old pod (`GetValidationOptionsFromPodSpecAndMeta`), so an
    /// already-invalid pod-deletion-cost annotation stays tolerated.
    #[test]
    fn resize_tolerates_invalid_deletion_cost_already_on_old_pod() {
        let mut p = mk(json!({"cpu": "1"}), json!({"cpu": "1"}), json!({}));
        p.metadata.annotations = Some(HashMap::from([(
            "controller.kubernetes.io/pod-deletion-cost".to_string(),
            "abc".to_string(),
        )]));
        assert_eq!(resize_errs(&p, &p), Vec::<String>::new());
    }

    /// `reflect.DeepEqual(field, zero)` (validation.go:3799): a non-nil empty
    /// slice or map is "set", unlike nil.
    #[test]
    fn ephemeral_allow_list_treats_empty_but_present_as_set() {
        let with = |ec: serde_json::Value| {
            pod(json!({
                "metadata": {"name": "pod", "namespace": "ns", "resourceVersion": "1"},
                "spec": {"containers": [{"name": "cnt", "image": "busybox"}],
                         "ephemeralContainers": [ec]}
            }))
        };
        let empty = pod(json!({
            "metadata": {"name": "pod", "namespace": "ns", "resourceVersion": "1"},
            "spec": {"containers": [{"name": "cnt", "image": "busybox"}]}
        }));
        for (key, val) in [
            ("ports", json!([])),
            ("resizePolicy", json!([])),
            ("restartPolicyRules", json!([])),
            ("resources", json!({"limits": {}})),
        ] {
            let mut ec = json!({"name": "e", "image": "busybox"});
            ec[key] = val;
            let errs = validate_pod_ephemeral_containers_update(&with(ec), &empty);
            assert!(
                errs.iter()
                    .any(|e| e.field == format!("spec.ephemeralContainers[0].{key}")
                        && e.detail == "cannot be set for an Ephemeral Container"),
                "{key}: {errs:?}"
            );
        }
        // an empty resources block is the zero value.
        let ec = json!({"name": "e", "image": "busybox", "resources": {}});
        let errs = validate_pod_ephemeral_containers_update(&with(ec), &empty);
        assert!(
            errs.iter().all(|e| !e.field.ends_with("resources")),
            "{errs:?}"
        );
    }
}
