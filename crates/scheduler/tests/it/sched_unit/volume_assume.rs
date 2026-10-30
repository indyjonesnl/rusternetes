//! Unit tests moved from `src/volume_assume.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::service_account::ObjectReference;
    pub use rusternetes_common::resources::{PersistentVolume, PersistentVolumeClaim, Pod, Volume};
    pub use rusternetes_scheduler::volume_binding::{
        ephemeral_volume_is_for_pod, is_pvc_fully_bound, is_volume_bound_to_claim, pod_volumes,
        volume_pvc_name, BindingInfo, DynamicProvision, PodVolumes, VolumeSnapshot,
        ANN_SELECTED_NODE,
    };
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::volume_assume::*;
    use serde_json::{json, Value};

    // The resource structs do not implement PartialEq; compare wire form.
    macro_rules! assert_same {
        ($a:expr, $b:expr $(, $($m:tt)+)?) => {
            assert_eq!(
                serde_json::to_value(&$a).unwrap(),
                serde_json::to_value(&$b).unwrap()
                $(, $($m)+)?
            )
        };
    }
    macro_rules! assert_differ {
        ($a:expr, $b:expr) => {
            assert_ne!(
                serde_json::to_value(&$a).unwrap(),
                serde_json::to_value(&$b).unwrap()
            )
        };
    }

    fn mk_pv(name: &str, rv: &str, claim_ref: Value) -> PersistentVolume {
        let mut spec = json!({
            "capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
            "storageClassName": "wait", "hostPath": {"path": "/x"}
        });
        if !claim_ref.is_null() {
            spec["claimRef"] = claim_ref;
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolume",
            "metadata": {"name": name, "resourceVersion": rv}, "spec": spec,
            "status": {"phase": "Available"}
        }))
        .unwrap()
    }

    fn mk_pvc(name: &str, rv: &str, volume_name: &str, ann: Value) -> PersistentVolumeClaim {
        let mut spec = json!({
            "resources": {"requests": {"storage": "1Gi"}}, "storageClassName": "wait"
        });
        if !volume_name.is_empty() {
            spec["volumeName"] = json!(volume_name);
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": "testns", "uid": format!("{name}-uid"),
                         "resourceVersion": rv, "annotations": ann},
            "spec": spec, "status": {"phase": "Pending"}
        }))
        .unwrap()
    }

    fn pod_with(pvcs: &[&PersistentVolumeClaim]) -> Pod {
        let vols: Vec<Value> = pvcs
            .iter()
            .map(|c| {
                json!({"name": format!("vol-{}", c.metadata.name),
                       "persistentVolumeClaim": {"claimName": c.metadata.name}})
            })
            .collect();
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "test-pod", "namespace": "testns", "uid": "pod-uid"},
            "spec": {"containers": [{"name": "c", "image": "i"}], "volumes": vols}
        }))
        .unwrap()
    }

    fn bound_pvc(name: &str, pv: &str) -> PersistentVolumeClaim {
        mk_pvc(
            name,
            "5",
            pv,
            json!({"pv.kubernetes.io/bind-completed": "yes"}),
        )
    }

    fn claim_ref_for(pvc: &PersistentVolumeClaim) -> Value {
        json!({"kind": "PersistentVolumeClaim", "apiVersion": "v1",
               "namespace": "testns", "name": pvc.metadata.name, "uid": pvc.metadata.uid})
    }

    fn binder_with(pvcs: &[PersistentVolumeClaim], pvs: &[PersistentVolume]) -> VolumeBinder {
        let mut b = VolumeBinder::new();
        b.sync(&VolumeSnapshot {
            pvcs: pvcs.to_vec(),
            pvs: pvs.to_vec(),
            ..Default::default()
        });
        b
    }

    fn binding(pv: &PersistentVolume, pvc: &PersistentVolumeClaim) -> BindingInfo {
        BindingInfo {
            pv: pv.clone(),
            pvc: pvc.clone(),
        }
    }

    // ---- assume_cache_test.go TestAssume ----

    #[test]
    fn assume_cache_assume_scenarios() {
        // (old rv, new rv, expect ok)
        let cases = [
            ("5", "5", true),  // success-same-version
            ("5", "6", true),  // success-new-higher-version
            ("5", "4", false), // fail-new-lower-version
            ("5", "a", false), // fail-new-bad-version
            ("a", "5", false), // fail-old-bad-version
        ];
        for (old_rv, new_rv, ok) in cases {
            let mut c = AssumeCache::<PersistentVolumeClaim>::new();
            let old = mk_pvc("pvc1", old_rv, "", json!({}));
            let new = mk_pvc("pvc1", new_rv, "", json!({"x": "y"}));
            c.add(&old);
            assert_eq!(c.assume(&new).is_ok(), ok, "{old_rv}->{new_rv}");
            let expect = if ok { &new } else { &old };
            assert_same!(c.get("testns/pvc1").unwrap(), expect);
            assert_same!(c.get_api_obj("testns/pvc1").unwrap(), &old);
        }
    }

    #[test]
    fn assume_cache_assume_unknown_object_is_not_found() {
        let mut c = AssumeCache::<PersistentVolumeClaim>::new();
        c.add(&mk_pvc("pvc2", "5", "", json!({})));
        let err = c.assume(&mk_pvc("pvc1", "5", "", json!({}))).unwrap_err();
        assert!(matches!(err, AssumeError::NotFound { .. }), "{err:?}");
    }

    // ---- assume_cache_test.go TestRestore ----

    #[test]
    fn assume_cache_restore() {
        let mut c = AssumeCache::<PersistentVolumeClaim>::new();
        c.restore("nothing"); // no-op on a missing key
        let old = mk_pvc("pvc1", "5", "", json!({}));
        let new = mk_pvc("pvc1", "5", "", json!({"a": "b"}));
        c.add(&old);
        c.restore("testns/pvc1");
        assert_same!(c.get("testns/pvc1").unwrap(), &old);
        c.assume(&new).unwrap();
        assert_same!(c.get("testns/pvc1").unwrap(), &new);
        c.restore("testns/pvc1");
        assert_same!(c.get("testns/pvc1").unwrap(), &old);
    }

    // ---- assume_cache.go add(): "don't override assumed objects due to resync" ----

    #[test]
    fn assume_cache_resync_same_version_keeps_assumed_object() {
        let mut c = AssumeCache::<PersistentVolumeClaim>::new();
        let old = mk_pvc("pvc1", "5", "", json!({}));
        let assumed = mk_pvc("pvc1", "5", "", json!({"a": "b"}));
        c.add(&old);
        c.assume(&assumed).unwrap();
        c.add(&old); // informer resync, same version
        assert_same!(c.get("testns/pvc1").unwrap(), &assumed);
        let newer = mk_pvc("pvc1", "6", "", json!({}));
        c.add(&newer); // genuinely newer API object wins
        assert_same!(c.get("testns/pvc1").unwrap(), &newer);
        assert_same!(c.get_api_obj("testns/pvc1").unwrap(), &newer);
    }

    #[test]
    fn assume_cache_sync_deletes_absent_and_lists_latest() {
        let mut c = AssumeCache::<PersistentVolume>::new();
        c.sync(&[mk_pv("a", "1", Value::Null), mk_pv("b", "1", Value::Null)]);
        assert_eq!(c.list().len(), 2);
        c.sync(&[mk_pv("a", "1", Value::Null)]);
        assert_eq!(c.list().len(), 1);
        assert!(matches!(c.get("b"), Err(AssumeError::NotFound { .. })));
    }

    // ---- pv_helpers.go GetBindVolumeToClaim ----

    #[test]
    fn get_bind_volume_to_claim_sets_claim_ref_and_bound_by_controller() {
        let pvc = mk_pvc("pvc1", "5", "", json!({}));
        let pv = mk_pv("pv1", "7", Value::Null);
        let (new, dirty) = get_bind_volume_to_claim(&pv, &pvc).unwrap();
        assert!(dirty);
        let cr = new.spec.claim_ref.as_ref().unwrap();
        assert_eq!(cr.name.as_deref(), Some("pvc1"));
        assert_eq!(cr.namespace.as_deref(), Some("testns"));
        assert_eq!(cr.uid.as_deref(), Some("pvc1-uid"));
        assert_eq!(cr.kind.as_deref(), Some("PersistentVolumeClaim"));
        assert_eq!(cr.api_version.as_deref(), Some("v1"));
        assert_eq!(
            new.metadata.annotations.as_ref().unwrap()[ANN_BOUND_BY_CONTROLLER],
            "yes"
        );
        assert_eq!(pv.spec.claim_ref, None, "input must not be mutated");
        assert_eq!(new.metadata.resource_version.as_deref(), Some("7"));
    }

    #[test]
    fn get_bind_volume_to_claim_already_bound_is_not_dirty() {
        let pvc = mk_pvc("pvc1", "5", "", json!({}));
        let pv = mk_pv("pv1", "7", claim_ref_for(&pvc));
        let (new, dirty) = get_bind_volume_to_claim(&pv, &pvc).unwrap();
        assert!(!dirty);
        assert_same!(new, pv);
    }

    // ---- binder_test.go TestAssumePodVolumes ----

    #[test]
    fn assume_pod_volumes_all_bound() {
        let pvc = bound_pvc("bound", "pv-bound");
        let pv = mk_pv("pv-bound", "1", claim_ref_for(&pvc));
        let mut b = binder_with(std::slice::from_ref(&pvc), std::slice::from_ref(&pv));
        let mut vols = PodVolumes::default();
        let all = b
            .assume_pod_volumes(&pod_with(&[&pvc]), "node1", &mut vols)
            .unwrap();
        assert!(all);
    }

    #[test]
    fn assume_pod_volumes_one_and_two_bindings() {
        let pvc1 = mk_pvc("pvc1", "5", "", json!({}));
        let pvc2 = mk_pvc("pvc2", "5", "", json!({}));
        let pv1 = mk_pv("pv1", "7", Value::Null);
        let pv2 = mk_pv("pv2", "7", Value::Null);
        let mut b = binder_with(&[pvc1.clone(), pvc2.clone()], &[pv1.clone(), pv2.clone()]);
        let mut vols = PodVolumes {
            static_bindings: vec![binding(&pv1, &pvc1), binding(&pv2, &pvc2)],
            dynamic_provisions: vec![],
        };
        let all = b
            .assume_pod_volumes(&pod_with(&[&pvc1, &pvc2]), "node1", &mut vols)
            .unwrap();
        assert!(!all);
        // PodVolumes now carries the assumed (cache) objects.
        for (i, pv) in ["pv1", "pv2"].iter().enumerate() {
            let assumed = &vols.static_bindings[i].pv;
            assert_eq!(assumed.metadata.name, *pv);
            assert!(assumed.spec.claim_ref.is_some());
            assert_same!(b.pv_cache.get(pv).unwrap(), assumed);
            assert_differ!(b.pv_cache.get_api_obj(pv).unwrap(), assumed);
        }
    }

    #[test]
    fn assume_pod_volumes_pv_already_bound_not_dirty() {
        let pvc = mk_pvc("pvc1", "5", "", json!({}));
        let pv = mk_pv("pv1", "7", claim_ref_for(&pvc));
        let mut b = binder_with(std::slice::from_ref(&pvc), std::slice::from_ref(&pv));
        let mut vols = PodVolumes {
            static_bindings: vec![binding(&pv, &pvc)],
            dynamic_provisions: vec![],
        };
        assert!(!b
            .assume_pod_volumes(&pod_with(&[&pvc]), "node1", &mut vols)
            .unwrap());
        assert_same!(vols.static_bindings[0].pv, pv);
        assert_same!(
            b.pv_cache.get("pv1").unwrap(),
            b.pv_cache.get_api_obj("pv1").unwrap()
        );
    }

    #[test]
    fn assume_pod_volumes_tmpupdate_failed_restores_cache() {
        let pvc1 = mk_pvc("pvc1", "5", "", json!({}));
        let pvc2 = mk_pvc("pvc2", "5", "", json!({}));
        let pv1 = mk_pv("pv1", "7", Value::Null);
        let pv2 = mk_pv("pv2", "7", Value::Null);
        // pv2 is not in the cache -> second Assume fails; first is reverted.
        let mut b = binder_with(std::slice::from_ref(&pvc1), std::slice::from_ref(&pv1));
        let mut vols = PodVolumes {
            static_bindings: vec![binding(&pv1, &pvc1), binding(&pv2, &pvc2)],
            dynamic_provisions: vec![],
        };
        let all = b.assume_pod_volumes(&pod_with(&[&pvc1]), "node1", &mut vols);
        assert!(all.is_err());
        assert_same!(b.pv_cache.get("pv1").unwrap(), &pv1);
        assert_eq!(
            vols.static_bindings.len(),
            2,
            "PodVolumes untouched on error"
        );
    }

    #[test]
    fn assume_pod_volumes_binding_and_provisioning() {
        let unbound = mk_pvc("unbound", "5", "", json!({}));
        let provisioned = mk_pvc("provisioned", "5", "", json!({}));
        let pv = mk_pv("pv1", "7", Value::Null);
        let mut b = binder_with(
            &[unbound.clone(), provisioned.clone()],
            std::slice::from_ref(&pv),
        );
        let mut vols = PodVolumes {
            static_bindings: vec![binding(&pv, &unbound)],
            dynamic_provisions: vec![DynamicProvision {
                pvc: provisioned.clone(),
                node_capacity: None,
            }],
        };
        let all = b
            .assume_pod_volumes(&pod_with(&[&unbound, &provisioned]), "node1", &mut vols)
            .unwrap();
        assert!(!all);
        let sel = |c: &PersistentVolumeClaim| {
            c.metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(ANN_SELECTED_NODE))
                .cloned()
        };
        assert_eq!(
            sel(&vols.dynamic_provisions[0].pvc).as_deref(),
            Some("node1")
        );
        assert_eq!(
            sel(b.pvc_cache.get("testns/provisioned").unwrap()).as_deref(),
            Some("node1")
        );
        // The lister's object must not be mutated.
        assert_eq!(sel(&provisioned), None);
        assert_eq!(
            sel(b.pvc_cache.get_api_obj("testns/provisioned").unwrap()),
            None
        );
    }

    #[test]
    fn assume_pod_volumes_provision_tmpupdate_failed_restores_cache() {
        let unbound = mk_pvc("unbound", "5", "", json!({}));
        let cached_higher = mk_pvc("provisioned", "6", "", json!({}));
        let stale = mk_pvc("provisioned", "5", "", json!({}));
        let pv = mk_pv("pv1", "7", Value::Null);
        let mut b = binder_with(
            &[unbound.clone(), cached_higher.clone()],
            std::slice::from_ref(&pv),
        );
        let mut vols = PodVolumes {
            static_bindings: vec![binding(&pv, &unbound)],
            dynamic_provisions: vec![DynamicProvision {
                pvc: stale,
                node_capacity: None,
            }],
        };
        let r = b.assume_pod_volumes(&pod_with(&[&unbound, &cached_higher]), "node1", &mut vols);
        assert!(r.is_err());
        assert_same!(b.pv_cache.get("pv1").unwrap(), &pv, "PV assume reverted");
        assert_same!(
            b.pvc_cache.get("testns/provisioned").unwrap(),
            &cached_higher
        );
    }

    // ---- binder_test.go TestRevertAssumedPodVolumes ----

    #[test]
    fn revert_assumed_pod_volumes_restores_both_caches() {
        let unbound = mk_pvc("unbound", "5", "", json!({}));
        let provisioned = mk_pvc("provisioned", "5", "", json!({}));
        let pv = mk_pv("pv1", "7", Value::Null);
        let mut b = binder_with(
            &[unbound.clone(), provisioned.clone()],
            std::slice::from_ref(&pv),
        );
        let mut vols = PodVolumes {
            static_bindings: vec![binding(&pv, &unbound)],
            dynamic_provisions: vec![DynamicProvision {
                pvc: provisioned.clone(),
                node_capacity: None,
            }],
        };
        b.assume_pod_volumes(&pod_with(&[&unbound, &provisioned]), "node1", &mut vols)
            .unwrap();
        assert_differ!(b.pv_cache.get("pv1").unwrap(), &pv);
        b.revert_assumed_pod_volumes(&vols);
        assert_same!(b.pv_cache.get("pv1").unwrap(), &pv);
        assert_same!(b.pvc_cache.get("testns/provisioned").unwrap(), &provisioned);
        // Idempotent (volume_binding.go Unreserve doc).
        b.revert_assumed_pod_volumes(&vols);
        assert_same!(b.pv_cache.get("pv1").unwrap(), &pv);
    }
}
