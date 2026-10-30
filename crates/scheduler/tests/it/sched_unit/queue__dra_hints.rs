//! Unit tests moved from `src/queue/dra_hints.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::{Pod, ResourceClaim};
    pub use rusternetes_scheduler::queue::events::*;
    pub use std::collections::HashMap;
    pub use std::sync::{Arc, RwLock};
    pub use tracing::debug;
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::queue::dra_hints::*;
    use serde_json::{json, Value};

    const POD_UID: &str = "1234";
    const CLAIM_NAME: &str = "my-pod-my-resource";

    fn pod(uid: &str, claim: Value, status: Option<Value>) -> Pod {
        let mut p = json!({
            "metadata": {"name": "my-pod", "namespace": "default", "uid": uid},
            "spec": {"resourceClaims": [claim]}
        });
        if let Some(s) = status {
            p["status"] = s;
        }
        serde_json::from_value(p).unwrap()
    }
    fn pod_with_claim_name() -> Pod {
        pod(
            POD_UID,
            json!({"name": "my-resource", "resourceClaimName": CLAIM_NAME}),
            None,
        )
    }
    fn pod_with_template() -> Pod {
        pod(
            POD_UID,
            json!({"name": "my-resource", "resourceClaimTemplateName": CLAIM_NAME}),
            None,
        )
    }
    fn pod_with_template_in_status() -> Pod {
        pod(
            POD_UID,
            json!({"name": "my-resource", "resourceClaimTemplateName": CLAIM_NAME}),
            Some(json!({"resourceClaimStatuses":
                [{"name": "my-resource", "resourceClaimName": CLAIM_NAME}]})),
        )
    }
    fn pod_with_two_templates(status: Option<Value>) -> Pod {
        let mut p = json!({
            "metadata": {"name": "my-pod", "namespace": "default", "uid": POD_UID},
            "spec": {"resourceClaims": [
                {"name": "my-resource", "resourceClaimTemplateName": CLAIM_NAME},
                {"name": "my-resource-2", "resourceClaimTemplateName": CLAIM_NAME}]}
        });
        if let Some(s) = status {
            p["status"] = s;
        }
        serde_json::from_value(p).unwrap()
    }
    /// `pendingClaim`: owned by the pod.
    fn pending_claim() -> ResourceClaim {
        serde_json::from_value(json!({
            "metadata": {"name": CLAIM_NAME, "namespace": "default", "uid": "claim-uid",
                "ownerReferences": [{"apiVersion": "v1", "kind": "Pod", "name": "my-pod",
                    "uid": POD_UID, "controller": true}]},
            "spec": {"devices": {"driver": "some-driver"}}
        }))
        .unwrap()
    }
    fn allocated(mut c: ResourceClaim) -> ResourceClaim {
        c.status = Some(
            serde_json::from_value(json!({"allocation": {"devices": {"results": []}}})).unwrap(),
        );
        c
    }
    /// `otherAllocatedClaim`.
    fn other_allocated_claim() -> ResourceClaim {
        let mut c: ResourceClaim = serde_json::from_value(json!({
            "metadata": {"name": "not-my-claim", "namespace": "default", "uid": "other-uid"},
            "spec": {"devices": {"driver": "some-driver"}}
        }))
        .unwrap();
        c = allocated(c);
        c
    }
    fn cache(claims: &[ResourceClaim]) -> ResourceClaimCache {
        let c = ResourceClaimCache::default();
        for claim in claims {
            c.insert(claim.clone());
        }
        c
    }
    fn obj<T: Send + Sync + 'static>(t: T) -> Box<EventObject> {
        Box::new(t)
    }

    /// `Test_isSchedulableAfterClaimChange` (dynamicresources_test.go:2997).
    /// The informer cache already holds a created/updated claim when the
    /// event handler runs, so the new object is cached for every case.
    #[test]
    fn claim_change_table() {
        let pending = pending_claim();
        let mut wrong_owner = allocated(pending.clone());
        wrong_owner.metadata.owner_references.as_mut().unwrap()[0].uid += "123";
        let mut unrelated = allocated(pending.clone());
        unrelated.metadata.name += "-foo";
        unrelated.metadata.uid += "123";
        let mut with_finalizer = pending.clone();
        with_finalizer.metadata.finalizers = Some(vec!["foo".into()]);
        let other = other_allocated_claim();
        let mut other_dealloc = other.clone();
        other_dealloc.status = None;

        type Case = (
            &'static str,
            Pod,
            Vec<ResourceClaim>,
            Option<Box<EventObject>>,
            Option<Box<EventObject>>,
            Result<QueueingHint, ()>,
        );
        let cases: Vec<Case> = vec![
            (
                "skip-deletes",
                pod_with_template(),
                vec![],
                Some(obj(allocated(pending.clone()))),
                None,
                Ok(QueueingHint::QueueSkip),
            ),
            (
                "backoff-wrong-new-object",
                pod_with_template(),
                vec![],
                None,
                Some(obj("not-a-claim".to_string())),
                Err(()),
            ),
            (
                "skip-wrong-claim",
                pod_with_template(),
                vec![],
                None,
                Some(obj(wrong_owner)),
                Ok(QueueingHint::QueueSkip),
            ),
            (
                "skip-unrelated-claim",
                pod_with_template(),
                vec![allocated(pending.clone())],
                None,
                Some(obj(unrelated)),
                Ok(QueueingHint::QueueSkip),
            ),
            (
                "queue-on-add",
                pod_with_claim_name(),
                vec![pending.clone()],
                None,
                Some(obj(pending.clone())),
                Ok(QueueingHint::Queue),
            ),
            (
                "backoff-wrong-old-object",
                pod_with_claim_name(),
                vec![pending.clone()],
                Some(obj("not-a-claim".to_string())),
                Some(obj(pending.clone())),
                Err(()),
            ),
            (
                "skip-adding-finalizer",
                pod_with_claim_name(),
                vec![with_finalizer.clone()],
                Some(obj(pending.clone())),
                Some(obj(with_finalizer)),
                Ok(QueueingHint::QueueSkip),
            ),
            (
                "queue-on-status-change",
                pod_with_claim_name(),
                vec![allocated(pending.clone())],
                Some(obj(pending.clone())),
                Some(obj(allocated(pending.clone()))),
                Ok(QueueingHint::Queue),
            ),
            (
                "claim-deallocate",
                pod_with_claim_name(),
                vec![pending.clone(), other_dealloc.clone()],
                Some(obj(other)),
                Some(obj(other_dealloc)),
                Ok(QueueingHint::Queue),
            ),
        ];
        for (name, pod, claims, old, new, want) in cases {
            let got = is_schedulable_after_claim_change(
                &cache(&claims),
                &pod,
                old.as_deref(),
                new.as_deref(),
            );
            match want {
                Ok(hint) => assert_eq!(got, Ok(hint), "{name}"),
                Err(()) => assert!(got.is_err(), "{name}"),
            }
        }
    }

    /// `Test_isSchedulableAfterPodChange` (dynamicresources_test.go:3154).
    #[test]
    fn pod_change_table() {
        let pending = pending_claim();
        let mut other_pod = pod_with_template();
        other_pod.metadata.name += "2";
        other_pod.metadata.uid += "2";
        let incomplete = pod_with_two_templates(Some(json!({"resourceClaimStatuses":
            [{"name": "my-resource", "resourceClaimName": CLAIM_NAME}]})));

        type Case = (
            &'static str,
            Vec<ResourceClaim>,
            Pod,
            Box<EventObject>,
            Result<QueueingHint, ()>,
        );
        let cases: Vec<Case> = vec![
            (
                "backoff-wrong-new-object",
                vec![],
                pod_with_template(),
                obj("not-a-claim".to_string()),
                Err(()),
            ),
            (
                "complete",
                vec![pending.clone()],
                pod_with_template(),
                obj(pod_with_template_in_status()),
                Ok(QueueingHint::Queue),
            ),
            (
                "wrong-pod",
                vec![pending.clone()],
                other_pod,
                obj(pod_with_template_in_status()),
                Ok(QueueingHint::QueueSkip),
            ),
            (
                "missing-claim",
                vec![],
                pod_with_template(),
                obj(pod_with_template_in_status()),
                Ok(QueueingHint::QueueSkip),
            ),
            (
                "incomplete",
                vec![pending.clone()],
                pod_with_two_templates(None),
                obj(incomplete),
                Ok(QueueingHint::QueueSkip),
            ),
        ];
        for (name, claims, pod, new, want) in cases {
            let got = is_schedulable_after_pod_change(&cache(&claims), &pod, None, Some(&*new));
            match want {
                Ok(hint) => assert_eq!(got, Ok(hint), "{name}"),
                Err(()) => assert!(got.is_err(), "{name}"),
            }
        }
    }

    #[test]
    fn claim_being_deleted_is_not_schedulable() {
        let mut deleting = pending_claim();
        deleting.metadata.deletion_timestamp = Some(chrono::Utc::now());
        let c = cache(&[deleting.clone()]);
        let new = obj(deleting);
        assert_eq!(
            is_schedulable_after_claim_change(&c, &pod_with_claim_name(), None, Some(&*new)),
            Ok(QueueingHint::QueueSkip)
        );
    }
}
