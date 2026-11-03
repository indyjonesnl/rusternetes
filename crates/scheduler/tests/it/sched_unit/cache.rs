//! Tests for `src/cache.rs`.
//!
//! Ports of upstream `backend/cache/cache_test.go`, restricted to the pod
//! state machine: `TestAssumePodScheduled`, `TestExpirePod`,
//! `TestAddPodWillConfirm`, `TestAddPodWillReplaceAssumed`,
//! `TestAddPodAfterExpiration`, `TestUpdatePod`, `TestUpdatePodAndGet`,
//! `TestExpireAddUpdatePod`, `TestRemovePod` and `TestForgetPod`. The
//! `NodeInfo` resource accounting those tests also assert is not ported (the
//! cache keeps no `NodeInfo`); a pod's `priority` stands in as the marker of
//! which version of the pod is cached.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::{Pod, PodSpec};
    pub use rusternetes_scheduler::cache::SchedulerCache;
    pub use std::time::{Duration, Instant};
}

mod tests {
    use super::__parent::*;

    fn pod(name: &str, node: &str, version: i32) -> Pod {
        let mut p = Pod::new(
            name,
            PodSpec {
                node_name: (!node.is_empty()).then(|| node.to_string()),
                priority: Some(version),
                ..Default::default()
            },
        );
        p.metadata.namespace = Some("default".to_string());
        p.metadata.uid = name.to_string();
        p
    }

    fn names(c: &SchedulerCache, node: &str) -> Vec<(String, i32)> {
        c.pods_on_node(node)
            .into_iter()
            .map(|p| (p.metadata.name.clone(), p.spec.unwrap().priority.unwrap()))
            .collect()
    }

    fn assume_and_finish(c: &SchedulerCache, p: &Pod, now: Instant) {
        c.assume_pod(p).unwrap();
        c.finish_binding_at(p, now).unwrap();
    }

    fn assert_forgotten(c: &SchedulerCache, p: &Pod) {
        assert!(!c.is_assumed_pod(p).unwrap(), "still assumed");
        assert!(c.get_pod(p).is_err(), "still in cache");
    }

    #[test]
    fn assume_pod_scheduled() {
        let c = SchedulerCache::new(Duration::from_secs(10));
        let (a, b) = (pod("a", "node", 0), pod("b", "node", 0));
        c.assume_pod(&a).unwrap();
        assert_eq!(names(&c, "node"), vec![("a".into(), 0)]);
        assert!(c.is_assumed_pod(&a).unwrap());
        c.assume_pod(&b).unwrap();
        assert_eq!(c.pods_on_node("node").len(), 2);
        assert_eq!(c.assumed_count(), 2);
        // "is in the cache, so can't be assumed"
        assert!(c.assume_pod(&a).is_err());
    }

    #[test]
    fn assume_pod_needs_a_uid() {
        let c = SchedulerCache::default();
        let mut p = pod("a", "node", 0);
        p.metadata.uid.clear();
        assert!(c.assume_pod(&p).is_err());
    }

    #[test]
    fn expire_pod() {
        let ttl = Duration::from_secs(10);
        let now = Instant::now();
        // assumed pod would expire
        let c = SchedulerCache::new(ttl);
        assume_and_finish(&c, &pod("t0", "node", 0), now);
        c.cleanup_assumed_pods(now + 2 * ttl);
        assert!(c.pods_on_node("node").is_empty());

        // first one would expire, second and third would not
        let c = SchedulerCache::new(ttl);
        assume_and_finish(&c, &pod("t0", "node", 0), now);
        assume_and_finish(&c, &pod("t1", "node", 0), now + 3 * ttl / 2);
        c.assume_pod(&pod("t2", "node", 0)).unwrap(); // binding not finished
        c.cleanup_assumed_pods(now + 2 * ttl);
        let got: Vec<String> = names(&c, "node").into_iter().map(|n| n.0).collect();
        assert_eq!(got, vec!["t1", "t2"]);

        // assumed pod would never expire (ttl 0)
        let c = SchedulerCache::new(Duration::ZERO);
        assume_and_finish(&c, &pod("t0", "node", 0), now);
        c.cleanup_assumed_pods(now + 3 * ttl);
        assert_eq!(c.pods_on_node("node").len(), 1);
    }

    #[test]
    fn add_pod_will_confirm() {
        let ttl = Duration::from_secs(10);
        let now = Instant::now();
        let c = SchedulerCache::new(ttl);
        let (a, b) = (pod("a", "node", 0), pod("b", "node", 0));
        assume_and_finish(&c, &a, now);
        assume_and_finish(&c, &b, now);
        c.add_pod(&a).unwrap();
        // pod already in added state
        assert!(c.add_pod(&a).is_err());
        c.cleanup_assumed_pods(now + 2 * ttl);
        // confirmed pods are not expired
        assert_eq!(names(&c, "node"), vec![("a".into(), 0)]);
    }

    #[test]
    fn add_pod_will_replace_assumed() {
        let now = Instant::now();
        let c = SchedulerCache::new(Duration::from_secs(10));
        assume_and_finish(&c, &pod("test-1", "assumed-node-1", 0), now);
        let added = pod("test-1", "actual-node", 0);
        c.add_pod(&added).unwrap();
        c.update_pod(&added, &pod("test-1", "actual-node", 1))
            .unwrap();
        assert!(c.pods_on_node("assumed-node-1").is_empty());
        assert_eq!(names(&c, "actual-node"), vec![("test-1".into(), 1)]);
        assert!(!c.is_assumed_pod(&added).unwrap());
    }

    #[test]
    fn add_pod_after_expiration() {
        let ttl = Duration::from_secs(10);
        let now = Instant::now();
        let c = SchedulerCache::new(ttl);
        let p = pod("test", "node", 0);
        assume_and_finish(&c, &p, now);
        c.cleanup_assumed_pods(now + 2 * ttl);
        assert_forgotten(&c, &p);
        c.add_pod(&p).unwrap();
        assert_eq!(names(&c, "node"), vec![("test".into(), 0)]);
        assert!(!c.is_assumed_pod(&p).unwrap());
    }

    #[test]
    fn update_pod() {
        let c = SchedulerCache::new(Duration::from_secs(10));
        let (v0, v1) = (pod("test", "node", 0), pod("test", "node", 1));
        c.add_pod(&v0).unwrap();
        c.update_pod(&v0, &v1).unwrap();
        assert_eq!(names(&c, "node"), vec![("test".into(), 1)]);
        c.update_pod(&v1, &v0).unwrap();
        assert_eq!(names(&c, "node"), vec![("test".into(), 0)]);
        // moving a confirmed pod to another node is a corrupted cache
        assert!(c.update_pod(&v0, &pod("test", "other", 0)).is_err());
        // updating a pod that was never added
        assert!(c
            .update_pod(&pod("x", "node", 0), &pod("x", "node", 1))
            .is_err());
    }

    #[test]
    fn update_pod_and_get() {
        let c = SchedulerCache::new(Duration::from_secs(10));
        let (v0, v1) = (pod("test", "node", 0), pod("test", "node", 1));
        c.add_pod(&v0).unwrap();
        c.update_pod(&v0, &v1).unwrap();
        assert_eq!(
            c.get_pod(&v1).unwrap().spec.unwrap().priority,
            Some(1),
            "GetPod returns the updated pod"
        );
    }

    #[test]
    fn assumed_pod_cannot_be_updated() {
        let c = SchedulerCache::new(Duration::from_secs(10));
        let v0 = pod("test", "node", 0);
        c.assume_pod(&v0).unwrap();
        assert!(c.update_pod(&v0, &pod("test", "node", 1)).is_err());
    }

    #[test]
    fn expire_add_update_pod() {
        let ttl = Duration::from_secs(10);
        let now = Instant::now();
        let c = SchedulerCache::new(ttl);
        let (v0, v1) = (pod("test", "node", 0), pod("test", "node", 1));
        assume_and_finish(&c, &v0, now);
        c.cleanup_assumed_pods(now + 2 * ttl);
        c.add_pod(&v0).unwrap();
        c.update_pod(&v0, &v1).unwrap();
        assert_eq!(names(&c, "node"), vec![("test".into(), 1)]);
        c.update_pod(&v1, &v0).unwrap();
        assert_eq!(names(&c, "node"), vec![("test".into(), 0)]);
    }

    #[test]
    fn remove_pod() {
        for assume in [false, true] {
            let c = SchedulerCache::new(Duration::from_secs(1));
            let p = pod("test", "node-1", 0);
            if assume {
                c.assume_pod(&p).unwrap();
            } else {
                c.add_pod(&p).unwrap();
            }
            assert_eq!(c.pods_on_node("node-1").len(), 1);
            c.remove_pod(&p).unwrap();
            assert!(c.get_pod(&p).is_err(), "pod was not deleted");
            // trying to remove a pod already removed should return an error
            assert!(c.remove_pod(&p).is_err());
        }
    }

    #[test]
    fn remove_pod_with_empty_node_name() {
        // "An empty NodeName is possible when the scheduler misses a Delete
        // event and it gets the last known state from the informer cache."
        let c = SchedulerCache::default();
        c.add_pod(&pod("test", "node", 0)).unwrap();
        c.remove_pod(&pod("test", "", 0)).unwrap();
        assert_eq!(c.pod_count(), 0);
    }

    #[test]
    fn forget_pod() {
        let c = SchedulerCache::new(Duration::from_secs(10));
        let p = pod("test", "node", 0);
        assume_and_finish(&c, &p, Instant::now());
        assert!(
            c.is_assumed_pod(&p).unwrap(),
            "Pod is expected to be assumed."
        );
        let got = c.get_pod(&p).unwrap();
        assert_eq!(got.metadata.name, p.metadata.name);
        assert_eq!(got.metadata.namespace, p.metadata.namespace);
        c.forget_pod(&p).unwrap();
        assert_forgotten(&c, &p);
        // trying to forget a pod already forgotten should return nil
        c.forget_pod(&p).unwrap();
    }

    #[test]
    fn forget_pod_rules() {
        let c = SchedulerCache::default();
        // only an assumed pod can be forgotten
        let added = pod("added", "node", 0);
        c.add_pod(&added).unwrap();
        assert!(c.forget_pod(&added).is_err());
        // assumed on one node, forgotten on another
        let a = pod("a", "node", 0);
        c.assume_pod(&a).unwrap();
        assert!(c.forget_pod(&pod("a", "elsewhere", 0)).is_err());
        assert!(c.is_assumed_pod(&a).unwrap());
    }

    #[test]
    fn assumed_pods_lists_only_assumed_ones() {
        let c = SchedulerCache::default();
        c.add_pod(&pod("added", "node", 0)).unwrap();
        c.assume_pod(&pod("a", "node", 0)).unwrap();
        let got: Vec<String> = c
            .assumed_pods()
            .into_iter()
            .map(|p| p.metadata.name)
            .collect();
        assert_eq!(got, vec!["a"]);
    }

    #[tokio::test]
    async fn in_flight_bindings_wait_idle() {
        use rusternetes_scheduler::async_binding::InFlightBindings;
        let b = std::sync::Arc::new(InFlightBindings::default());
        b.wait_idle().await;
        let g = b.start();
        assert_eq!(b.running(), 1);
        let b2 = b.clone();
        let waiter = tokio::spawn(async move { b2.wait_idle().await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        drop(g);
        waiter.await.unwrap();
        assert_eq!(b.running(), 0);
    }

    #[test]
    fn finish_binding_of_a_forgotten_pod_is_a_no_op() {
        let c = SchedulerCache::new(Duration::from_secs(10));
        let p = pod("test", "node", 0);
        c.assume_pod(&p).unwrap();
        c.forget_pod(&p).unwrap();
        c.finish_binding(&p).unwrap();
        assert_forgotten(&c, &p);
    }
}
