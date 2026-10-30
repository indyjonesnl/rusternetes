//! Unit tests moved from `src/node_status.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::{ContainerImage, Node, NodeCondition, NodeStatus};
    pub use rusternetes_kubelet::image_gc::Image;
    pub use serde_json::Value;
    pub use std::time::{Duration, Instant};
}

mod tests {
    // Ports pkg/kubelet/nodestatus/setters_test.go TestImages.
    fn make_image_list(n_images: usize, n_names: usize, min: i64, max: i64) -> Vec<Image> {
        (0..n_images)
            .map(|i| Image {
                id: format!("img-{i}"),
                size: min + (max - min) * i as i64 / n_images.max(1) as i64,
                repo_tags: (0..n_names).map(|j| format!("tag-{i}-{j}")).collect(),
                ..Default::default()
            })
            .collect()
    }

    fn sorted(mut l: Vec<Image>) -> Vec<Image> {
        l.sort_by(|a, b| b.size.cmp(&a.size).then_with(|| b.id.cmp(&a.id)));
        l
    }

    fn node_with_status() -> Node {
        let mut n = Node::new("n");
        n.status = Some(NodeStatus::default());
        n
    }

    fn expected(list: &[Image], max: i32) -> Vec<ContainerImage> {
        let n = if max > -1 {
            (max as usize).min(list.len())
        } else {
            list.len()
        };
        list[..n]
            .iter()
            .map(|i| {
                let mut names = i.repo_digests.clone();
                names.extend(i.repo_tags.clone());
                names.truncate(MAX_NAMES_PER_IMAGE_IN_NODE_STATUS);
                ContainerImage {
                    names: if names.is_empty() { None } else { Some(names) },
                    size_bytes: Some(i.size),
                }
            })
            .collect()
    }

    #[test]
    fn images_max_images_enforced() {
        let list = sorted(make_image_list(2, 1, 23 << 20, 1000 << 20));
        let mut node = node_with_status();
        set_images(&mut node, 1, Ok(list.clone())).unwrap();
        assert_eq!(
            serde_json::to_value(node.status.unwrap().images.unwrap()).unwrap(),
            serde_json::to_value(expected(&list, 1)).unwrap()
        );
    }

    #[test]
    fn images_no_cap_for_minus_one() {
        let list = sorted(make_image_list(2, 1, 23 << 20, 1000 << 20));
        let mut node = node_with_status();
        set_images(&mut node, -1, Ok(list.clone())).unwrap();
        assert_eq!(
            serde_json::to_value(node.status.unwrap().images.unwrap()).unwrap(),
            serde_json::to_value(expected(&list, -1)).unwrap()
        );
    }

    #[test]
    fn images_max_names_per_image_enforced() {
        let list = make_image_list(
            1,
            MAX_NAMES_PER_IMAGE_IN_NODE_STATUS + 1,
            23 << 20,
            1000 << 20,
        );
        let mut node = node_with_status();
        set_images(&mut node, -1, Ok(list)).unwrap();
        let images = node.status.unwrap().images.unwrap();
        assert_eq!(
            images[0].names.as_ref().unwrap().len(),
            MAX_NAMES_PER_IMAGE_IN_NODE_STATUS
        );
    }

    #[test]
    fn images_sorted_by_size_descending() {
        // The setter trusts the list order (the image cache sorts on write).
        let list = sorted(vec![
            Image {
                size: 3,
                ..Default::default()
            },
            Image {
                size: 1,
                ..Default::default()
            },
            Image {
                size: 4,
                ..Default::default()
            },
            Image {
                size: 2,
                ..Default::default()
            },
        ]);
        let mut node = node_with_status();
        set_images(&mut node, -1, Ok(list)).unwrap();
        let sizes: Vec<_> = node
            .status
            .unwrap()
            .images
            .unwrap()
            .iter()
            .map(|i| i.size_bytes.unwrap())
            .collect();
        assert_eq!(sizes, vec![4, 3, 2, 1]);
    }

    #[test]
    fn images_repo_digests_and_tags_both_show_up() {
        let list = vec![Image {
            repo_digests: vec!["foo".into(), "bar".into()],
            repo_tags: vec!["baz".into(), "quux".into()],
            ..Default::default()
        }];
        let mut node = node_with_status();
        set_images(&mut node, -1, Ok(list)).unwrap();
        assert_eq!(
            node.status.unwrap().images.unwrap()[0]
                .names
                .as_ref()
                .unwrap(),
            &vec!["foo", "bar", "baz", "quux"]
        );
    }

    #[test]
    fn images_error_resets_image_list() {
        let mut node = node_with_status();
        set_images(&mut node, -1, Ok(make_image_list(1, 1, 1, 2))).unwrap();
        let err = set_images(&mut node, -1, Err(anyhow::anyhow!("foo"))).unwrap_err();
        assert_eq!(err, "error getting image list: foo");
        assert!(node.status.unwrap().images.is_none());
    }

    use super::__parent::*;
    use chrono::{DateTime, Duration as CDur, TimeZone, Utc};
    use rusternetes_kubelet::node_status::*;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2015, 1, 1, 12, 0, 0).unwrap()
    }
    fn cond(t: &str, s: &str, hb: DateTime<Utc>, tr: DateTime<Utc>) -> NodeCondition {
        NodeCondition {
            condition_type: t.into(),
            status: s.into(),
            last_heartbeat_time: Some(hb),
            last_transition_time: Some(tr),
            reason: None,
            message: None,
        }
    }
    fn st(c: Vec<NodeCondition>) -> NodeStatus {
        NodeStatus {
            conditions: Some(c),
            ..Default::default()
        }
    }

    /// Port of TestNodeStatusHasChanged
    /// (pkg/kubelet/kubelet_node_status_test.go:2785-2935).
    #[test]
    fn node_status_has_changed_cases() {
        let n = now();
        let f = n + CDur::minutes(1);
        let ready = cond("Ready", "True", n, n);
        let ready_hb = cond("Ready", "True", f, n);
        let ready_tr = cond("Ready", "True", f, f);
        let not_ready = cond("Ready", "False", n, n);
        let mem = cond("MemoryPressure", "False", n, n);
        let d = NodeStatus::default();

        assert!(!node_status_has_changed(None, None));
        assert!(!node_status_has_changed(Some(&d), Some(&d)));
        assert!(node_status_has_changed(None, Some(&d)));
        assert!(node_status_has_changed(
            None,
            Some(&st(vec![ready.clone(), mem.clone()]))
        ));
        assert!(!node_status_has_changed(
            Some(&st(vec![])),
            Some(&st(vec![]))
        ));
        let base = st(vec![ready.clone(), mem.clone()]);
        assert!(!node_status_has_changed(
            Some(&base),
            Some(&st(vec![ready.clone(), mem.clone()]))
        ));
        // heartbeat-only change
        assert!(!node_status_has_changed(
            Some(&base),
            Some(&st(vec![ready_hb.clone(), mem.clone()]))
        ));
        // order-only change
        assert!(!node_status_has_changed(
            Some(&base),
            Some(&st(vec![mem.clone(), ready_hb.clone()]))
        ));
        assert!(node_status_has_changed(
            Some(&base),
            Some(&st(vec![not_ready, mem.clone()]))
        ));
        assert!(node_status_has_changed(
            Some(&base),
            Some(&st(vec![ready_tr, mem.clone()]))
        ));
        assert!(node_status_has_changed(
            Some(&st(vec![ready.clone()])),
            Some(&st(vec![ready.clone(), mem]))
        ));
        let mut p1 = st(vec![ready.clone()]);
        let mut p2 = p1.clone();
        // (no `phase` in our NodeStatus; any non-condition field stands in)
        p1.volumes_in_use = Some(vec!["a".into()]);
        p2.volumes_in_use = Some(vec!["b".into()]);
        assert!(node_status_has_changed(Some(&p1), Some(&p2)));
    }

    #[test]
    fn non_condition_field_change_is_a_change_but_empty_equals_none() {
        let a = st(vec![]);
        let mut b = a.clone();
        b.addresses = Some(vec![]);
        assert!(!node_status_has_changed(Some(&a), Some(&b)));
        b.volumes_in_use = Some(vec!["v".into()]);
        assert!(node_status_has_changed(Some(&a), Some(&b)));
    }

    /// Port of TestIsUpdateStatusPeriodExpired
    /// (kubelet_node_status_test.go:3094-3150).
    #[test]
    fn is_update_status_period_expired_cases() {
        let freq = Duration::from_secs(300);
        let t = Instant::now();
        let ago = |s: u64| t.checked_sub(Duration::from_secs(s));
        assert!(is_update_status_period_expired(None, t, freq, 0.0));
        assert!(is_update_status_period_expired(None, t, freq, 30.0));
        assert!(!is_update_status_period_expired(ago(240), t, freq, 0.0));
        assert!(!is_update_status_period_expired(ago(300), t, freq, 60.0));
        assert!(is_update_status_period_expired(ago(240), t, freq, -120.0));
        assert!(is_update_status_period_expired(ago(300), t, freq, 0.0));
    }

    /// Port of TestCalculateDelay (kubelet_node_status_test.go:3152-3162).
    #[test]
    fn calculate_delay_is_within_half_the_report_frequency() {
        let f = Duration::from_secs(300);
        for _ in 0..100 {
            let d = calculate_delay_secs(f, rand_unit());
            assert!(d.abs() <= f.as_secs_f64() / 2.0);
        }
        assert_eq!(calculate_delay_secs(f, 0.0), -150.0);
    }

    /// `wait.Jitter(d, 0.04)` stays within [d, 1.04 d).
    #[test]
    fn jitter_is_within_four_percent() {
        let d = Duration::from_secs(10);
        for _ in 0..100 {
            let j = jitter(d, NODE_STATUS_LOOP_JITTER_FACTOR, rand_unit());
            assert!(j >= d && j < d.mul_f64(1.04));
        }
    }

    // ---- prepare_patch_for_node_status ----------------------------------
    // Expected patches are what strategicpatch.CreateTwoWayMergePatch emits
    // for v1.Node (patch.go diffMaps/diffLists; $setElementOrder on every
    // changed merge list).

    use rusternetes_common::resources::{NodeAddress, NodeSpec};
    use serde_json::json;

    fn node(c: Vec<NodeCondition>) -> Node {
        let mut n = Node::new("n1");
        n.metadata.uid = "uid-1".into(); // Node::new mints a random uid
        n.status = Some(st(c));
        n
    }
    fn addr(t: &str, a: &str) -> NodeAddress {
        NodeAddress {
            address_type: t.into(),
            address: a.into(),
        }
    }

    #[test]
    fn identical_nodes_yield_an_empty_patch() {
        let n = node(vec![cond("Ready", "True", now(), now())]);
        assert_eq!(prepare_patch_for_node_status(&n, &n).unwrap(), json!({}));
    }

    #[test]
    fn heartbeat_only_patches_the_one_condition_with_its_merge_key() {
        let old = node(vec![
            cond("Ready", "True", now(), now()),
            cond("MemoryPressure", "False", now(), now()),
        ]);
        let later = now() + CDur::seconds(10);
        let new = node(vec![
            cond("Ready", "True", later, now()),
            cond("MemoryPressure", "False", now(), now()),
        ]);
        let p = prepare_patch_for_node_status(&old, &new).unwrap();
        assert_eq!(
            p,
            json!({"status": {
                "conditions": [{"type": "Ready", "lastHeartbeatTime": later}],
                "$setElementOrder/conditions": [{"type": "Ready"}, {"type": "MemoryPressure"}],
            }})
        );
    }

    #[test]
    fn spec_changes_are_never_patched() {
        let mut old = node(vec![]);
        let spec = NodeSpec {
            pod_cidr: None,
            pod_cidrs: None,
            provider_id: None,
            unschedulable: None,
            taints: None,
        };
        old.spec = Some(spec);
        let mut new = old.clone();
        new.spec = Some(NodeSpec {
            pod_cidr: Some("10.0.0.0/24".into()),
            pod_cidrs: None,
            provider_id: None,
            unschedulable: None,
            taints: None,
        });
        assert_eq!(
            prepare_patch_for_node_status(&old, &new).unwrap(),
            json!({})
        );
    }

    #[test]
    fn removed_condition_becomes_a_delete_directive() {
        let old = node(vec![
            cond("Ready", "True", now(), now()),
            cond("MemoryPressure", "False", now(), now()),
        ]);
        let new = node(vec![cond("Ready", "True", now(), now())]);
        let p = prepare_patch_for_node_status(&old, &new).unwrap();
        assert_eq!(
            p,
            json!({"status": {
                "conditions": [{"type": "MemoryPressure", "$patch": "delete"}],
                "$setElementOrder/conditions": [{"type": "Ready"}],
            }})
        );
    }

    #[test]
    fn label_added_and_removed() {
        let mut old = node(vec![]);
        old.metadata.labels = Some([("a".to_string(), "1".to_string())].into());
        let mut new = node(vec![]);
        new.metadata.labels = Some([("b".to_string(), "2".to_string())].into());
        let p = prepare_patch_for_node_status(&old, &new).unwrap();
        assert_eq!(p, json!({"metadata": {"labels": {"a": null, "b": "2"}}}));
    }

    /// status.addresses is mis-annotated `merge`; when it changed the patch
    /// carries an explicit replace list (fixupPatchForNodeStatusAddresses,
    /// status.go:84-130) so a removed address really goes away.
    #[test]
    fn changed_addresses_are_sent_as_a_replace_list() {
        let mut old = node(vec![]);
        old.status.as_mut().unwrap().addresses =
            Some(vec![addr("InternalIP", "10.0.0.1"), addr("Hostname", "n1")]);
        let mut new = old.clone();
        new.status.as_mut().unwrap().addresses = Some(vec![addr("InternalIP", "10.0.0.2")]);
        let p = prepare_patch_for_node_status(&old, &new).unwrap();
        assert_eq!(
            p,
            json!({"status": {"addresses": [
                {"type": "InternalIP", "address": "10.0.0.2"},
                {"$patch": "replace"},
            ]}})
        );
    }

    #[test]
    fn unchanged_addresses_are_absent_and_first_addresses_are_a_plain_add() {
        let mut old = node(vec![]);
        old.status.as_mut().unwrap().addresses = Some(vec![addr("Hostname", "n1")]);
        assert_eq!(
            prepare_patch_for_node_status(&old, &old).unwrap(),
            json!({})
        );

        let empty = node(vec![]);
        let mut new = empty.clone();
        new.status.as_mut().unwrap().addresses = Some(vec![addr("Hostname", "n1")]);
        assert_eq!(
            prepare_patch_for_node_status(&empty, &new).unwrap(),
            json!({"status": {"addresses": [{"type": "Hostname", "address": "n1"}]}})
        );
    }

    /// Non-merge lists (volumesInUse) are replaced whole.
    #[test]
    fn non_merge_lists_are_replaced_whole() {
        let mut old = node(vec![]);
        old.status.as_mut().unwrap().volumes_in_use = Some(vec!["a".into(), "b".into()]);
        let mut new = old.clone();
        new.status.as_mut().unwrap().volumes_in_use = Some(vec!["a".into()]);
        assert_eq!(
            prepare_patch_for_node_status(&old, &new).unwrap(),
            json!({"status": {"volumesInUse": ["a"]}})
        );
    }
}
