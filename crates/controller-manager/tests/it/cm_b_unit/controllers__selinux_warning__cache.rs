//! Unit tests moved from `src/controllers/selinux_warning/cache.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_controller_manager::controllers::selinux_warning::translator::{
        parse_selinux_label, ControllerSELinuxTranslator,
    };
    pub use std::collections::{BTreeSet, HashMap, HashSet};
    pub use std::fmt;
    pub use std::sync::RwLock;
    pub use tracing::{enabled, info, Level};
}

mod tests {
    //! Port of `cache/volumecache_test.go`: `TestVolumeCache_DeleteAll`,
    //! `TestVolumeCache_AddVolumeGetConflicts`,
    //! `TestVolumeCache_MultiVolumeConflicts`,
    //! `TestVolumeCache_DeletePodConflicts`,
    //! `TestVolumeCache_GetPodsForCSIDriver`.
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::selinux_warning::cache::*;

    const MOUNT: &str = SELINUX_CHANGE_POLICY_MOUNT_OPTION;
    const RECURSIVE: &str = SELINUX_CHANGE_POLICY_RECURSIVE;

    fn new_cache() -> VolumeLabelCache {
        VolumeLabelCache::new(ControllerSELinuxTranslator)
    }

    fn sort_conflicts(c: &mut [Conflict]) {
        c.sort_by(|a, b| {
            (a.pod.to_string(), a.other_pod.to_string())
                .cmp(&(b.pod.to_string(), b.other_pod.to_string()))
        });
    }

    /// `verifyReverseIndexConsistency`: the forward and reverse indexes are
    /// symmetric.
    fn verify_reverse_index_consistency(c: &VolumeLabelCache) {
        let inner = c.inner.read().unwrap();
        for (pod_key, volumes) in &inner.pod_to_volumes {
            for volume_name in volumes {
                let volume = inner.volumes.get(volume_name).unwrap_or_else(|| {
                    panic!(
                        "Reverse index has pod {pod_key} -> volume {volume_name}, but volume not in forward index"
                    )
                });
                assert!(
                    volume.pods.contains_key(pod_key),
                    "Reverse index has pod {pod_key} -> volume {volume_name}, but pod not in volume's pod list"
                );
            }
        }
        for (volume_name, volume) in &inner.volumes {
            for pod_key in volume.pods.keys() {
                let pod_volumes = inner.pod_to_volumes.get(pod_key).unwrap_or_else(|| {
                    panic!(
                        "Forward index has volume {volume_name} -> pod {pod_key}, but pod not in reverse index"
                    )
                });
                assert!(
                    pod_volumes.contains(volume_name),
                    "Forward index has volume {volume_name} -> pod {pod_key}, but volume not in pod's volume list"
                );
            }
        }
    }

    /// Delete all items in a bigger cache and check it's empty.
    #[test]
    fn delete_all() {
        let c = new_cache();
        let mut pods_to_delete = Vec::new();
        for ns in ["ns1", "ns2", "ns3", "ns4"] {
            for name in ["pod1", "pod2", "pod3", "pod4"] {
                for vol in ["vol1", "vol2", "vol3", "vol4"] {
                    let key = ObjectName::new(ns, name);
                    pods_to_delete.push(key.clone());
                    let conflicts = c.add_volume(vol, &key, "label1", MOUNT, "csiDriver1");
                    // All volumes have the same labels and policy, there
                    // should be no conflicts.
                    assert!(conflicts.is_empty(), "{ns}/{name} {vol}: {conflicts:?}");
                }
            }
        }
        verify_reverse_index_consistency(&c);
        for key in &pods_to_delete {
            c.delete_pod(key);
        }
        let inner = c.inner.read().unwrap();
        assert!(inner.volumes.is_empty(), "cache must be empty");
        assert!(
            inner.pod_to_volumes.is_empty(),
            "reverse index must be empty"
        );
        drop(inner);
        verify_reverse_index_consistency(&c);
    }

    #[derive(Clone)]
    struct PodWithVolume {
        ns: &'static str,
        name: &'static str,
        volume: &'static str,
        label: &'static str,
        policy: &'static str,
    }

    fn pwv(
        ns: &'static str,
        name: &'static str,
        volume: &'static str,
        label: &'static str,
        policy: &'static str,
    ) -> PodWithVolume {
        PodWithVolume {
            ns,
            name,
            volume,
            label,
            policy,
        }
    }

    fn add_reverse_conflict(conflicts: Vec<Conflict>) -> Vec<Conflict> {
        let mut out = Vec::with_capacity(conflicts.len() * 2);
        for c in conflicts {
            let reversed = Conflict {
                property_name: c.property_name.clone(),
                event_reason: c.event_reason.clone(),
                pod: c.other_pod.clone(),
                property_value: c.other_property_value.clone(),
                other_pod: c.pod.clone(),
                other_property_value: c.property_value.clone(),
            };
            out.push(c);
            out.push(reversed);
        }
        out
    }

    fn label_conflict(
        pod: (&str, &str),
        value: &str,
        other: (&str, &str),
        other_value: &str,
    ) -> Conflict {
        Conflict {
            property_name: "SELinuxLabel".into(),
            event_reason: "SELinuxLabelConflict".into(),
            pod: ObjectName::new(pod.0, pod.1),
            property_value: value.into(),
            other_pod: ObjectName::new(other.0, other.1),
            other_property_value: other_value.into(),
        }
    }

    fn policy_conflict(
        pod: (&str, &str),
        value: &str,
        other: (&str, &str),
        other_value: &str,
    ) -> Conflict {
        Conflict {
            property_name: "SELinuxChangePolicy".into(),
            event_reason: "SELinuxChangePolicyConflict".into(),
            pod: ObjectName::new(pod.0, pod.1),
            property_value: value.into(),
            other_pod: ObjectName::new(other.0, other.1),
            other_property_value: other_value.into(),
        }
    }

    /// AddVolume and GetConflicts return the same conflict data.
    #[test]
    fn add_volume_get_conflicts() {
        let existing = vec![
            pwv(
                "ns1",
                "pod1-mountOption",
                "vol1",
                "system_u:system_r:label1",
                MOUNT,
            ),
            // Labels on volumes with Recursive policy are cleared, we don't
            // want the controller to report label conflicts on them.
            pwv("ns2", "pod2-recursive", "vol2", "", RECURSIVE),
            // vol3 is used by 2 pods with the same label + recursive policy.
            pwv("ns3", "pod3-1", "vol3", "", RECURSIVE),
            pwv("ns3", "pod3-2", "vol3", "", RECURSIVE),
            // vol4 is used by 2 pods with the same label + mount policy.
            pwv("ns4", "pod4-1", "vol4", "system_u:system_r:label4", MOUNT),
            pwv("ns4", "pod4-2", "vol4", "system_u:system_r:label4", MOUNT),
            // vol5 has no user and role.
            pwv("ns5", "pod5", "vol5", "::label5", MOUNT),
            // vol6 has no user.
            pwv("ns6", "pod6", "vol6", ":system_r:label6", MOUNT),
            // vol7 has no user and role, but has categories.
            pwv("ns7", "pod7", "vol7", "::label7:c0,c1", MOUNT),
            // vol8 has no label.
            pwv("ns8", "pod8", "vol8", "", MOUNT),
        ];

        struct Case {
            name: &'static str,
            initial: Vec<PodWithVolume>,
            to_add: PodWithVolume,
            expected: Vec<Conflict>,
        }
        let t = ("testns", "testpod");
        let cases = vec![
            Case {
                name: "new volume in empty cache",
                initial: vec![],
                to_add: pwv("testns", "testpod", "vol-new", "system_u:system_r:label-new", MOUNT),
                expected: vec![],
            },
            Case {
                name: "new volume",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol-new", "system_u:system_r:label-new", MOUNT),
                expected: vec![],
            },
            Case {
                name: "existing volume in a new pod with existing policy and label",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol1", "system_u:system_r:label1", MOUNT),
                expected: vec![],
            },
            Case {
                name: "existing volume in a new pod with existing policy and new conflicting label",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol1", "system_u:system_r:label-new", MOUNT),
                expected: vec![label_conflict(
                    t,
                    "system_u:system_r:label-new",
                    ("ns1", "pod1-mountOption"),
                    "system_u:system_r:label1",
                )],
            },
            Case {
                name: "existing volume in a new pod with new conflicting policy",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol1", "", RECURSIVE),
                expected: vec![policy_conflict(
                    t,
                    "Recursive",
                    ("ns1", "pod1-mountOption"),
                    "MountOption",
                )],
            },
            Case {
                name: "existing pod is replaced with different non-conflicting policy and label",
                initial: existing.clone(),
                // There is no other pod that uses vol2 -> change of policy
                // and label is possible.
                to_add: pwv("ns2", "pod2-recursive", "vol2", "system_u:system_r:label-new", MOUNT),
                expected: vec![],
            },
            Case {
                name: "existing pod is replaced with conflicting policy",
                initial: existing.clone(),
                // vol3 is used by pod3-2 with Recursive policy. A policy
                // change can happen when a CSIDriver is updated from
                // SELinuxMount: false to SELinuxMount: true.
                to_add: pwv("ns3", "pod3-1", "vol3", "system_u:system_r:label-new", MOUNT),
                expected: vec![policy_conflict(
                    ("ns3", "pod3-1"),
                    "MountOption",
                    ("ns3", "pod3-2"),
                    "Recursive",
                )],
            },
            Case {
                name: "existing volume in a new pod with existing policy and new incomparable label (missing user and role)",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol5", "system_u:system_r:label5", MOUNT),
                expected: vec![],
            },
            Case {
                name: "existing volume in a new pod with conflicting policy with incomparable parts",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol5", "::label6", MOUNT),
                expected: vec![label_conflict(t, "::label6", ("ns5", "pod5"), "::label5")],
            },
            Case {
                name: "existing volume in a new pod with existing policy and new incomparable label (missing user)",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol6", "system_u::label6", MOUNT),
                expected: vec![],
            },
            Case {
                name: "existing volume in a new pod with existing policy and new comparable label (missing categories)",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol7", "system_u:system_r:label7", MOUNT),
                expected: vec![label_conflict(
                    t,
                    "system_u:system_r:label7",
                    ("ns7", "pod7"),
                    "::label7:c0,c1",
                )],
            },
            Case {
                name: "existing volume in a new pod with existing policy and new incomparable label (missing everything)",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol8", "system_u:system_r:label8", MOUNT),
                expected: vec![],
            },
            Case {
                name: "existing volume in a new pod with existing policy and new comparable label (missing everything but categories)",
                initial: existing.clone(),
                to_add: pwv("testns", "testpod", "vol8", "system_u:system_r:label8:c0,c1", MOUNT),
                expected: vec![label_conflict(
                    t,
                    "system_u:system_r:label8:c0,c1",
                    ("ns8", "pod8"),
                    "",
                )],
            },
        ];

        for tt in cases {
            let c = new_cache();
            for p in &tt.initial {
                let conflicts = c.add_volume(
                    p.volume,
                    &ObjectName::new(p.ns, p.name),
                    p.label,
                    p.policy,
                    "csiDriver1",
                );
                assert!(
                    conflicts.is_empty(),
                    "{}: initial AddVolume {}/{} {} returned conflicts: {conflicts:?}",
                    tt.name,
                    p.ns,
                    p.name,
                    p.volume
                );
            }

            let p = &tt.to_add;
            let pod_key = ObjectName::new(p.ns, p.name);
            let mut conflicts = c.add_volume(p.volume, &pod_key, p.label, p.policy, "csiDriver1");

            let mut expected = add_reverse_conflict(tt.expected.clone());
            sort_conflicts(&mut conflicts);
            sort_conflicts(&mut expected);
            assert_eq!(conflicts, expected, "{}: AddVolume conflicts", tt.name);

            // Expect the pod + volume to be present in the cache.
            {
                let inner = c.inner.read().unwrap();
                let volume = inner
                    .volumes
                    .get(p.volume)
                    .unwrap_or_else(|| panic!("{}: volume missing", tt.name));
                let existing_info = volume
                    .pods
                    .get(&pod_key)
                    .unwrap_or_else(|| panic!("{}: pod missing", tt.name));
                assert_eq!(
                    existing_info,
                    &PodInfo::new(p.label, p.policy),
                    "{}: pod info",
                    tt.name
                );
            }

            verify_reverse_index_consistency(&c);

            // GetConflicts returns the same conflicts.
            let mut received = c.get_conflicts();
            sort_conflicts(&mut received);
            assert_eq!(received, expected, "{}: GetConflicts", tt.name);
        }
    }

    /// Conflicts are tracked per-volume: a pod with conflicts on multiple
    /// volumes retains all of them after successive AddVolume calls.
    #[test]
    fn multi_volume_conflicts() {
        let c = new_cache();
        let pod_a = ObjectName::new("ns", "podA");
        let pod_b = ObjectName::new("ns", "podB");
        let pod_c = ObjectName::new("ns", "podC");

        c.add_volume("vol1", &pod_b, "system_u:system_r:labelB", MOUNT, "driver1");
        c.add_volume("vol2", &pod_c, "system_u:system_r:labelC", MOUNT, "driver1");

        // podA uses vol1 with a different label (conflict with podB).
        let conflicts1 = c.add_volume("vol1", &pod_a, "system_u:system_r:labelA", MOUNT, "driver1");
        assert!(!conflicts1.is_empty(), "expected conflicts on vol1");
        // podA also uses vol2 with a different label (conflict with podC).
        let conflicts2 = c.add_volume("vol2", &pod_a, "system_u:system_r:labelA", MOUNT, "driver1");
        assert!(!conflicts2.is_empty(), "expected conflicts on vol2");

        // GetConflicts must return conflicts from BOTH volumes.
        let all = c.get_conflicts();
        assert_eq!(all.len(), conflicts1.len() + conflicts2.len());

        // After deleting podA, all conflicts should be gone.
        c.delete_pod(&pod_a);
        assert!(c.get_conflicts().is_empty());

        // Deduplication: podD and podE conflict on two volumes with the same
        // labels. Identical Conflict entries from different volumes must be
        // deduplicated by GetConflicts.
        let pod_d = ObjectName::new("ns", "podD");
        let pod_e = ObjectName::new("ns", "podE");
        c.add_volume("vol3", &pod_d, "system_u:system_r:labelD", MOUNT, "driver1");
        c.add_volume("vol4", &pod_d, "system_u:system_r:labelD", MOUNT, "driver1");
        let conflicts_vol3 =
            c.add_volume("vol3", &pod_e, "system_u:system_r:labelE", MOUNT, "driver1");
        let conflicts_vol4 =
            c.add_volume("vol4", &pod_e, "system_u:system_r:labelE", MOUNT, "driver1");
        assert_eq!(conflicts_vol3.len(), conflicts_vol4.len());
        assert!(!conflicts_vol3.is_empty());

        let de_count = c
            .get_conflicts()
            .iter()
            .filter(|x| {
                x.pod == pod_d || x.pod == pod_e || x.other_pod == pod_d || x.other_pod == pod_e
            })
            .count();
        assert_eq!(de_count, conflicts_vol3.len());
    }

    #[test]
    fn delete_pod_conflicts() {
        let a = ObjectName::new("ns", "podA");
        let b = ObjectName::new("ns", "podB");
        let cc = ObjectName::new("ns", "podC");
        let d = ObjectName::new("ns", "podD");

        struct Case {
            name: &'static str,
            initial: Vec<PodWithVolume>,
            to_delete: ObjectName,
            delete_twice: bool,
            surviving: Vec<(ObjectName, ObjectName)>,
        }
        let la = "system_u:system_r:labelA";
        let lb = "system_u:system_r:labelB";
        let lc = "system_u:system_r:labelC";
        let ld = "system_u:system_r:labelD";
        let cases = vec![
            Case {
                name: "delete one of two conflicting pods clears all conflicts",
                initial: vec![
                    pwv("ns", "podA", "vol1", la, MOUNT),
                    pwv("ns", "podB", "vol1", lb, MOUNT),
                ],
                to_delete: a.clone(),
                delete_twice: false,
                surviving: vec![],
            },
            Case {
                name: "delete non-conflicting pod preserves existing conflicts",
                initial: vec![
                    pwv("ns", "podA", "vol1", la, MOUNT),
                    pwv("ns", "podB", "vol1", lb, MOUNT),
                    pwv("ns", "podC", "vol2", lc, MOUNT),
                ],
                to_delete: cc.clone(),
                delete_twice: false,
                surviving: vec![(a.clone(), b.clone())],
            },
            Case {
                name: "three pods on same volume delete one leaves remaining pair conflict",
                initial: vec![
                    pwv("ns", "podA", "vol1", la, MOUNT),
                    pwv("ns", "podB", "vol1", lb, MOUNT),
                    pwv("ns", "podC", "vol1", lc, MOUNT),
                ],
                to_delete: a.clone(),
                delete_twice: false,
                surviving: vec![(b.clone(), cc.clone())],
            },
            Case {
                name: "delete pod with conflicts on multiple volumes",
                initial: vec![
                    pwv("ns", "podB", "vol1", lb, MOUNT),
                    pwv("ns", "podC", "vol2", lc, MOUNT),
                    pwv("ns", "podA", "vol1", la, MOUNT),
                    pwv("ns", "podA", "vol2", la, MOUNT),
                ],
                to_delete: a.clone(),
                delete_twice: false,
                surviving: vec![],
            },
            Case {
                name: "delete pod preserves conflicts on unrelated volumes",
                initial: vec![
                    pwv("ns", "podA", "vol1", la, MOUNT),
                    pwv("ns", "podB", "vol1", lb, MOUNT),
                    pwv("ns", "podC", "vol2", lc, MOUNT),
                    pwv("ns", "podD", "vol2", ld, MOUNT),
                ],
                to_delete: a.clone(),
                delete_twice: false,
                surviving: vec![(cc.clone(), d.clone())],
            },
            Case {
                name: "delete pod that was already deleted is a no-op",
                initial: vec![
                    pwv("ns", "podA", "vol1", la, MOUNT),
                    pwv("ns", "podB", "vol1", lb, MOUNT),
                ],
                to_delete: a.clone(),
                delete_twice: true,
                surviving: vec![],
            },
        ];

        for tt in cases {
            let c = new_cache();
            for p in &tt.initial {
                c.add_volume(
                    p.volume,
                    &ObjectName::new(p.ns, p.name),
                    p.label,
                    p.policy,
                    "driver1",
                );
            }
            c.delete_pod(&tt.to_delete);
            if tt.delete_twice {
                c.delete_pod(&tt.to_delete);
            }
            let remaining = c.get_conflicts();

            // Deleted pod must not appear in any conflict.
            for conflict in &remaining {
                assert!(
                    conflict.pod != tt.to_delete && conflict.other_pod != tt.to_delete,
                    "{}: conflict involving deleted pod: {conflict:?}",
                    tt.name
                );
            }
            // Each expected surviving pair exists in both directions.
            for (p0, p1) in &tt.surviving {
                let forward = remaining.iter().any(|x| &x.pod == p0 && &x.other_pod == p1);
                let reverse = remaining.iter().any(|x| &x.pod == p1 && &x.other_pod == p0);
                assert!(
                    forward && reverse,
                    "{}: expected symmetric conflict between {p0} and {p1}, got {remaining:?}",
                    tt.name
                );
            }
            if tt.surviving.is_empty() {
                assert!(remaining.is_empty(), "{}: {remaining:?}", tt.name);
            }
            verify_reverse_index_consistency(&c);
        }
    }

    #[test]
    fn get_pods_for_csi_driver() {
        let c = new_cache();
        let existing: Vec<(&str, Vec<PodWithVolume>)> = vec![
            (
                "csiDriver1",
                vec![pwv("ns1", "pod1-mountOption", "vol1", "label1", MOUNT)],
            ),
            (
                "csiDriver2",
                vec![
                    pwv("ns2", "pod2-recursive", "vol2", "label2", RECURSIVE),
                    // vol3 is used by 2 pods with the same label + recursive policy.
                    pwv("ns3", "pod3-1", "vol3", "label3", RECURSIVE),
                    pwv("ns3", "pod3-2", "vol3", "label3", RECURSIVE),
                ],
            ),
            (
                "csiDriver3",
                vec![
                    // vol4 is used by 2 pods with the same label + mount policy.
                    pwv("ns4", "pod4-1", "vol4", "label4", MOUNT),
                    pwv("ns4", "pod4-2", "vol4", "label4", MOUNT),
                ],
            ),
        ];
        for (driver, pods) in &existing {
            for p in pods {
                let conflicts = c.add_volume(
                    p.volume,
                    &ObjectName::new(p.ns, p.name),
                    p.label,
                    p.policy,
                    driver,
                );
                assert!(conflicts.is_empty(), "{conflicts:?}");
            }
        }

        let expected: Vec<(&str, Vec<ObjectName>)> = vec![
            (
                "csiDriver1",
                vec![ObjectName::new("ns1", "pod1-mountOption")],
            ),
            (
                "csiDriver2",
                vec![
                    ObjectName::new("ns2", "pod2-recursive"),
                    ObjectName::new("ns3", "pod3-1"),
                    ObjectName::new("ns3", "pod3-2"),
                ],
            ),
            (
                "csiDriver3",
                vec![
                    ObjectName::new("ns4", "pod4-1"),
                    ObjectName::new("ns4", "pod4-2"),
                ],
            ),
            // Totally unknown CSI driver.
            ("csiDriver4", vec![]),
        ];
        for (driver, want) in expected {
            let mut got = c.get_pods_for_csi_driver(driver);
            got.sort_by_key(|k| k.to_string());
            assert_eq!(got, want, "GetPodsForCSIDriver({driver})");
        }
    }

    /// `Conflict.EventMessage` (conflict.go), both namespace branches.
    #[test]
    fn event_message_matches_upstream_format() {
        let same_ns = label_conflict(("ns", "p1"), "a", ("ns", "p2"), "b");
        assert_eq!(
            same_ns.event_message(),
            "SELinuxLabel \"a\" conflicts with pod p2 that uses the same volume as this pod with SELinuxLabel \"b\". If both pods land on the same node, only one of them may access the volume."
        );
        let other_ns = label_conflict(("ns", "p1"), "a", ("ns2", "p2"), "b");
        assert_eq!(
            other_ns.event_message(),
            "SELinuxLabel\"a\" conflicts with another pod that uses the same volume as this pod with a different SELinuxLabel. If both pods land on the same node, only one of them may access the volume."
        );
    }
}
