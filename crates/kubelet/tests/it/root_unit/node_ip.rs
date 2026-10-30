//! Unit tests moved from `src/node_ip.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::net::{IpAddr, Ipv4Addr};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::node_ip::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    // hostname_test.go TestGetHostname
    #[test]
    fn get_hostname_cases() {
        let os = || Ok("OsHost".to_string());
        assert_eq!(get_hostname("overridden", &os).unwrap(), "overridden");
        assert_eq!(get_hostname("OVERRIDDEN", &os).unwrap(), "overridden");
        assert_eq!(get_hostname(" OVERRIDDEN ", &os).unwrap(), "overridden");
        assert_eq!(
            get_hostname(" ", &os).unwrap_err(),
            "empty hostname is invalid"
        );
        // empty parameter: os.Hostname(), lowercased.
        assert_eq!(get_hostname("", &os).unwrap(), "oshost");
        // os.Hostname failure.
        let bad = || Err(std::io::Error::other("boom"));
        assert!(get_hostname("", &bad)
            .unwrap_err()
            .starts_with("couldn't determine hostname: "));
        // os hostname that is only whitespace.
        let blank = || Ok("  ".to_string());
        assert_eq!(
            get_hostname("", &blank).unwrap_err(),
            "empty hostname is invalid"
        );
    }

    // server.go:1208-1211: nodeName derives from the hostname.
    #[test]
    fn resolve_node_identity_cases() {
        let os = || Ok("OsHost".to_string());
        // No override: nodeName == hostname == lowercased os hostname.
        assert_eq!(
            resolve_node_identity(None, &os).unwrap(),
            ("oshost".to_string(), "oshost".to_string())
        );
        // An empty override is "unset" (`len(hostnameOverride) > 0`, node.go).
        assert_eq!(
            resolve_node_identity(Some(""), &os).unwrap(),
            ("oshost".to_string(), "oshost".to_string())
        );
        // Override: nodeName follows it, lowercased and trimmed.
        assert_eq!(
            resolve_node_identity(Some(" Over "), &os).unwrap(),
            ("over".to_string(), "over".to_string())
        );
        // A blank override trims to empty: GetHostname's error.
        assert_eq!(
            resolve_node_identity(Some(" "), &os).unwrap_err(),
            "empty hostname is invalid"
        );
    }

    // pkg/util/node/node_test.go TestGetNodeHostIPs
    #[test]
    fn get_node_host_ips_cases() {
        let a = |t: &str, v: &str| rusternetes_common::resources::NodeAddress {
            address_type: t.to_string(),
            address: v.to_string(),
        };
        let ips = |l: &[&str]| l.iter().map(|s| ip(s)).collect::<Vec<_>>();
        assert!(get_node_host_ips(&[]).is_err());
        assert!(get_node_host_ips(&[a("Hostname", "example.com")]).is_err());
        let err = get_node_host_ips(&[a("Hostname", "example.com")]).unwrap_err();
        assert!(
            err.starts_with("host IP unknown; known addresses: "),
            "{err}"
        );
        // IPv4-only, simple
        assert_eq!(
            get_node_host_ips(&[
                a("InternalIP", "1.2.3.4"),
                a("ExternalIP", "4.3.2.1"),
                a("ExternalIP", "4.3.2.2")
            ])
            .unwrap(),
            ips(&["1.2.3.4"])
        );
        // external-first
        assert_eq!(
            get_node_host_ips(&[
                a("ExternalIP", "4.3.2.1"),
                a("ExternalIP", "4.3.2.2"),
                a("InternalIP", "1.2.3.4")
            ])
            .unwrap(),
            ips(&["1.2.3.4"])
        );
        // no internal
        assert_eq!(
            get_node_host_ips(&[a("ExternalIP", "4.3.2.1"), a("ExternalIP", "4.3.2.2")]).unwrap(),
            ips(&["4.3.2.1"])
        );
        // dual-stack node
        assert_eq!(
            get_node_host_ips(&[
                a("InternalIP", "1.2.3.4"),
                a("ExternalIP", "4.3.2.1"),
                a("ExternalIP", "4.3.2.2"),
                a("InternalIP", "a:b::c:d"),
                a("ExternalIP", "d:c::b:a")
            ])
            .unwrap(),
            ips(&["1.2.3.4", "a:b::c:d"])
        );
        // dual-stack node, IPv6-first, no internal IPv4
        assert_eq!(
            get_node_host_ips(&[
                a("InternalIP", "a:b::c:d"),
                a("ExternalIP", "d:c::b:a"),
                a("ExternalIP", "4.3.2.1"),
                a("ExternalIP", "4.3.2.2")
            ])
            .unwrap(),
            ips(&["a:b::c:d", "4.3.2.1"])
        );
    }

    // ips_test.go TestParseNodeIPArgument
    #[test]
    fn parse_argument_cases() {
        let p = |s: &str| parse_node_ip_argument(s, "");
        assert_eq!(p("").unwrap(), (vec![], vec![]));
        assert_eq!(p(" ").unwrap(), (vec![], vec!["".to_string()]));
        assert_eq!(p("blah").unwrap(), (vec![], vec!["blah".to_string()]));
        assert_eq!(p(" 1.2.3.4   ").unwrap().0, vec![ip("1.2.3.4")]);
        assert_eq!(p("01.2.3.004").unwrap().0, vec![ip("1.2.3.4")]);
        assert_eq!(p("1.2.3").unwrap().1, vec!["1.2.3".to_string()]);
        assert_eq!(p("1.2.3.0/24").unwrap().1, vec!["1.2.3.0/24".to_string()]);
        assert_eq!(p("0.0.0.0").unwrap().0, vec![ip("0.0.0.0")]);
        let r = p("1.2.3.4,not-an-IPv6-address").unwrap();
        assert_eq!(r.0, vec![ip("1.2.3.4")]);
        assert_eq!(r.1, vec!["not-an-IPv6-address".to_string()]);
        assert_eq!(
            p("abcd:0abc:00ab:0000:0000::1").unwrap().0,
            vec![ip("abcd:abc:ab::1")]
        );
        assert_eq!(
            p("1.2.3.4,abcd::ef01").unwrap().0,
            vec![ip("1.2.3.4"), ip("abcd::ef01")]
        );
        assert_eq!(
            p("abcd::ef01 , 1.2.3.4").unwrap().0,
            vec![ip("abcd::ef01"), ip("1.2.3.4")]
        );
        let r = p("abcd::ef01 , 1.2.3.4, something else").unwrap();
        assert_eq!(r.0, vec![ip("abcd::ef01"), ip("1.2.3.4")]);
        assert_eq!(r.1, vec!["something else".to_string()]);
        for bad in [
            "1.2.3.4,5.6.7.8",
            "abcd::1,abcd::2",
            "1.2.3.4,abcd::1,5.6.7.8",
        ] {
            assert!(
                p(bad)
                    .unwrap_err()
                    .contains("either a single IP or a dual-stack pair of IPs"),
                "{bad}"
            );
        }
        for bad in ["1.2.3.4,::", "0.0.0.0,abcd::1"] {
            assert!(
                p(bad)
                    .unwrap_err()
                    .contains("cannot include '0.0.0.0' or '::'"),
                "{bad}"
            );
        }
    }

    #[test]
    fn parse_argument_dual_stack_cloud_provider() {
        assert!(parse_node_ip_argument("1.2.3.4,abcd::ef01", "external").is_ok());
        let e = parse_node_ip_argument("1.2.3.4,abcd::ef01", "aws").unwrap_err();
        assert!(e.contains("not supported in this configuration"));
        assert!(parse_node_ip_argument("1.2.3.4", "aws").is_ok());
    }

    // kubelet_node_status.go validateNodeIP
    #[test]
    fn validate_rejects_special_addresses() {
        for bad in [
            "127.0.0.1",
            "::1",
            "224.0.0.1",
            "ff02::1",
            "169.254.1.1",
            "fe80::1",
            "0.0.0.0",
            "::",
        ] {
            assert!(validate_node_ip(ip(bad)).is_err(), "{bad}");
        }
        assert!(validate_node_ip(ip("10.1.1.1")).is_ok());
        assert!(validate_node_ip(ip("fd00::1")).is_ok());
    }

    // kubelet_node_status_test.go TestValidateNodeIPParam: an address that
    // is not on any host interface is rejected (cases "IPv4 address that
    // doesn't belong to host").
    #[test]
    fn host_validator_rejects_ip_not_on_host_interfaces() {
        let e = validate_node_ip_on_host(ip("1.2.3.4")).unwrap_err();
        assert!(
            e.contains("not found in the host's network interfaces"),
            "{e}"
        );
        assert!(validate_node_ip_on_host(ip("127.0.0.1")).is_err());
    }

    // Same test: every real interface address validates.
    #[test]
    fn host_validator_accepts_interface_addresses() {
        for a in host_interface_ips().unwrap() {
            if validate_node_ip(a).is_ok() {
                assert!(validate_node_ip_on_host(a).is_ok(), "{a}");
            }
        }
    }

    fn none(_: &str) -> Vec<IpAddr> {
        vec![]
    }

    // setters_test.go TestNodeAddress: single --node-ip
    #[test]
    fn address_single_node_ip() {
        let r = node_addresses(&[ip("fd00::5")], &validate_node_ip, "h", &none, &|_| {
            Err("no default route".to_string())
        })
        .unwrap();
        assert_eq!(
            r,
            vec![
                ("InternalIP".into(), "fd00::5".into()),
                ("Hostname".into(), "h".into())
            ]
        );
    }

    // setters_test.go: dual --node-ips => two InternalIPs then Hostname
    #[test]
    fn address_dual_stack() {
        let r = node_addresses(
            &[ip("10.0.0.2"), ip("fd00::2")],
            &validate_node_ip,
            "h",
            &none,
            &|_| Err("no default route".to_string()),
        )
        .unwrap();
        assert_eq!(
            r,
            vec![
                ("InternalIP".into(), "10.0.0.2".into()),
                ("InternalIP".into(), "fd00::2".into()),
                ("Hostname".into(), "h".into())
            ]
        );
    }

    // setters_test.go: invalid (loopback / multicast) node IP errors
    #[test]
    fn address_invalid_node_ip_errors() {
        assert!(
            node_addresses(&[ip("127.0.0.1")], &validate_node_ip, "h", &none, &|_| Err(
                "no default route".to_string()
            ))
            .is_err()
        );
        assert!(node_addresses(
            &[ip("10.0.0.2"), ip("ff02::1")],
            &validate_node_ip,
            "h",
            &none,
            &|_| Err("no default route".to_string())
        )
        .is_err());
    }

    // setters_test.go: no nodeIP prefers IPv4 from the DNS lookup
    #[test]
    fn address_no_node_ip_prefers_ipv4_lookup() {
        let lk = |_: &str| vec![ip("fd00::9"), ip("10.0.0.9")];
        let r = node_addresses(&[], &validate_node_ip, "h", &lk, &|_| {
            Err("no default route".to_string())
        })
        .unwrap();
        assert_eq!(r[0], ("InternalIP".into(), "10.0.0.9".into()));
    }

    // setters_test.go: nodeIP "::" prefers IPv6; resolve fallback when lookup empty
    #[test]
    fn address_unspecified_ipv6_prefers_ipv6_and_resolves() {
        let lk = |_: &str| vec![ip("10.0.0.9"), ip("fd00::9")];
        let r = node_addresses(&[ip("::")], &validate_node_ip, "h", &lk, &|_| {
            Err("no default route".to_string())
        })
        .unwrap();
        assert_eq!(r[0], ("InternalIP".into(), "fd00::9".into()));
        let r = node_addresses(&[ip("0.0.0.0")], &validate_node_ip, "h", &none, &|_| {
            Ok(ip("10.9.9.9"))
        })
        .unwrap();
        assert_eq!(r[0], ("InternalIP".into(), "10.9.9.9".into()));
    }

    // setters.go:160: a hostname that is itself an IP is used
    #[test]
    fn address_hostname_ip_literal_used() {
        let r = node_addresses(&[], &validate_node_ip, "10.2.2.2", &none, &|_| {
            Err("no default route".to_string())
        })
        .unwrap();
        assert_eq!(r[0], ("InternalIP".into(), "10.2.2.2".into()));
    }

    // setters.go:181: nothing found => error
    #[test]
    fn address_nothing_found_errors() {
        assert!(node_addresses(&[], &validate_node_ip, "h", &none, &|_| Err(
            "no default route".to_string()
        ))
        .is_err());
    }

    // setters.go:183 `can't get ip address of node %s. error: %v`
    #[test]
    fn address_nothing_found_error_text_carries_resolver_error() {
        let e = node_addresses(&[], &validate_node_ip, "h", &none, &|_| {
            Err("no default route".to_string())
        })
        .unwrap_err();
        assert_eq!(e, "can't get ip address of node h. error: no default route");
    }

    // setters.go:163-179 runs with NO --node-ip too: an IPv6-only host (DNS
    // yields only a valid v6 address) advertises it instead of 127.0.0.1 (#3186).
    #[test]
    fn addresses_without_node_ip_use_ipv6_only_lookup() {
        let lk = |_: &str| vec![ip("fd00::9")];
        let got = node_addresses(&[], &validate_node_ip, "h", &lk, &|_| {
            Err("no default route".to_string())
        })
        .unwrap();
        assert_eq!(got[0], ("InternalIP".to_string(), "fd00::9".to_string()));
    }

    #[test]
    fn addresses_without_node_ip_prefer_ipv4() {
        let lk = |_: &str| vec![ip("fd00::9"), ip("10.0.0.9")];
        let got = node_addresses(&[], &validate_node_ip, "h", &lk, &|_| {
            Err("no default route".to_string())
        })
        .unwrap();
        assert_eq!(got[0], ("InternalIP".to_string(), "10.0.0.9".to_string()));
    }

    // kubelet.go:461-464 + plugins.go:94-96
    #[test]
    fn cloud_provider_must_be_empty_or_external() {
        assert!(validate_cloud_provider("").is_ok());
        assert!(validate_cloud_provider("external").is_ok());
        assert_eq!(
            validate_cloud_provider("aws").unwrap_err(),
            "cloud provider \"aws\" was specified, but built-in cloud providers are disabled. Please set --cloud-provider=external and migrate to an external cloud provider"
        );
        assert!(is_external_cloud_provider("external"));
        assert!(!is_external_cloud_provider(""));
    }

    // server.go:1216-1222 + kubelet.go:461-464, 619
    #[test]
    fn node_ip_config_parses_and_gates_cloud_provider() {
        assert_eq!(node_ip_config("", "").unwrap(), (vec![], false));
        assert_eq!(
            node_ip_config("10.1.1.1,fd00::1", "external").unwrap(),
            (vec![ip("10.1.1.1"), ip("fd00::1")], true)
        );
        assert!(node_ip_config("10.1.1.1", "aws")
            .unwrap_err()
            .contains("built-in cloud providers are disabled"));
        // ParseNodeIPArgument rejects dual-stack for a legacy provider first.
        assert!(node_ip_config("10.1.1.1,fd00::1", "aws")
            .unwrap_err()
            .contains("dual-stack not supported in this configuration"));
        assert!(node_ip_config("10.1.1.1,10.1.1.2", "")
            .unwrap_err()
            .starts_with("bad --node-ip"));
    }

    // ---- setters_test.go TestNodeAddress (cloud-provider cases) ----

    type TestNode = rusternetes_common::resources::Node;

    fn setter_node(annotations: Option<&[(&str, &str)]>, existing: Option<&str>) -> TestNode {
        let mut n = TestNode::new("test-node");
        n.metadata.annotations = annotations.map(|a| {
            a.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        });
        let status = n.status.get_or_insert_with(Default::default);
        status.addresses = Some(match existing {
            Some(a) => vec![rusternetes_common::resources::NodeAddress {
                address_type: String::new(),
                address: a.to_string(),
            }],
            None => vec![],
        });
        n
    }

    fn addrs_of(n: &TestNode) -> Vec<(String, String)> {
        n.status
            .as_ref()
            .and_then(|s| s.addresses.clone())
            .unwrap_or_default()
            .into_iter()
            .map(|a| (a.address_type, a.address))
            .collect()
    }

    fn ann_of(n: &TestNode) -> std::collections::HashMap<String, String> {
        n.metadata.annotations.clone().unwrap_or_default()
    }

    /// Run the setter the way TestNodeAddress does: validator always ok, DNS
    /// lookup yielding nothing, `resolve` returning `resolved`.
    fn run_setter(
        node: &mut TestNode,
        ips: &[&str],
        cloud: bool,
        resolved: Option<&str>,
    ) -> Result<(), String> {
        let ips: Vec<IpAddr> = ips.iter().map(|s| ip(s)).collect();
        let r = resolved.map(ip);
        set_node_address(
            node,
            &ips,
            &|_| Ok(()),
            "test-hostname",
            cloud,
            &none,
            &|_| r.ok_or_else(|| "no resolved ip".to_string()),
        )
    }

    fn pair(t: &str, a: &str) -> (String, String) {
        (t.to_string(), a.to_string())
    }

    #[test]
    fn setter_cloud_provider_and_node_ip_specified() {
        let mut n = setter_node(None, None);
        run_setter(&mut n, &["10.0.0.1"], true, None).unwrap();
        assert_eq!(
            addrs_of(&n),
            vec![
                pair("InternalIP", "10.0.0.1"),
                pair("Hostname", "test-hostname")
            ]
        );
    }

    #[test]
    fn setter_unspecified_node_ip_with_cloud_provider_resolves() {
        for (nip, res) in [("0.0.0.0", "10.0.0.2"), ("::", "2001:db2::2")] {
            for cloud in [false, true] {
                let mut n = setter_node(None, None);
                run_setter(&mut n, &[nip], cloud, Some(res)).unwrap();
                assert_eq!(
                    addrs_of(&n),
                    vec![pair("InternalIP", res), pair("Hostname", "test-hostname")],
                    "{nip} cloud={cloud}"
                );
            }
        }
    }

    #[test]
    fn setter_no_node_ip_resolve() {
        for res in ["10.0.0.2", "2001:db2::2"] {
            let mut n = setter_node(None, None);
            run_setter(&mut n, &[], false, Some(res)).unwrap();
            assert_eq!(
                addrs_of(&n),
                vec![pair("InternalIP", res), pair("Hostname", "test-hostname")]
            );
            // "using cloud provider and no nodeIP resolve": hostname only.
            let mut n = setter_node(None, None);
            run_setter(&mut n, &[], true, Some(res)).unwrap();
            assert_eq!(addrs_of(&n), vec![pair("Hostname", "test-hostname")]);
        }
    }

    #[test]
    fn setter_cloud_provider_gets_node_ip_annotation() {
        let mut n = setter_node(None, None);
        run_setter(&mut n, &["10.1.1.1"], true, None).unwrap();
        assert_eq!(
            ann_of(&n),
            [(
                ANNOTATION_ALPHA_PROVIDED_IP_ADDR.to_string(),
                "10.1.1.1".to_string()
            )]
            .into()
        );
    }

    #[test]
    fn setter_cloud_provider_and_address_already_set_returns_early() {
        let mut n = setter_node(None, Some("10.1.1.2"));
        run_setter(&mut n, &["10.1.1.1"], true, None).unwrap();
        assert_eq!(addrs_of(&n), vec![pair("", "10.1.1.2")]);
    }

    #[test]
    fn setter_no_cloud_provider_no_annotation() {
        let mut n = setter_node(Some(&[]), None);
        run_setter(&mut n, &["10.1.1.1"], false, None).unwrap();
        assert!(ann_of(&n).is_empty());
        assert_eq!(
            addrs_of(&n),
            vec![
                pair("InternalIP", "10.1.1.1"),
                pair("Hostname", "test-hostname")
            ]
        );
    }

    #[test]
    fn setter_stale_annotation_removed_without_cloud_provider() {
        let mut n = setter_node(
            Some(&[(ANNOTATION_ALPHA_PROVIDED_IP_ADDR, "10.1.1.3")]),
            None,
        );
        run_setter(&mut n, &["10.1.1.1"], false, None).unwrap();
        assert!(ann_of(&n).is_empty());
    }

    #[test]
    fn setter_incorrect_annotation_fixed() {
        let mut n = setter_node(
            Some(&[(ANNOTATION_ALPHA_PROVIDED_IP_ADDR, "10.1.1.3")]),
            None,
        );
        run_setter(&mut n, &["10.1.1.1"], true, None).unwrap();
        assert_eq!(ann_of(&n)[ANNOTATION_ALPHA_PROVIDED_IP_ADDR], "10.1.1.1");
    }

    #[test]
    fn setter_dual_stack_cloud_annotation() {
        let mut n = setter_node(None, None);
        run_setter(
            &mut n,
            &["2600:1f14:1d4:d101::ba3d", "10.1.1.2"],
            true,
            None,
        )
        .unwrap();
        assert_eq!(
            addrs_of(&n),
            vec![
                pair("InternalIP", "2600:1f14:1d4:d101::ba3d"),
                pair("InternalIP", "10.1.1.2"),
                pair("Hostname", "test-hostname")
            ]
        );
        assert_eq!(
            ann_of(&n)[ANNOTATION_ALPHA_PROVIDED_IP_ADDR],
            "2600:1f14:1d4:d101::ba3d,10.1.1.2"
        );
    }

    #[test]
    fn setter_upgrade_and_downgrade_cloud_dual_stack() {
        let mut n = setter_node(
            Some(&[(ANNOTATION_ALPHA_PROVIDED_IP_ADDR, "10.1.1.1")]),
            None,
        );
        run_setter(
            &mut n,
            &["10.1.1.1", "2600:1f14:1d4:d101::ba3d"],
            true,
            None,
        )
        .unwrap();
        assert_eq!(
            ann_of(&n)[ANNOTATION_ALPHA_PROVIDED_IP_ADDR],
            "10.1.1.1,2600:1f14:1d4:d101::ba3d"
        );
        let mut n = setter_node(
            Some(&[(
                ANNOTATION_ALPHA_PROVIDED_IP_ADDR,
                "10.1.1.1,2600:1f14:1d4:d101::ba3d",
            )]),
            None,
        );
        run_setter(&mut n, &["10.1.1.1"], true, None).unwrap();
        assert_eq!(ann_of(&n)[ANNOTATION_ALPHA_PROVIDED_IP_ADDR], "10.1.1.1");
    }

    // TestNodeAddress_NoCloudProvider: invalid IPs error, addresses stay unset.
    #[test]
    fn setter_invalid_node_ips_error() {
        let mut n = setter_node(None, None);
        let e = set_node_address(
            &mut n,
            &[ip("127.0.0.1")],
            &validate_node_ip,
            "h",
            false,
            &none,
            &|_| Err("not reached".into()),
        )
        .unwrap_err();
        assert!(e.starts_with("failed to validate nodeIP:"), "{e}");
        let e = set_node_address(
            &mut n,
            &[ip("10.1.1.1"), ip("ff02::1")],
            &validate_node_ip,
            "h",
            false,
            &none,
            &|_| Err("not reached".into()),
        )
        .unwrap_err();
        assert!(e.starts_with("failed to validate secondaryNodeIP:"), "{e}");
        assert!(addrs_of(&n).is_empty());
    }

    fn status() -> rusternetes_common::resources::pod::PodStatus {
        Default::default()
    }

    fn pod_ips(s: &rusternetes_common::resources::pod::PodStatus) -> Vec<String> {
        s.pod_i_ps
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|p| p.ip)
            .collect()
    }

    // kubelet_pods_test.go TestGenerateAPIPodStatusHostNetworkPodIPs
    #[test]
    fn host_network_pod_ips_inherit_node_ips() {
        use rusternetes_common::resources::pod::PodIP;
        // (node host IPs, CRI pod IPs, expected PodIPs)
        let cases: Vec<(Vec<&str>, Vec<&str>, Vec<&str>)> = vec![
            (vec!["10.0.0.1"], vec![], vec!["10.0.0.1"]),
            (
                vec!["10.0.0.1", "fd01::1234"],
                vec![],
                vec!["10.0.0.1", "fd01::1234"],
            ),
            // CRI PodIPs override NodeAddresses; the secondary is appended
            (
                vec!["10.0.0.1", "fd01::1234"],
                vec!["192.168.0.1"],
                vec!["192.168.0.1", "fd01::1234"],
            ),
            (
                vec!["10.0.0.1", "fd01::1234"],
                vec!["192.168.0.1", "2001:db8::2"],
                vec!["192.168.0.1", "2001:db8::2"],
            ),
        ];
        for (host, cri, want) in cases {
            let host: Vec<IpAddr> = host.into_iter().map(ip).collect();
            let mut s = status();
            if !cri.is_empty() {
                s.pod_ip = Some(cri[0].to_string());
                s.pod_i_ps = Some(cri.iter().map(|i| PodIP { ip: i.to_string() }).collect());
            }
            apply_host_ips(true, &mut s, &host);
            assert_eq!(pod_ips(&s), want, "{host:?} {cri:?}");
            assert_eq!(s.pod_ip.as_deref(), Some(want[0]));
            assert_eq!(s.host_ip, Some(host[0].to_string()));
            assert_eq!(s.host_i_ps.unwrap().len(), host.len());
        }
    }

    #[test]
    fn non_host_network_pod_does_not_inherit_node_ips() {
        let mut s = status();
        apply_host_ips(false, &mut s, &[ip("10.0.0.1")]);
        assert_eq!(s.host_ip.as_deref(), Some("10.0.0.1"));
        assert!(s.pod_ip.is_none() && s.pod_i_ps.is_none());
    }

    // pod IPs are immutable once set; only the secondary is added (#105320)
    #[test]
    fn host_network_pod_ips_immutable_but_gain_secondary() {
        use rusternetes_common::resources::pod::PodIP;
        let mut s = status();
        s.pod_ip = Some("10.0.0.1".into());
        s.pod_i_ps = Some(vec![PodIP {
            ip: "10.0.0.1".into(),
        }]);
        apply_host_ips(true, &mut s, &[ip("1.1.1.1")]);
        assert_eq!(pod_ips(&s), vec!["10.0.0.1"]);
        apply_host_ips(true, &mut s, &[ip("1.1.1.1"), ip("2001:db8::2")]);
        assert_eq!(pod_ips(&s), vec!["10.0.0.1", "2001:db8::2"]);
    }

    // kubelet_pods.go:2011-2017 HostIPsIPFamilyMismatch
    #[test]
    fn host_ip_family_mismatch_message() {
        let mut s = status();
        s.host_ip = Some("10.0.0.1".into());
        let msg = apply_host_ips(false, &mut s, &[ip("fd00::1")]).unwrap();
        assert_eq!(
            msg,
            "Kubelet detected an IPv4 node IP (10.0.0.1), but the cloud provider selected an IPv6 node IP (fd00::1); pass an explicit `--node-ip` to kubelet to fix this."
        );
        assert_eq!(s.host_ip.as_deref(), Some("fd00::1"));
        let mut s = status();
        s.host_ip = Some("10.0.0.1".into());
        assert!(apply_host_ips(false, &mut s, &[ip("10.0.0.2")]).is_none());
        assert!(apply_host_ips(false, &mut status(), &[ip("10.0.0.2")]).is_none());
    }
}
