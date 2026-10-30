//! Unit tests moved from `src/clusterauthenticationtrust.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::clusterauthenticationtrust::*;

    const SOME_RANDOM_CA: &str = r#"-----BEGIN CERTIFICATE-----
MIIBqDCCAU2gAwIBAgIUfbqeieihh/oERbfvRm38XvS/xHAwCgYIKoZIzj0EAwIw
GjEYMBYGA1UEAxMPSW50ZXJtZWRpYXRlLUNBMCAXDTE2MTAxMTA1MDYwMFoYDzIx
MTYwOTE3MDUwNjAwWjAUMRIwEAYDVQQDEwlNeSBDbGllbnQwWTATBgcqhkjOPQIB
BggqhkjOPQMBBwNCAARv6N4R/sjMR65iMFGNLN1GC/vd7WhDW6J4X/iAjkRLLnNb
KbRG/AtOUZ+7upJ3BWIRKYbOabbQGQe2BbKFiap4o3UwczAOBgNVHQ8BAf8EBAMC
BaAwEwYDVR0lBAwwCgYIKwYBBQUHAwIwDAYDVR0TAQH/BAIwADAdBgNVHQ4EFgQU
K/pZOWpNcYai6eHFpmJEeFpeQlEwHwYDVR0jBBgwFoAUX6nQlxjfWnP6aM1meO/Q
a6b3a9kwCgYIKoZIzj0EAwIDSQAwRgIhAIWTKw/sjJITqeuNzJDAKU4xo1zL+xJ5
MnVCuBwfwDXCAiEAw/1TA+CjPq9JC5ek1ifR0FybTURjeQqYkKpve1dveps=
-----END CERTIFICATE-----
"#;
    const ANOTHER_RANDOM_CA: &str = r#"-----BEGIN CERTIFICATE-----
MIIDQDCCAiigAwIBAgIJANWw74P5KJk2MA0GCSqGSIb3DQEBCwUAMDQxMjAwBgNV
BAMMKWdlbmVyaWNfd2ViaG9va19hZG1pc3Npb25fcGx1Z2luX3Rlc3RzX2NhMCAX
DTE3MTExNjAwMDUzOVoYDzIyOTEwOTAxMDAwNTM5WjAjMSEwHwYDVQQDExh3ZWJo
b29rLXRlc3QuZGVmYXVsdC5zdmMwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK
AoIBAQDXd/nQ89a5H8ifEsigmMd01Ib6NVR3bkJjtkvYnTbdfYEBj7UzqOQtHoLa
dIVmefny5uIHvj93WD8WDVPB3jX2JHrXkDTXd/6o6jIXHcsUfFTVLp6/bZ+Anqe0
r/7hAPkzA2A7APyTWM3ZbEeo1afXogXhOJ1u/wz0DflgcB21gNho4kKTONXO3NHD
XLpspFqSkxfEfKVDJaYAoMnYZJtFNsa2OvsmLnhYF8bjeT3i07lfwrhUZvP+7Gsp
7UgUwc06WuNHjfx1s5e6ySzH0QioMD1rjYneqOvk0pKrMIhuAEWXqq7jlXcDtx1E
j+wnYbVqqVYheHZ8BCJoVAAQGs9/AgMBAAGjZDBiMAkGA1UdEwQCMAAwCwYDVR0P
BAQDAgXgMB0GA1UdJQQWMBQGCCsGAQUFBwMCBggrBgEFBQcDATApBgNVHREEIjAg
hwR/AAABghh3ZWJob29rLXRlc3QuZGVmYXVsdC5zdmMwDQYJKoZIhvcNAQELBQAD
ggEBAD/GKSPNyQuAOw/jsYZesb+RMedbkzs18sSwlxAJQMUrrXwlVdHrA8q5WhE6
ABLqU1b8lQ8AWun07R8k5tqTmNvCARrAPRUqls/ryER+3Y9YEcxEaTc3jKNZFLbc
T6YtcnkdhxsiO136wtiuatpYL91RgCmuSpR8+7jEHhuFU01iaASu7ypFrUzrKHTF
bKwiLRQi1cMzVcLErq5CDEKiKhUkoDucyARFszrGt9vNIl/YCcBOkcNvM3c05Hn3
M++C29JwS3Hwbubg6WO3wjFjoEhpCwU6qRYUz3MRp4tHO4kxKXx+oQnUiFnR7vW0
YkNtGc1RUDHwecCTFpJtPb7Yu/E=
-----END CERTIFICATE-----
"#;

    fn now() -> i64 {
        chrono::Utc::now().timestamp()
    }

    fn strs(v: &[&str]) -> Option<Vec<String>> {
        Some(v.iter().map(|s| s.to_string()).collect())
    }

    fn data(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Name, required info, stored data, `RemoteRequestHeaderUID`, and the
    /// data a sync writes (`None`: no write).
    type Case = (
        &'static str,
        ClusterAuthenticationInfo,
        Option<HashMap<String, String>>,
        bool,
        Option<HashMap<String, String>>,
    );

    /// `TestWriteClientCAs` (cluster_authentication_trust_controller_test.go:94-476).
    #[test]
    fn write_client_cas() {
        let cases: Vec<Case> = vec![
            (
                "basic",
                ClusterAuthenticationInfo {
                    client_ca: Some(SOME_RANDOM_CA.into()),
                    request_header_username_headers: strs(&["alfa", "bravo", "charlie"]),
                    request_header_uid_headers: strs(&["golf", "hotel", "india"]),
                    request_header_group_headers: strs(&["delta"]),
                    request_header_extra_header_prefixes: strs(&["echo", "foxtrot"]),
                    request_header_ca: Some(ANOTHER_RANDOM_CA.into()),
                    request_header_allowed_names: strs(&["first", "second"]),
                },
                None,
                false,
                Some(data(&[
                    ("client-ca-file", SOME_RANDOM_CA),
                    (
                        "requestheader-username-headers",
                        r#"["alfa","bravo","charlie"]"#,
                    ),
                    ("requestheader-group-headers", r#"["delta"]"#),
                    (
                        "requestheader-extra-headers-prefix",
                        r#"["echo","foxtrot"]"#,
                    ),
                    ("requestheader-client-ca-file", ANOTHER_RANDOM_CA),
                    ("requestheader-allowed-names", r#"["first","second"]"#),
                ])),
            ),
            (
                "basic with feature gate",
                ClusterAuthenticationInfo {
                    client_ca: Some(SOME_RANDOM_CA.into()),
                    request_header_username_headers: strs(&["alfa", "bravo", "charlie"]),
                    request_header_uid_headers: strs(&["golf", "hotel", "india"]),
                    request_header_group_headers: strs(&["delta"]),
                    request_header_extra_header_prefixes: strs(&["echo", "foxtrot"]),
                    request_header_ca: Some(ANOTHER_RANDOM_CA.into()),
                    request_header_allowed_names: strs(&["first", "second"]),
                },
                None,
                true,
                Some(data(&[
                    ("client-ca-file", SOME_RANDOM_CA),
                    (
                        "requestheader-username-headers",
                        r#"["alfa","bravo","charlie"]"#,
                    ),
                    ("requestheader-uid-headers", r#"["golf","hotel","india"]"#),
                    ("requestheader-group-headers", r#"["delta"]"#),
                    (
                        "requestheader-extra-headers-prefix",
                        r#"["echo","foxtrot"]"#,
                    ),
                    ("requestheader-client-ca-file", ANOTHER_RANDOM_CA),
                    ("requestheader-allowed-names", r#"["first","second"]"#),
                ])),
            ),
            (
                "skip client-ca",
                ClusterAuthenticationInfo {
                    request_header_ca: Some(ANOTHER_RANDOM_CA.into()),
                    request_header_allowed_names: strs(&["first", "second"]),
                    ..Default::default()
                },
                None,
                false,
                Some(data(&[
                    ("requestheader-username-headers", "[]"),
                    ("requestheader-group-headers", "[]"),
                    ("requestheader-extra-headers-prefix", "[]"),
                    ("requestheader-client-ca-file", ANOTHER_RANDOM_CA),
                    ("requestheader-allowed-names", r#"["first","second"]"#),
                ])),
            ),
            (
                "skip requestheader",
                ClusterAuthenticationInfo {
                    client_ca: Some(SOME_RANDOM_CA.into()),
                    ..Default::default()
                },
                None,
                false,
                Some(data(&[("client-ca-file", SOME_RANDOM_CA)])),
            ),
            (
                "overwrite extension-apiserver-authentication",
                ClusterAuthenticationInfo {
                    client_ca: Some(SOME_RANDOM_CA.into()),
                    ..Default::default()
                },
                Some(data(&[("client-ca-file", ANOTHER_RANDOM_CA)])),
                false,
                Some(data(&[(
                    "client-ca-file",
                    &format!("{ANOTHER_RANDOM_CA}{SOME_RANDOM_CA}"),
                )])),
            ),
            (
                "overwrite extension-apiserver-authentication requestheader",
                ClusterAuthenticationInfo {
                    request_header_username_headers: strs(&[]),
                    request_header_group_headers: strs(&[]),
                    request_header_extra_header_prefixes: strs(&[]),
                    request_header_ca: Some(ANOTHER_RANDOM_CA.into()),
                    request_header_allowed_names: strs(&[]),
                    ..Default::default()
                },
                Some(data(&[
                    ("requestheader-username-headers", "[]"),
                    ("requestheader-group-headers", "[]"),
                    ("requestheader-extra-headers-prefix", "[]"),
                    ("requestheader-client-ca-file", SOME_RANDOM_CA),
                    ("requestheader-allowed-names", "[]"),
                ])),
                false,
                Some(data(&[
                    ("requestheader-username-headers", "[]"),
                    ("requestheader-group-headers", "[]"),
                    ("requestheader-extra-headers-prefix", "[]"),
                    (
                        "requestheader-client-ca-file",
                        &format!("{SOME_RANDOM_CA}{ANOTHER_RANDOM_CA}"),
                    ),
                    ("requestheader-allowed-names", "[]"),
                ])),
            ),
            (
                "skip on no change",
                ClusterAuthenticationInfo {
                    request_header_username_headers: strs(&[]),
                    request_header_group_headers: strs(&[]),
                    request_header_extra_header_prefixes: strs(&[]),
                    request_header_ca: Some(ANOTHER_RANDOM_CA.into()),
                    request_header_allowed_names: strs(&[]),
                    ..Default::default()
                },
                Some(data(&[
                    ("requestheader-username-headers", "[]"),
                    ("requestheader-group-headers", "[]"),
                    ("requestheader-extra-headers-prefix", "[]"),
                    ("requestheader-client-ca-file", ANOTHER_RANDOM_CA),
                    ("requestheader-allowed-names", "[]"),
                ])),
                false,
                None,
            ),
            (
                "drop uid without feature gate",
                ClusterAuthenticationInfo {
                    request_header_username_headers: strs(&[]),
                    request_header_uid_headers: strs(&["panda"]),
                    request_header_group_headers: strs(&[]),
                    request_header_extra_header_prefixes: strs(&[]),
                    request_header_ca: Some(ANOTHER_RANDOM_CA.into()),
                    request_header_allowed_names: strs(&[]),
                    ..Default::default()
                },
                Some(data(&[
                    ("requestheader-username-headers", "[]"),
                    ("requestheader-uid-headers", r#"["snorlax"]"#),
                    ("requestheader-group-headers", "[]"),
                    ("requestheader-extra-headers-prefix", "[]"),
                    ("requestheader-client-ca-file", ANOTHER_RANDOM_CA),
                    ("requestheader-allowed-names", "[]"),
                ])),
                false,
                Some(data(&[
                    ("requestheader-username-headers", "[]"),
                    ("requestheader-group-headers", "[]"),
                    ("requestheader-extra-headers-prefix", "[]"),
                    ("requestheader-client-ca-file", ANOTHER_RANDOM_CA),
                    ("requestheader-allowed-names", "[]"),
                ])),
            ),
            (
                "append uid with feature gate",
                ClusterAuthenticationInfo {
                    request_header_username_headers: strs(&[]),
                    request_header_uid_headers: strs(&["panda"]),
                    request_header_group_headers: strs(&[]),
                    request_header_extra_header_prefixes: strs(&[]),
                    request_header_ca: Some(ANOTHER_RANDOM_CA.into()),
                    request_header_allowed_names: strs(&[]),
                    ..Default::default()
                },
                Some(data(&[
                    ("requestheader-username-headers", "[]"),
                    ("requestheader-uid-headers", r#"["snorlax"]"#),
                    ("requestheader-group-headers", "[]"),
                    ("requestheader-extra-headers-prefix", "[]"),
                    ("requestheader-client-ca-file", ANOTHER_RANDOM_CA),
                    ("requestheader-allowed-names", "[]"),
                ])),
                true,
                Some(data(&[
                    ("requestheader-username-headers", "[]"),
                    ("requestheader-uid-headers", r#"["snorlax","panda"]"#),
                    ("requestheader-group-headers", "[]"),
                    ("requestheader-extra-headers-prefix", "[]"),
                    ("requestheader-client-ca-file", ANOTHER_RANDOM_CA),
                    ("requestheader-allowed-names", "[]"),
                ])),
            ),
        ];
        for (name, required, existing, uid_gate, want) in cases {
            let got = sync_config_map_data(existing.as_ref(), &required, uid_gate, now())
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(got, want, "{name}");
        }
    }

    /// `filterExpiredCerts` (:435-447): a bundle whose only certificate
    /// expired more than five minutes ago combines to nothing.
    #[test]
    fn expired_certificates_are_dropped() {
        let after_expiry = i64::MAX / 2;
        assert_eq!(
            combine_cert_lists(&Some(SOME_RANDOM_CA.into()), &None, after_expiry),
            Ok(None)
        );
        assert_eq!(
            combine_cert_lists(
                &Some(SOME_RANDOM_CA.into()),
                &Some(SOME_RANDOM_CA.into()),
                now()
            ),
            Ok(Some(SOME_RANDOM_CA.to_string()))
        );
    }
}
