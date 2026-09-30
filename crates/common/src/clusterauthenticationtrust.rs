//! The data of the `kube-system/extension-apiserver-authentication`
//! ConfigMap — port of the pure half of
//! `pkg/controlplane/controller/clusterauthenticationtrust/cluster_authentication_trust_controller.go`:
//! reading the stored data, merging it with what this api-server requires,
//! and writing it back out. The control loop is the api-server's
//! (`bootstrap::spawn_cluster_authentication_trust_controller`).

use std::collections::HashMap;

/// `configMapNamespace` (cluster_authentication_trust_controller.go:52).
pub const CONFIG_MAP_NAMESPACE: &str = "kube-system";
/// `configMapName` (:53).
pub const CONFIG_MAP_NAME: &str = "extension-apiserver-authentication";

/// `ClusterAuthenticationInfo` (:76-93). A CA is a PEM bundle; a header list
/// of `None` is upstream's nil `StringSliceProvider`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ClusterAuthenticationInfo {
    /// Verifies the identity of normal clients.
    pub client_ca: Option<String>,
    pub request_header_username_headers: Option<Vec<String>>,
    pub request_header_uid_headers: Option<Vec<String>>,
    pub request_header_group_headers: Option<Vec<String>>,
    pub request_header_extra_header_prefixes: Option<Vec<String>>,
    /// Subjects allowed to act as a front proxy.
    pub request_header_allowed_names: Option<Vec<String>>,
    /// Verifies the front proxy.
    pub request_header_ca: Option<String>,
}

/// `jsonDeserializeStringSlice` (:343-353): an absent key is nil.
fn json_deserialize_string_slice(input: Option<&String>) -> Result<Option<Vec<String>>, String> {
    match input.filter(|s| !s.is_empty()) {
        None => Ok(None),
        Some(s) => serde_json::from_str(s).map(Some).map_err(|e| e.to_string()),
    }
}

/// `getClusterAuthenticationInfoFor` (:290-333). The UID headers are read
/// only when `RemoteRequestHeaderUID` is on.
pub fn get_cluster_authentication_info_for(
    data: &HashMap<String, String>,
    uid_gate: bool,
) -> Result<ClusterAuthenticationInfo, String> {
    let mut ret = ClusterAuthenticationInfo {
        request_header_group_headers: json_deserialize_string_slice(
            data.get("requestheader-group-headers"),
        )?,
        request_header_extra_header_prefixes: json_deserialize_string_slice(
            data.get("requestheader-extra-headers-prefix"),
        )?,
        request_header_allowed_names: json_deserialize_string_slice(
            data.get("requestheader-allowed-names"),
        )?,
        request_header_username_headers: json_deserialize_string_slice(
            data.get("requestheader-username-headers"),
        )?,
        ..Default::default()
    };
    if uid_gate {
        ret.request_header_uid_headers =
            json_deserialize_string_slice(data.get("requestheader-uid-headers"))?;
    }
    // `NewStaticCAContent` rejects a bundle with no certificate in it.
    for (key, slot) in [
        ("requestheader-client-ca-file", &mut ret.request_header_ca),
        ("client-ca-file", &mut ret.client_ca),
    ] {
        if let Some(bundle) = data.get(key).filter(|b| !b.is_empty()) {
            parse_certs_pem(bundle)?;
            *slot = Some(bundle.clone());
        }
    }
    Ok(ret)
}

/// `combineUniqueStringSlices` (:355-380): lhs then rhs, first occurrence
/// wins. The result is never nil.
fn combine_unique_string_slices(
    lhs: &Option<Vec<String>>,
    rhs: &Option<Vec<String>>,
) -> Vec<String> {
    let mut ret: Vec<String> = Vec::new();
    for curr in lhs.iter().chain(rhs.iter()).flatten() {
        if !ret.contains(curr) {
            ret.push(curr.clone());
        }
    }
    ret
}

/// `cert.ParseCertsPEM`: every `CERTIFICATE` block, as DER. At least one is
/// required.
fn parse_certs_pem(bundle: &str) -> Result<Vec<Vec<u8>>, String> {
    let blocks = pem::parse_many(bundle).map_err(|e| e.to_string())?;
    let certs: Vec<Vec<u8>> = blocks
        .into_iter()
        .filter(|b| b.tag() == "CERTIFICATE")
        .map(|b| b.into_contents())
        .collect();
    if certs.is_empty() {
        return Err("data does not contain any valid RSA or ECDSA certificates".to_string());
    }
    for der in &certs {
        x509_parser::parse_x509_certificate(der).map_err(|e| e.to_string())?;
    }
    Ok(certs)
}

/// `filterExpiredCerts` (:435-447): keep a certificate whose NotAfter is
/// later than five minutes ago.
fn not_expired(der: &[u8], now: i64) -> bool {
    x509_parser::parse_x509_certificate(der)
        .is_ok_and(|(_, c)| c.validity().not_after.timestamp() > now - 5 * 60)
}

/// `encodeCertificates` (:527-535): Go's `pem.Encode`, which ends lines with
/// LF.
fn encode_certificates(certs: &[Vec<u8>]) -> String {
    let config = pem::EncodeConfig::new().set_line_ending(pem::LineEnding::LF);
    certs
        .iter()
        .map(|der| pem::encode_config(&pem::Pem::new("CERTIFICATE", der.clone()), config))
        .collect()
}

/// `combineCertLists` (:382-432): lhs then rhs, expired and duplicate
/// certificates dropped. An empty result is nil.
fn combine_cert_lists(
    lhs: &Option<String>,
    rhs: &Option<String>,
    now: i64,
) -> Result<Option<String>, String> {
    let mut certificates: Vec<Vec<u8>> = Vec::new();
    for bundle in lhs.iter().chain(rhs.iter()) {
        certificates.extend(parse_certs_pem(bundle)?);
    }
    let mut finals: Vec<Vec<u8>> = Vec::new();
    for der in certificates.into_iter().filter(|d| not_expired(d, now)) {
        if !finals.contains(&der) {
            finals.push(der);
        }
    }
    if finals.is_empty() {
        return Ok(None);
    }
    Ok(Some(encode_certificates(&finals)))
}

/// `combinedClusterAuthenticationInfo` (:222-242).
pub fn combined_cluster_authentication_info(
    lhs: &ClusterAuthenticationInfo,
    rhs: &ClusterAuthenticationInfo,
    now: i64,
) -> Result<ClusterAuthenticationInfo, String> {
    Ok(ClusterAuthenticationInfo {
        request_header_allowed_names: Some(combine_unique_string_slices(
            &lhs.request_header_allowed_names,
            &rhs.request_header_allowed_names,
        )),
        request_header_extra_header_prefixes: Some(combine_unique_string_slices(
            &lhs.request_header_extra_header_prefixes,
            &rhs.request_header_extra_header_prefixes,
        )),
        request_header_group_headers: Some(combine_unique_string_slices(
            &lhs.request_header_group_headers,
            &rhs.request_header_group_headers,
        )),
        request_header_username_headers: Some(combine_unique_string_slices(
            &lhs.request_header_username_headers,
            &rhs.request_header_username_headers,
        )),
        request_header_uid_headers: Some(combine_unique_string_slices(
            &lhs.request_header_uid_headers,
            &rhs.request_header_uid_headers,
        )),
        client_ca: combine_cert_lists(&lhs.client_ca, &rhs.client_ca, now)?,
        request_header_ca: combine_cert_lists(&lhs.request_header_ca, &rhs.request_header_ca, now)?,
    })
}

fn json_serialize_string_slice(input: &Option<Vec<String>>) -> String {
    serde_json::to_string(input.as_deref().unwrap_or_default()).expect("a string list encodes")
}

/// `getConfigMapDataFor` (:244-288).
pub fn get_config_map_data_for(
    info: &ClusterAuthenticationInfo,
    uid_gate: bool,
) -> HashMap<String, String> {
    let mut data = HashMap::new();
    if let Some(ca) = info.client_ca.as_ref().filter(|c| !c.is_empty()) {
        data.insert("client-ca-file".to_string(), ca.clone());
    }
    let Some(ca) = info.request_header_ca.as_ref().filter(|c| !c.is_empty()) else {
        return data;
    };
    data.insert(
        "requestheader-username-headers".to_string(),
        json_serialize_string_slice(&info.request_header_username_headers),
    );
    if uid_gate
        && info
            .request_header_uid_headers
            .as_ref()
            .is_some_and(|h| !h.is_empty())
    {
        data.insert(
            "requestheader-uid-headers".to_string(),
            json_serialize_string_slice(&info.request_header_uid_headers),
        );
    }
    data.insert(
        "requestheader-group-headers".to_string(),
        json_serialize_string_slice(&info.request_header_group_headers),
    );
    data.insert(
        "requestheader-extra-headers-prefix".to_string(),
        json_serialize_string_slice(&info.request_header_extra_header_prefixes),
    );
    data.insert("requestheader-client-ca-file".to_string(), ca.clone());
    data.insert(
        "requestheader-allowed-names".to_string(),
        json_serialize_string_slice(&info.request_header_allowed_names),
    );
    data
}

/// The data half of `syncConfigMap` (:140-179): the stored data merged with
/// what is required, or `None` when nothing changes. `existing` is `None`
/// when the ConfigMap does not exist.
pub fn sync_config_map_data(
    existing: Option<&HashMap<String, String>>,
    required: &ClusterAuthenticationInfo,
    uid_gate: bool,
    now: i64,
) -> Result<Option<HashMap<String, String>>, String> {
    let empty = HashMap::new();
    let existing_info = get_cluster_authentication_info_for(existing.unwrap_or(&empty), uid_gate)?;
    let combined = combined_cluster_authentication_info(&existing_info, required, now)?;
    let data = get_config_map_data_for(&combined, uid_gate);
    // A missing ConfigMap has nil data, which no computed map equals.
    if existing == Some(&data) {
        return Ok(None);
    }
    Ok(Some(data))
}

#[cfg(test)]
mod tests {
    use super::*;

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
