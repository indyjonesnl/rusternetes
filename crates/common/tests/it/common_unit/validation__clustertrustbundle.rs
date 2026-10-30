//! Unit tests moved from `src/validation/clustertrustbundle.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::ClusterTrustBundle;
    pub use rusternetes_common::validation::certificatesigningrequest::validate_signer_name;
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_immutable_field,
        validate_object_meta_accessor_with_opts_common, validate_object_meta_update,
    };
}

mod tests {
    use super::__parent::*;
    use rcgen::{BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa, KeyPair};
    use rusternetes_common::validation::clustertrustbundle::*;

    /// A DER certificate; `ca` sets the basic-constraints CA bit
    /// (validation_test.go `mustMakeCertificate`).
    fn make_cert(cn: &str, ca: bool) -> Vec<u8> {
        let mut params = CertificateParams::default();
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, cn);
        params.distinguished_name = dn;
        params.is_ca = if ca {
            IsCa::Ca(BasicConstraints::Unconstrained)
        } else {
            IsCa::ExplicitNoCa
        };
        let key = KeyPair::generate().unwrap();
        params.self_signed(&key).unwrap().der().to_vec()
    }

    /// `mustMakePEMBlock`.
    fn block(ty: &str, headers: &[(&str, &str)], data: &[u8]) -> String {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD.encode(data);
        let mut out = format!("-----BEGIN {ty}-----\n");
        for (k, v) in headers {
            out.push_str(&format!("{k}: {v}\n"));
        }
        if !headers.is_empty() {
            out.push('\n');
        }
        for chunk in b64.as_bytes().chunks(64) {
            out.push_str(std::str::from_utf8(chunk).unwrap());
            out.push('\n');
        }
        out.push_str(&format!("-----END {ty}-----\n"));
        out
    }

    fn ctb(name: &str, signer: &str, bundle: &str) -> ClusterTrustBundle {
        ClusterTrustBundle::new(name, signer, bundle)
    }

    const SUBDOMAIN_MSG: &str = "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')";

    fn trust_path() -> Path {
        Path::new("spec").child("trustBundle")
    }

    fn omitted(detail: &str) -> Error {
        Error::invalid(&trust_path(), "<value omitted>", detail)
    }

    /// validation_test.go:1172-1479 TestValidateClusterTrustBundle.
    #[test]
    fn validate_cluster_trust_bundle_cases() {
        let c1 = make_cert("root1", true);
        let c2 = make_cert("root2", true);
        let not_ca = make_cert("root3", false);
        let good1 = block("CERTIFICATE", &[], &c1);
        let good2 = block("CERTIFICATE", &[], &c2);
        let good1_alt = good1.replace('\n', "\n\t\n");
        let bad_not_ca = block("CERTIFICATE", &[], &not_ca);
        let bad_headers = block("CERTIFICATE", &[("key", "value")], &c1);
        let bad_type = block("NOTACERTIFICATE", &[], &c1);
        let bad_parse = block("CERTIFICATE", &[], b"this is not a certificate");
        let too_big = format!("{good1}\n").repeat(MAX_TRUST_BUNDLE_SIZE / good1.len() + 1);
        let name_path = Path::new("metadata").child("name");

        let cases: Vec<(&str, ClusterTrustBundle, bool, ErrorList)> = vec![
            ("valid, no signer name", ctb("foo", "", &good1), false, vec![]),
            (
                "invalid, too big",
                ctb("foo", "", &too_big),
                false,
                vec![Error::too_long(&trust_path(), MAX_TRUST_BUNDLE_SIZE)],
            ),
            (
                "invalid, no signer name, invalid name",
                ctb("k8s.io:bar:foo", "", &good1),
                false,
                vec![Error::invalid(
                    &name_path,
                    "k8s.io:bar:foo",
                    "ClusterTrustBundle without signer name must not have \":\" in its name",
                )],
            ),
            (
                "valid, with signer name",
                ctb("k8s.io:foo:bar", "k8s.io/foo", &good1),
                false,
                vec![],
            ),
            (
                "invalid, with signer name, missing name prefix",
                ctb("look-ma-no-prefix", "k8s.io/foo", &good1),
                false,
                vec![Error::invalid(
                    &name_path,
                    "look-ma-no-prefix",
                    "ClusterTrustBundle for signerName k8s.io/foo must be named with prefix k8s.io:foo:",
                )],
            ),
            (
                "invalid, with signer name, empty name suffix",
                ctb("k8s.io:foo:", "k8s.io/foo", &good1),
                false,
                vec![Error::invalid(&name_path, "k8s.io:foo:", SUBDOMAIN_MSG)],
            ),
            (
                "invalid, with signer name, bad name suffix",
                ctb("k8s.io:foo:123notvalidDNSSubdomain", "k8s.io/foo", &good1),
                false,
                vec![Error::invalid(
                    &name_path,
                    "k8s.io:foo:123notvalidDNSSubdomain",
                    SUBDOMAIN_MSG,
                )],
            ),
            (
                "valid, with signer name, with inter-block garbage",
                ctb(
                    "k8s.io:foo:abc",
                    "k8s.io/foo",
                    &format!("garbage\n{good1}\ngarbage\n{good2}"),
                ),
                false,
                vec![],
            ),
            (
                "invalid, no signer name, no trust anchors",
                ctb("foo", "", ""),
                false,
                vec![omitted("at least one trust anchor must be provided")],
            ),
            (
                "invalid, no trust anchors",
                ctb("k8s.io:foo:abc", "k8s.io/foo", ""),
                false,
                vec![omitted("at least one trust anchor must be provided")],
            ),
            (
                "invalid, bad signer name",
                ctb("invalid:foo", "invalid", &good1),
                false,
                vec![Error::invalid(
                    &Path::new("spec").child("signerName"),
                    "invalid",
                    "must be a fully qualified domain and path of the form 'example.com/signer-name'",
                )],
            ),
            (
                "invalid, no blocks",
                ctb("foo", "", "non block garbage"),
                false,
                vec![omitted("at least one trust anchor must be provided")],
            ),
            (
                "invalid, bad block type",
                ctb("foo", "", &format!("{good1}\n{bad_type}")),
                false,
                vec![omitted("entry 1 has bad block type: NOTACERTIFICATE")],
            ),
            (
                "invalid, block with headers",
                ctb("foo", "", &format!("{good1}\n{bad_headers}")),
                false,
                vec![omitted("entry 1 has PEM block headers")],
            ),
            (
                "invalid, cert is not a CA cert",
                ctb("foo", "", &bad_not_ca),
                false,
                vec![omitted("entry 0 does not have the CA bit set")],
            ),
            (
                "invalid, duplicated blocks",
                ctb("foo", "", &format!("{good1}\n{good1_alt}")),
                false,
                vec![omitted("duplicate trust anchor (indices [0 1])")],
            ),
            (
                "invalid, non-certificate entry",
                ctb("foo", "", &format!("{good1}\n{bad_parse}")),
                false,
                vec![omitted("entry 1 does not parse as X.509")],
            ),
            (
                "allow any old garbage in the PEM field if we suppress parsing",
                ctb("foo", "", "garbage"),
                true,
                vec![],
            ),
        ];

        for (description, bundle, suppress, want) in cases {
            let opts = ValidateClusterTrustBundleOptions {
                suppress_bundle_parsing: suppress,
            };
            assert_eq!(
                validate_cluster_trust_bundle(&bundle, opts),
                want,
                "{description}"
            );

            // With no change to the object, the update must not report errors
            // about spec.trustBundle (validation_test.go:1459-1476).
            let mut old = bundle.clone();
            old.metadata.resource_version = Some("1".to_string());
            let mut new = bundle.clone();
            new.metadata.resource_version = Some("2".to_string());
            let want_update: ErrorList = want
                .into_iter()
                .filter(|e| e.field != "spec.trustBundle")
                .collect();
            assert_eq!(
                validate_cluster_trust_bundle_update(&new, &old),
                want_update,
                "{description} (update)"
            );
        }
    }

    /// validation_test.go:1481-1609 TestValidateClusterTrustBundleUpdate.
    #[test]
    fn validate_cluster_trust_bundle_update_cases() {
        let good1 = block("CERTIFICATE", &[], &make_cert("root1", true));
        let good2 = block("CERTIFICATE", &[], &make_cert("root2", true));
        let name = "k8s.io:foo:bar";

        let cases: Vec<(&str, ClusterTrustBundle, ClusterTrustBundle, ErrorList)> = vec![
            (
                "changing signer name disallowed",
                ctb(name, "k8s.io/foo", &good1),
                ctb(name, "k8s.io/bar", &good1),
                vec![
                    Error::invalid(
                        &Path::new("metadata").child("name"),
                        name,
                        "ClusterTrustBundle for signerName k8s.io/bar must be named with prefix k8s.io:bar:",
                    ),
                    Error::invalid(
                        &Path::new("spec").child("signerName"),
                        serde_json::json!("k8s.io/bar"),
                        "field is immutable",
                    ),
                ],
            ),
            (
                "adding certificate allowed",
                ctb(name, "k8s.io/foo", &good1),
                ctb(name, "k8s.io/foo", &format!("{good1}\n{good2}")),
                vec![],
            ),
            (
                "emptying trustBundle disallowed",
                ctb(name, "k8s.io/foo", &good1),
                ctb(name, "k8s.io/foo", ""),
                vec![omitted("at least one trust anchor must be provided")],
            ),
            (
                "emptying trustBundle (replace with non-block garbage) disallowed",
                ctb(name, "k8s.io/foo", &good1),
                ctb(name, "k8s.io/foo", "non block garbage"),
                vec![omitted("at least one trust anchor must be provided")],
            ),
        ];
        for (description, mut old, mut new, want) in cases {
            // The Go fixtures carry no uid/creationTimestamp; `ObjectMeta::new`
            // stamps fresh ones, so line them up.
            new.metadata.uid = old.metadata.uid.clone();
            new.metadata.creation_timestamp = old.metadata.creation_timestamp;
            old.metadata.resource_version = Some("1".to_string());
            new.metadata.resource_version = Some("2".to_string());
            assert_eq!(
                validate_cluster_trust_bundle_update(&new, &old),
                want,
                "{description}"
            );
        }
    }
}
