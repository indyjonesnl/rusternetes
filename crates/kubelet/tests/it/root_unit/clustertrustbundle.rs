//! Unit tests moved from `src/clustertrustbundle.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use rusternetes_common::resources::ClusterTrustBundle;
    pub use rusternetes_common::types::{label_selector_as_selector, LabelSelector};
    pub use rusternetes_storage::{build_key, build_prefix, Storage};
    pub use std::collections::BTreeSet;
    pub use std::hash::BuildHasher;
    pub use std::sync::OnceLock;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::clustertrustbundle::*;
    use rusternetes_storage::StorageBackend;
    use std::collections::HashMap;

    /// A syntactically valid PEM `CERTIFICATE` block. `diffBundles`-style
    /// comparison (decode, sort) is used below, as upstream's test does
    /// (`clustertrustbundle_manager_test.go:501-544`), so only the decoded
    /// bytes matter, not that this is a real certificate.
    fn root(tag: &str) -> String {
        pem::encode_config(
            &pem::Pem::new("CERTIFICATE", tag.as_bytes().to_vec()),
            pem::EncodeConfig::new().set_line_ending(pem::LineEnding::LF),
        )
    }

    fn decoded_sorted(b: &[u8]) -> Vec<Vec<u8>> {
        let mut v: Vec<Vec<u8>> = pem::parse_many(b)
            .unwrap()
            .into_iter()
            .map(|p| p.into_contents())
            .collect();
        v.sort();
        v
    }

    fn same_bundle(got: &[u8], want: &str) -> bool {
        decoded_sorted(got) == decoded_sorted(want.as_bytes())
    }

    async fn put(
        st: &StorageBackend,
        name: &str,
        signer: &str,
        labels: serde_json::Value,
        bundle: &str,
    ) {
        let mut ctb = ClusterTrustBundle::new(name, signer, bundle);
        ctb.metadata.labels = serde_json::from_value(labels).unwrap();
        st.create(&build_key(RESOURCE, None, name), &ctb)
            .await
            .unwrap();
    }

    fn ls(v: serde_json::Value) -> LabelSelector {
        serde_json::from_value(v).unwrap()
    }

    /// `testGetTrustAnchorsByName` (`clustertrustbundle_manager_test.go:116-165`).
    #[tokio::test]
    async fn by_name_found_missing_and_allow_missing() {
        let st = StorageBackend::new_memory();
        put(&st, "ctb1", "", serde_json::json!({}), &root("root1")).await;
        put(&st, "ctb2", "", serde_json::json!({}), &root("root2")).await;
        let got = get_trust_anchors_by_name(&st, "ctb1", false).await.unwrap();
        assert!(same_bundle(&got, &root("root1")));
        let got = get_trust_anchors_by_name(&st, "ctb2", false).await.unwrap();
        assert!(same_bundle(&got, &root("root2")));
        assert!(get_trust_anchors_by_name(&st, "not-found", false)
            .await
            .is_err());
        assert!(get_trust_anchors_by_name(&st, "not-found", true)
            .await
            .unwrap()
            .is_empty());
    }

    /// `testGetTrustAnchorsBySignerName` (`clustertrustbundle_manager_test.go:
    /// 253-368`), every subtest, including the duplicate bundle that must be
    /// deduplicated.
    #[tokio::test]
    async fn by_signer_matches_upstream_subtests() {
        let st = StorageBackend::new_memory();
        let a = serde_json::json!({"label": "a"});
        let b = serde_json::json!({"label": "b"});
        put(
            &st,
            "signer-a-label-a-1",
            "foo.bar/a",
            a.clone(),
            &root("0"),
        )
        .await;
        put(
            &st,
            "signer-a-label-a-2",
            "foo.bar/a",
            a.clone(),
            &root("1"),
        )
        .await;
        put(
            &st,
            "signer-a-label-2-dup",
            "foo.bar/a",
            a.clone(),
            &root("1"),
        )
        .await;
        put(
            &st,
            "signer-a-label-b-1",
            "foo.bar/a",
            b.clone(),
            &root("2"),
        )
        .await;
        put(
            &st,
            "signer-b-label-a-1",
            "foo.bar/b",
            a.clone(),
            &root("3"),
        )
        .await;

        // "signer-a label-a should yield two sorted certificates"
        let sel = ls(serde_json::json!({"matchLabels": {"label": "a"}}));
        let got = get_trust_anchors_by_signer(&st, "foo.bar/a", Some(&sel), false)
            .await
            .unwrap();
        assert!(same_bundle(&got, &(root("0") + &root("1"))));

        // "signer-a with nil selector should yield zero certificates"
        let got = get_trust_anchors_by_signer(&st, "foo.bar/a", None, true)
            .await
            .unwrap();
        assert!(got.is_empty());

        // "signer-b with empty selector should yield one certificates"
        let empty = ls(serde_json::json!({}));
        let got = get_trust_anchors_by_signer(&st, "foo.bar/b", Some(&empty), false)
            .await
            .unwrap();
        assert!(same_bundle(&got, &root("3")));

        // "signer-a label-b should yield one certificate"
        let sel = ls(serde_json::json!({"matchLabels": {"label": "b"}}));
        let got = get_trust_anchors_by_signer(&st, "foo.bar/a", Some(&sel), false)
            .await
            .unwrap();
        assert!(same_bundle(&got, &root("2")));

        // "signer-b label-a should yield one certificate"
        let sel = ls(serde_json::json!({"matchLabels": {"label": "a"}}));
        let got = get_trust_anchors_by_signer(&st, "foo.bar/b", Some(&sel), false)
            .await
            .unwrap();
        assert!(same_bundle(&got, &root("3")));

        // "signer-b label-b allowMissing=true should yield zero certificates"
        let sel = ls(serde_json::json!({"matchLabels": {"label": "b"}}));
        assert!(
            get_trust_anchors_by_signer(&st, "foo.bar/b", Some(&sel), true)
                .await
                .unwrap()
                .is_empty()
        );
        // "... allowMissing=false should yield zero certificates (error)"
        let err = get_trust_anchors_by_signer(&st, "foo.bar/b", Some(&sel), false)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "combination of signerName and labelSelector matched zero ClusterTrustBundles"
        );
    }

    /// `GetSignerName(ctb) == signerName` (`:260`) compares the typed
    /// `spec.signerName`, so a name-only bundle (signerName "") matches "".
    #[tokio::test]
    async fn by_signer_empty_signer_name_matches_unsigned_bundles() {
        let st = StorageBackend::new_memory();
        put(&st, "plain", "", serde_json::json!({}), &root("p")).await;
        put(
            &st,
            "signed",
            "foo.bar/a",
            serde_json::json!({}),
            &root("s"),
        )
        .await;
        let empty = ls(serde_json::json!({}));
        let got = get_trust_anchors_by_signer(&st, "", Some(&empty), false)
            .await
            .unwrap();
        assert!(same_bundle(&got, &root("p")));
    }

    /// "big labelselector should cause error" (`...manager_test.go:277-291`).
    #[tokio::test]
    async fn big_label_selector_is_an_error() {
        let st = StorageBackend::new_memory();
        let value = "v".repeat(63);
        let labels: HashMap<String, String> = (0..(100 * 1024 / 63 + 1))
            .map(|i| (format!("key-{i}"), value.clone()))
            .collect();
        let sel = LabelSelector {
            match_labels: Some(labels),
            match_expressions: None,
        };
        let err = get_trust_anchors_by_signer(&st, "foo.bar/a", Some(&sel), false)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("label selector length"), "{err}");
    }

    /// `normalizeTrustAnchors` (`:285-312`): output is LF-ended `CERTIFICATE`
    /// blocks, deduplicated across bundles, stable across calls.
    #[test]
    fn normalize_dedupes_and_is_stable_within_a_process() {
        let bundles = vec![root("x") + &root("y"), root("y")];
        let a = normalize_trust_anchors(&bundles);
        let b = normalize_trust_anchors(&bundles);
        assert_eq!(a, b);
        assert_eq!(decoded_sorted(&a).len(), 2);
        let text = String::from_utf8(a).unwrap();
        assert!(text.starts_with("-----BEGIN CERTIFICATE-----\n"));
        assert!(!text.contains('\r'));
    }
}
