//! Port of the kubelet's ClusterTrustBundle manager,
//! `pkg/kubelet/clustertrustbundle/clustertrustbundle_manager.go`
//! (`GetTrustAnchorsByName` `:198-227`, `GetTrustAnchorsBySigner` `:229-283`,
//! `normalizeTrustAnchors` `:285-312`), as consumed by the projected volume's
//! `ClusterTrustBundle` source (`pkg/volume/projected/projected.go:320-355`).
//!
//! Deviations, all deliberate:
//!
//! * Reads straight from storage on every call instead of an informer lister
//!   behind a TTL cache (`normalizationCache`, 5 minutes). The projected
//!   volume's periodic re-SetUp is the only caller, and the AtomicWriter makes
//!   an unchanged result inert; there is no "informer has not yet synced" state.
//! * `normalizeTrustAnchors` shuffles the sorted anchors with `rand.Shuffle`
//!   (`:296-299`) — "a stable ordering that changes each time Kubelet
//!   restarts" per its comment — but the cache is what keeps that stable
//!   within a run. With no cache, the order is instead a fixed pseudo-random
//!   permutation seeded once per process, which is what the comment describes.
//! * ClusterTrustBundles are read as untyped JSON: `rusternetes-common` has no
//!   ClusterTrustBundle type yet (see the follow-up issue).

use anyhow::{anyhow, Result};
use rusternetes_common::types::{label_selector_as_selector, LabelSelector};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::{BTreeSet, HashMap};
use std::hash::BuildHasher;
use std::sync::OnceLock;

/// `maxLabelSelectorLength` (`clustertrustbundle_manager.go:45`).
const MAX_LABEL_SELECTOR_LENGTH: usize = 100 * 1024;

const RESOURCE: &str = "clustertrustbundles";

/// `GetTrustAnchorsByName` (`clustertrustbundle_manager.go:198-227`): the
/// normalized, deduplicated anchors of one named ClusterTrustBundle.
pub async fn get_trust_anchors_by_name<S: Storage + ?Sized>(
    storage: &S,
    name: &str,
    allow_missing: bool,
) -> Result<Vec<u8>> {
    let ctb: serde_json::Value = match storage.get(&build_key(RESOURCE, None, name)).await {
        Ok(v) => v,
        // `k8serrors.IsNotFound(err) && allowMissing` (`:208-211`).
        Err(rusternetes_common::Error::NotFound(_)) if allow_missing => return Ok(Vec::new()),
        Err(e) => return Err(anyhow!("while getting ClusterTrustBundle: {e}")),
    };
    Ok(normalize_trust_anchors(&[trust_bundle_of(&ctb)]))
}

/// `GetTrustAnchorsBySigner` (`clustertrustbundle_manager.go:229-283`): the
/// normalized, deduplicated anchors of every ClusterTrustBundle of
/// `signer_name` whose labels match `label_selector`. A nil selector matches
/// nothing, an empty one everything (`:236-237`).
pub async fn get_trust_anchors_by_signer<S: Storage + ?Sized>(
    storage: &S,
    signer_name: &str,
    label_selector: Option<&LabelSelector>,
    allow_missing: bool,
) -> Result<Vec<u8>> {
    let selector = label_selector_as_selector(label_selector)
        .map_err(|e| anyhow!("while parsing label selector: {e}"))?;
    // `cacheKey.labelSelector = selector.String()`; `len` is checked at `:245`.
    let selector_len = match label_selector {
        Some(ls) => ls.as_selector_string().map(|s| s.len()).unwrap_or(0),
        None => 0,
    };
    if selector_len > MAX_LABEL_SELECTOR_LENGTH {
        return Err(anyhow!(
            "label selector length ({selector_len}) is larger than {MAX_LABEL_SELECTOR_LENGTH}"
        ));
    }

    let all: Vec<serde_json::Value> =
        storage
            .list(&build_prefix(RESOURCE, None))
            .await
            .map_err(|e| {
                anyhow!(
                "while listing ClusterTrustBundles matching label selector {label_selector:?}: {e}"
            )
            })?;

    let bundles: Vec<String> = all
        .iter()
        .filter(|ctb| {
            let labels: HashMap<String, String> = ctb
                .pointer("/metadata/labels")
                .and_then(|l| serde_json::from_value(l.clone()).ok())
                .unwrap_or_default();
            selector.matches(Some(&labels))
                // `m.ctbHandlers.GetSignerName(ctb) == signerName` (`:260`).
                && ctb.pointer("/spec/signerName").and_then(|s| s.as_str()) == Some(signer_name)
        })
        .map(trust_bundle_of)
        .collect();

    if bundles.is_empty() {
        if allow_missing {
            return Ok(Vec::new());
        }
        return Err(anyhow!(
            "combination of signerName and labelSelector matched zero ClusterTrustBundles"
        ));
    }
    Ok(normalize_trust_anchors(&bundles))
}

fn trust_bundle_of(ctb: &serde_json::Value) -> String {
    ctb.pointer("/spec/trustBundle")
        .and_then(|t| t.as_str())
        .unwrap_or_default()
        .to_string()
}

/// `normalizeTrustAnchors` (`clustertrustbundle_manager.go:285-312`): decode
/// every PEM block of every bundle, deduplicate the block BYTES (not text),
/// order them, and re-encode each as a `CERTIFICATE` block (`pem.Encode` uses
/// LF line endings and 64-column lines).
fn normalize_trust_anchors(bundles: &[String]) -> Vec<u8> {
    let mut anchors: BTreeSet<Vec<u8>> = BTreeSet::new();
    for bundle in bundles {
        // `pem.Decode` stops at the first thing that is not a PEM block.
        if let Ok(blocks) = pem::parse_many(bundle) {
            for b in blocks {
                anchors.insert(b.into_contents());
            }
        }
    }

    // See the module docs: stable within a process, different across restarts.
    static SEED: OnceLock<std::collections::hash_map::RandomState> = OnceLock::new();
    let seed = SEED.get_or_init(std::collections::hash_map::RandomState::new);
    let mut anchors: Vec<Vec<u8>> = anchors.into_iter().collect();
    anchors.sort_by_cached_key(|a| seed.hash_one(a));

    let config = pem::EncodeConfig::new().set_line_ending(pem::LineEnding::LF);
    let mut out = Vec::new();
    for ta in anchors {
        out.extend_from_slice(
            pem::encode_config(&pem::Pem::new("CERTIFICATE", ta), config).as_bytes(),
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_storage::StorageBackend;

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
        let mut spec = serde_json::json!({"trustBundle": bundle});
        if !signer.is_empty() {
            spec["signerName"] = signer.into();
        }
        let ctb = serde_json::json!({
            "apiVersion": "certificates.k8s.io/v1beta1",
            "kind": "ClusterTrustBundle",
            "metadata": {"name": name, "labels": labels},
            "spec": spec
        });
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
