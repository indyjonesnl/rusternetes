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
//! * ClusterTrustBundles are read as the typed
//!   `rusternetes_common::resources::ClusterTrustBundle` (certificates.k8s.io
//!   v1beta1). `m.ctbHandlers.GetSignerName(ctb)` (`:260`) is its
//!   `spec.signerName`, `GetTrustBundle` its `spec.trustBundle`.

use anyhow::{anyhow, Result};
use rusternetes_common::resources::ClusterTrustBundle;
use rusternetes_common::types::{label_selector_as_selector, LabelSelector};
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::collections::BTreeSet;
use std::hash::BuildHasher;
use std::sync::OnceLock;

/// `maxLabelSelectorLength` (`clustertrustbundle_manager.go:45`).
const MAX_LABEL_SELECTOR_LENGTH: usize = 100 * 1024;

pub const RESOURCE: &str = "clustertrustbundles";

/// `GetTrustAnchorsByName` (`clustertrustbundle_manager.go:198-227`): the
/// normalized, deduplicated anchors of one named ClusterTrustBundle.
pub async fn get_trust_anchors_by_name<S: Storage + ?Sized>(
    storage: &S,
    name: &str,
    allow_missing: bool,
) -> Result<Vec<u8>> {
    let ctb: ClusterTrustBundle = match storage.get(&build_key(RESOURCE, None, name)).await {
        Ok(v) => v,
        // `k8serrors.IsNotFound(err) && allowMissing` (`:208-211`).
        Err(rusternetes_common::Error::NotFound(_)) if allow_missing => return Ok(Vec::new()),
        Err(e) => return Err(anyhow!("while getting ClusterTrustBundle: {e}")),
    };
    Ok(normalize_trust_anchors(&[ctb.spec.trust_bundle]))
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

    let all: Vec<ClusterTrustBundle> =
        storage
            .list(&build_prefix(RESOURCE, None))
            .await
            .map_err(|e| {
                anyhow!(
                "while listing ClusterTrustBundles matching label selector {label_selector:?}: {e}"
            )
            })?;

    let bundles: Vec<String> = all
        .into_iter()
        .filter(|ctb| {
            selector.matches(Some(&ctb.metadata.labels.clone().unwrap_or_default()))
                // `m.ctbHandlers.GetSignerName(ctb) == signerName` (`:260`).
                && ctb.spec.signer_name == signer_name
        })
        .map(|ctb| ctb.spec.trust_bundle)
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

/// `normalizeTrustAnchors` (`clustertrustbundle_manager.go:285-312`): decode
/// every PEM block of every bundle, deduplicate the block BYTES (not text),
/// order them, and re-encode each as a `CERTIFICATE` block (`pem.Encode` uses
/// LF line endings and 64-column lines).
pub fn normalize_trust_anchors(bundles: &[String]) -> Vec<u8> {
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
