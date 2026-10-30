//! Validation for `certificates.k8s.io` ClusterTrustBundle, ported from
//! `pkg/apis/certificates/validation/validation.go` (`ValidateClusterTrustBundle`
//! :482-499, `ValidateClusterTrustBundleUpdate` :503-517, `validateTrustBundle`
//! :522-587) and `pkg/apis/core/validation/names.go:114-129`
//! (`ValidateClusterTrustBundleName`).
use crate::resources::ClusterTrustBundle;
use crate::validation::certificatesigningrequest::validate_signer_name;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_immutable_field,
    validate_object_meta_accessor_with_opts_common, validate_object_meta_update,
};

/// `certificates.MaxTrustBundleSize` (pkg/apis/certificates/types.go:283).
pub const MAX_TRUST_BUNDLE_SIZE: usize = 1024 * 1024;

/// `ValidateClusterTrustBundleOptions` (validation.go).
#[derive(Debug, Clone, Copy, Default)]
pub struct ValidateClusterTrustBundleOptions {
    pub suppress_bundle_parsing: bool,
}

/// `ValidateClusterTrustBundleName` (names.go:114-129), as a closure over the
/// signer name.
fn validate_name(signer_name: &str, name: &str, prefix: bool) -> Vec<String> {
    if signer_name.is_empty() {
        if name.contains(':') {
            return vec![
                "ClusterTrustBundle without signer name must not have \":\" in its name"
                    .to_string(),
            ];
        }
        return name_is_dns_subdomain(name, prefix);
    }
    let required_prefix = format!("{}:", signer_name.replace('/', ":"));
    match name.strip_prefix(&required_prefix) {
        None => vec![format!(
            "ClusterTrustBundle for signerName {signer_name} must be named with prefix {required_prefix}"
        )],
        Some(rest) => name_is_dns_subdomain(rest, prefix),
    }
}

/// `ValidateClusterTrustBundle` (validation.go:482-499).
pub fn validate_cluster_trust_bundle(
    bundle: &ClusterTrustBundle,
    opts: ValidateClusterTrustBundleOptions,
) -> ErrorList {
    let signer_name = bundle.spec.signer_name.as_str();
    let meta_path = Path::new("metadata");
    let meta = &bundle.metadata;

    // `ValidateObjectMeta` with `ValidateClusterTrustBundleName(signerName)`:
    // that name function closes over the signer, which `ValidateNameFunc`
    // (a plain `fn`) cannot, so the name checks are inlined in the same
    // order and the shared remainder is reused.
    let mut errs = ErrorList::new();
    if let Some(gn) = meta.generate_name.as_deref().filter(|g| !g.is_empty()) {
        for msg in validate_name(signer_name, gn, true) {
            errs.push(Error::invalid(&meta_path.child("generateName"), gn, msg));
        }
    }
    if meta.name.is_empty() {
        errs.push(Error::required(
            &meta_path.child("name"),
            "name or generateName is required",
        ));
    } else {
        for msg in validate_name(signer_name, &meta.name, false) {
            errs.push(Error::invalid(
                &meta_path.child("name"),
                meta.name.clone(),
                msg,
            ));
        }
    }
    errs.extend(validate_object_meta_accessor_with_opts_common(
        meta, false, &meta_path,
    ));

    if !signer_name.is_empty() {
        errs.extend(validate_signer_name(
            &Path::new("spec").child("signerName"),
            signer_name,
        ));
    }

    if !opts.suppress_bundle_parsing {
        errs.extend(validate_trust_bundle(
            &Path::new("spec").child("trustBundle"),
            &bundle.spec.trust_bundle,
        ));
    }
    errs
}

/// `ValidateClusterTrustBundleUpdate` (validation.go:503-517).
pub fn validate_cluster_trust_bundle_update(
    new_bundle: &ClusterTrustBundle,
    old_bundle: &ClusterTrustBundle,
) -> ErrorList {
    // If the caller isn't changing the TrustBundle field, don't parse it
    // (validation.go:505-510).
    let opts = ValidateClusterTrustBundleOptions {
        suppress_bundle_parsing: new_bundle.spec.trust_bundle == old_bundle.spec.trust_bundle,
    };
    let mut errs = validate_cluster_trust_bundle(new_bundle, opts);
    errs.extend(validate_object_meta_update(
        &new_bundle.metadata,
        &old_bundle.metadata,
        &Path::new("metadata"),
    ));
    errs.extend(validate_immutable_field(
        &new_bundle.spec.signer_name,
        &old_bundle.spec.signer_name,
        &Path::new("spec").child("signerName"),
    ));
    errs
}

/// `validateTrustBundle` (validation.go:522-587): rejects intra-block headers,
/// blocks that don't parse as X.509 CA certificates, and duplicate trust
/// anchors, and requires at least one trust anchor.
fn validate_trust_bundle(path: &Path, input: &str) -> ErrorList {
    let mut errs = ErrorList::new();

    if input.len() > MAX_TRUST_BUNDLE_SIZE {
        errs.push(Error::too_long(path, MAX_TRUST_BUNDLE_SIZE));
        return errs;
    }

    // Go keys a map by the DER bytes and iterates it in random order; a
    // BTreeMap keeps the duplicate errors deterministic.
    let mut block_dedupe: std::collections::BTreeMap<Vec<u8>, Vec<usize>> = Default::default();

    let mut rest = input.as_bytes();
    let mut idx = 0usize;
    while let Some((block, next)) = pem_decode(rest) {
        rest = next;
        let i = idx;
        idx += 1;

        if block.ty != "CERTIFICATE" {
            errs.push(Error::invalid(
                path,
                "<value omitted>",
                format!("entry {i} has bad block type: {}", block.ty),
            ));
            continue;
        }
        if block.has_headers {
            errs.push(Error::invalid(
                path,
                "<value omitted>",
                format!("entry {i} has PEM block headers"),
            ));
            continue;
        }
        // `x509.ParseCertificate` also rejects trailing data.
        let parsed = match x509_parser::parse_x509_certificate(&block.bytes) {
            Ok(([], cert)) => cert,
            _ => {
                errs.push(Error::invalid(
                    path,
                    "<value omitted>",
                    format!("entry {i} does not parse as X.509"),
                ));
                continue;
            }
        };
        // Go's `IsCA` is set only from a present basic-constraints extension,
        // so a certificate without one has the CA bit unset.
        let basic_constraints = parsed.basic_constraints().ok().flatten();
        if !basic_constraints.is_some_and(|bc| bc.value.ca) {
            errs.push(Error::invalid(
                path,
                "<value omitted>",
                format!("entry {i} does not have the CA bit set"),
            ));
            continue;
        }
        block_dedupe.entry(block.bytes).or_default().push(i);
    }

    // If we had a malformed block, don't also output potentially-redundant
    // errors about duplicate or missing trust anchors.
    if !errs.is_empty() {
        return errs;
    }

    if block_dedupe.is_empty() {
        errs.push(Error::invalid(
            path,
            "<value omitted>",
            "at least one trust anchor must be provided",
        ));
    }
    for indices in block_dedupe.values() {
        if indices.len() > 1 {
            // Go's `%v` of a `[]int`: `[0 1]`.
            let joined: Vec<String> = indices.iter().map(|i| i.to_string()).collect();
            errs.push(Error::invalid(
                path,
                "<value omitted>",
                format!("duplicate trust anchor (indices [{}])", joined.join(" ")),
            ));
        }
    }
    errs
}

/// A PEM block, as `encoding/pem.Block`; only whether it carried headers
/// matters here.
struct PemBlock {
    ty: String,
    has_headers: bool,
    bytes: Vec<u8>,
}

/// `getLine` (encoding/pem/pem.go:36-50): the first `\r\n`- or `\n`-delimited
/// line, trailing spaces and tabs trimmed, and the rest.
fn get_line(data: &[u8]) -> (&[u8], &[u8]) {
    let (i, j) = match data.iter().position(|&b| b == b'\n') {
        None => (data.len(), data.len()),
        Some(i) => (
            if i > 0 && data[i - 1] == b'\r' {
                i - 1
            } else {
                i
            },
            i + 1,
        ),
    };
    let mut line = &data[..i];
    while let Some((&last, init)) = line.split_last() {
        if last == b' ' || last == b'\t' {
            line = init;
        } else {
            break;
        }
    }
    (line, &data[j..])
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// `pem.Decode` (encoding/pem/pem.go:98-190): the next PEM block in `data`
/// and the remainder, skipping any text that is not a well-formed block.
fn pem_decode(data: &[u8]) -> Option<(PemBlock, &[u8])> {
    use base64::Engine;
    const START: &[u8] = b"\n-----BEGIN ";
    const END: &[u8] = b"\n-----END ";
    const END_OF_LINE: &[u8] = b"-----";

    let mut rest = data;
    loop {
        if rest.starts_with(&START[1..]) {
            rest = &rest[START.len() - 1..];
        } else {
            let at = find(rest, START)?;
            rest = &rest[at + START.len()..];
        }

        let (type_line, after) = get_line(rest);
        rest = after;
        if !type_line.ends_with(END_OF_LINE) {
            continue;
        }
        let type_line = &type_line[..type_line.len() - END_OF_LINE.len()];
        let mut has_headers = false;

        loop {
            if rest.is_empty() {
                return None;
            }
            let (line, next) = get_line(rest);
            if !line.contains(&b':') {
                break;
            }
            // TODO(agl) upstream: values that spread across lines.
            has_headers = true;
            rest = next;
        }

        // If there were no headers, the END line might occur immediately,
        // without a leading newline.
        let (end_index, end_trailer_index) = if !has_headers && rest.starts_with(&END[1..]) {
            (0, END.len() - 1)
        } else if let Some(at) = find(rest, END) {
            (at, at + END.len())
        } else {
            continue;
        };

        // After the "-----" of the ending line, there should be the same type
        // and then a final five dashes.
        let end_trailer = &rest[end_trailer_index..];
        let end_trailer_len = type_line.len() + END_OF_LINE.len();
        if end_trailer.len() < end_trailer_len {
            continue;
        }
        let rest_of_end_line = &end_trailer[end_trailer_len..];
        let end_trailer = &end_trailer[..end_trailer_len];
        if !end_trailer.starts_with(type_line) || !end_trailer.ends_with(END_OF_LINE) {
            continue;
        }
        // The line must end with only whitespace.
        if !get_line(rest_of_end_line).0.is_empty() {
            continue;
        }

        // `removeSpacesAndTabs`; the base64 decoder skips newlines.
        let b64: Vec<u8> = rest[..end_index]
            .iter()
            .copied()
            .filter(|b| !matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
            .collect();
        let engine = base64::engine::GeneralPurpose::new(
            &base64::alphabet::STANDARD,
            base64::engine::GeneralPurposeConfig::new().with_decode_allow_trailing_bits(true),
        );
        let Ok(bytes) = engine.decode(&b64) else {
            continue;
        };

        // the -1 is because we might have only matched pemEnd without the
        // leading newline if the PEM block was empty.
        let (_, remainder) = get_line(&rest[end_index + END.len() - 1..]);
        return Some((
            PemBlock {
                ty: String::from_utf8_lossy(type_line).into_owned(),
                has_headers,
                bytes,
            },
            remainder,
        ));
    }
}
