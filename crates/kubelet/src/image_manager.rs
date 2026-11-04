//! Image manager error taxonomy and pure helpers.
//!
//! Ports the parts of `pkg/kubelet/images/image_manager.go` that do not need
//! a runtime: `applyDefaultImageTag` (`:393-410`), `evalCRIPullErr`
//! (`:361-391`), the sentinel errors of `images/types.go:26-38`, and the
//! user-facing message strings of `imagePullPrecheck` /
//! `imageNotPresentOnNeverPolicyError` / `EnsureImageExists`.
//!
//! The kubelet turns the returned error into the container's
//! `waiting.reason` (`kubelet_pods.go` `convertToAPIContainerStatuses`:
//! `err.Error()` is the reason, the message is the detail), so each kind's
//! [`ImageErrorKind::as_str`] is that reason.
//!
//! Not ported: `KubeletEnsureSecretPulledImages` credential verification
//! (`pullmanager`), the pull back-off (`ErrImagePullBackOff`), and the
//! parallel/serial puller limits (`puller.go`).

/// The sentinel errors of `images/types.go:26-38` (plus the CRI errors
/// `evalCRIPullErr` maps to, `cri-api/pkg/errors/errors.go:27-31`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageErrorKind {
    /// `ErrImagePull`: general image pull error.
    ImagePull,
    /// `ErrImageInspect`: unable to inspect image.
    ImageInspect,
    /// `ErrInvalidImageName`: unable to parse the image name.
    InvalidImageName,
    /// `crierrors.ErrRegistryUnavailable`.
    RegistryUnavailable,
    /// `crierrors.ErrSignatureValidationFailed`.
    SignatureValidationFailed,
}

impl ImageErrorKind {
    /// `err.Error()` of the upstream sentinel: the waiting reason.
    pub fn as_str(self) -> &'static str {
        match self {
            ImageErrorKind::ImagePull => "ErrImagePull",
            ImageErrorKind::ImageInspect => "ImageInspectError",
            ImageErrorKind::InvalidImageName => "InvalidImageName",
            ImageErrorKind::RegistryUnavailable => "RegistryUnavailable",
            ImageErrorKind::SignatureValidationFailed => "SignatureValidationFailed",
        }
    }
}

/// An `EnsureImageExists` failure: the sentinel (`kind`) and the message
/// upstream returns beside it. `Display` is the message; the reason is
/// `kind.as_str()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageManagerError {
    pub kind: ImageErrorKind,
    pub message: String,
}

impl std::fmt::Display for ImageManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ImageManagerError {}

/// `imagePullPrecheck` (`image_manager.go:117-123`): `ImageStatus` failed.
pub fn failed_to_inspect_message(image_ref: &str, err: &str) -> String {
    format!("Failed to inspect image {image_ref:?}: {err}")
}

/// `EnsureImageExists` (`image_manager.go:171`): the default tag could not
/// be applied.
pub fn failed_default_tag_message(requested: &str, err: &str) -> String {
    format!("Failed to apply default image tag {requested:?}: {err}")
}

/// `imageNotPresentOnNeverPolicyError` (`image_manager.go:155`).
pub fn never_pull_message(requested: &str) -> String {
    format!("Container image {requested:?} is not present with pull policy of Never")
}

/// `EnsureImageExists` with the gate off (`image_manager.go:241`): the image
/// is present and no pull is needed.
pub fn already_present_message(requested: &str) -> String {
    format!("Container image {requested:?} already present on machine")
}

/// `evalCRIPullErr` (`image_manager.go:361-391`). `err` is the message of the
/// gRPC status (`remote_image.go:208-211` strips the code), so the CRI
/// sentinels are recognised by prefix.
pub fn eval_cri_pull_err(img_ref: &str, err: &str) -> (String, ImageErrorKind) {
    let reg = ImageErrorKind::RegistryUnavailable.as_str();
    if let Some(rest) = err.strip_prefix(reg) {
        return (
            format!("image pull failed for {img_ref} because the registry is unavailable{rest}"),
            ImageErrorKind::RegistryUnavailable,
        );
    }
    let sig = ImageErrorKind::SignatureValidationFailed.as_str();
    if let Some(rest) = err.strip_prefix(sig) {
        return (
            format!(
                "image pull failed for {img_ref} because the signature validation failed{rest}"
            ),
            ImageErrorKind::SignatureValidationFailed,
        );
    }
    (err.to_string(), ImageErrorKind::ImagePull)
}

/// The tag/digest facts `parsers.ParseImageName` reports.
struct ParsedImage {
    tag: String,
    digest: String,
}

fn is_word(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// `[a-z0-9]+(?:(?:[._]|__|[-]+)[a-z0-9]+)*` (`distribution/reference`
/// `pathComponentRegexp`).
fn valid_path_component(s: &str) -> bool {
    let lower = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
    let b: Vec<char> = s.chars().collect();
    if b.is_empty() || !lower(b[0]) || !lower(*b.last().unwrap()) {
        return false;
    }
    let mut i = 0;
    while i < b.len() {
        if lower(b[i]) {
            i += 1;
            continue;
        }
        // A separator run: `.`, `_`, `__` or `-+`, always between alnum runs.
        let start = i;
        while i < b.len() && !lower(b[i]) {
            i += 1;
        }
        let sep: String = b[start..i].iter().collect();
        let ok = sep == "." || sep == "_" || sep == "__" || sep.chars().all(|c| c == '-');
        if !ok {
            return false;
        }
    }
    true
}

/// `domainNameComponent(\.domainNameComponent)*(:[0-9]+)?` with
/// `[a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]` components; an IPv6
/// `[..]` host is accepted loosely.
fn valid_domain(s: &str) -> bool {
    let host = match s.rsplit_once(':') {
        Some((h, port)) if !s.ends_with(']') => {
            if port.is_empty() || !port.chars().all(|c| c.is_ascii_digit()) {
                return false;
            }
            h
        }
        _ => s,
    };
    if host.starts_with('[') && host.ends_with(']') {
        return true;
    }
    !host.is_empty()
        && host.split('.').all(|c| {
            !c.is_empty()
                && c.chars().all(|x| x.is_ascii_alphanumeric() || x == '-')
                && !c.starts_with('-')
                && !c.ends_with('-')
        })
}

/// `reference.ParseNormalizedNamed` grammar, reduced to the facts
/// `ParseImageName` returns (`pkg/util/parsers/parsers.go:31-52`).
fn parse_image_name(image: &str) -> Result<ParsedImage, String> {
    let invalid = || format!("couldn't parse image name {image:?}: invalid reference format");
    if image.is_empty() {
        return Err(invalid());
    }
    let (rest, digest) = match image.split_once('@') {
        Some((r, d)) => (r, d),
        None => (image, ""),
    };
    if !digest.is_empty() {
        let (algo, hex) = digest.split_once(':').ok_or_else(invalid)?;
        let algo_ok = !algo.is_empty()
            && algo.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
            && algo
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "-_+.".contains(c));
        if !algo_ok || hex.len() < 32 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(invalid());
        }
    } else if image.contains('@') {
        return Err(invalid());
    }
    let last_slash = rest.rfind('/').map_or(0, |i| i + 1);
    let (name, tag) = match rest[last_slash..].rfind(':') {
        Some(i) => (&rest[..last_slash + i], &rest[last_slash + i + 1..]),
        None => (rest, ""),
    };
    if rest[last_slash..].contains(':') {
        let tag_ok = !tag.is_empty()
            && tag.len() <= 128
            && tag.chars().next().is_some_and(is_word)
            && tag.chars().all(|c| is_word(c) || c == '.' || c == '-');
        if !tag_ok {
            return Err(invalid());
        }
    }
    if name.is_empty() {
        return Err(invalid());
    }
    // `splitDockerDomain`: the first component is a domain only when it looks
    // like one.
    let remainder = match name.split_once('/') {
        Some((d, r)) if d.contains(['.', ':']) || d == "localhost" || d.to_lowercase() != d => {
            if !valid_domain(d) {
                return Err(invalid());
            }
            r
        }
        _ => name,
    };
    // `NameTotalLengthMax` is 255 on the normalised name.
    if name.len() > 255 || !remainder.split('/').all(valid_path_component) {
        return Err(invalid());
    }
    let tagged = rest[last_slash..].contains(':');
    let tag = if tagged {
        tag.to_string()
    } else if digest.is_empty() {
        "latest".to_string()
    } else {
        String::new()
    };
    Ok(ParsedImage {
        tag,
        digest: digest.to_string(),
    })
}

/// `applyDefaultImageTag` (`image_manager.go:393-410`): append `:latest` to an
/// image with no tag and no digest, without normalising the name.
pub fn apply_default_image_tag(image: &str) -> Result<String, String> {
    let p = parse_image_name(image)?;
    if p.digest.is_empty() && !p.tag.is_empty() && !image.ends_with(&format!(":{}", p.tag)) {
        return Ok(format!("{image}:{}", p.tag));
    }
    Ok(image.to_string())
}

/// Go `time.Duration.String()` for a duration already truncated to
/// milliseconds (`%v` of `Duration.Truncate(time.Millisecond)`).
pub fn go_duration_string(d: std::time::Duration) -> String {
    let ms = d.as_millis() as u64;
    if ms == 0 {
        return "0s".to_string();
    }
    if ms < 1000 {
        return format!("{ms}ms");
    }
    let (h, m, s_ms) = (ms / 3_600_000, (ms / 60_000) % 60, ms % 60_000);
    let secs = if s_ms % 1000 == 0 {
        format!("{}", s_ms / 1000)
    } else {
        format!("{:.3}", s_ms as f64 / 1000.0)
            .trim_end_matches('0')
            .to_string()
    };
    match (h, m) {
        (0, 0) => format!("{secs}s"),
        (0, m) => format!("{m}m{secs}s"),
        (h, m) => format!("{h}h{m}m{secs}s"),
    }
}
