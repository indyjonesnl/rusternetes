//! ReferenceGrant validation.
//!
//! **Stated exception to the upstream-first rule (CLAUDE.md rule 8):**
//! `gateway.networking.k8s.io` is not a Kubernetes API; `../kubernetes` has no
//! validator for it. ReferenceGrant is a CRD owned by
//! `kubernetes-sigs/gateway-api`, and its *schema* is the contract the
//! api-server enforces:
//! `config/crd/standard/gateway.networking.k8s.io_referencegrants.yaml`
//! (v1.4.0, version `v1beta1`). Rusternetes serves it natively, so nothing
//! else applies the schema. The schema keywords ported:
//!
//! * `spec.from` / `spec.to`: `required`, `minItems: 1`, `maxItems: 16`;
//! * `from[].group` / `to[].group`: `maxLength: 253`, pattern
//!   `^$|^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`;
//! * `from[].kind` / `to[].kind`: `minLength: 1`, `maxLength: 63`, pattern
//!   `^[a-zA-Z]([-a-zA-Z0-9]*[a-zA-Z0-9])?$`;
//! * `from[].namespace`: `minLength: 1`, `maxLength: 63`, pattern
//!   `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`;
//! * `to[].name`: `minLength: 1`, `maxLength: 253`.
//!
//! The error shapes follow the apiextensions-apiserver's
//! `kubeOpenAPIResultToFieldErrors`
//! (`staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/validation/validation.go:162-215`):
//! `TooLong` / `TooMany` for the max keywords, and `field.Invalid` carrying the
//! go-openapi message for `minItems`, `minLength` and `pattern`.
//!
//! Absent `from` / `to` / `group` / `kind` / `namespace` are `Required`
//! (`required:` in the CRD), distinct from an empty array or string; an empty
//! `group` is valid (the core group).

use crate::resources::volume::{ReferenceGrant, ReferenceGrantFrom, ReferenceGrantSpec};
use crate::validation::field::{Error, ErrorList, Path};
use once_cell::sync::Lazy;
use regex::Regex;

const GROUP_PATTERN: &str = r"^$|^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$";
const KIND_PATTERN: &str = r"^[a-zA-Z]([-a-zA-Z0-9]*[a-zA-Z0-9])?$";
const NAMESPACE_PATTERN: &str = r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$";

static GROUP_RE: Lazy<Regex> = Lazy::new(|| Regex::new(GROUP_PATTERN).expect("static regex"));
static KIND_RE: Lazy<Regex> = Lazy::new(|| Regex::new(KIND_PATTERN).expect("static regex"));
static NAMESPACE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(NAMESPACE_PATTERN).expect("static regex"));

const MAX_ITEMS: usize = 16;

/// Validate a `ReferenceGrant`'s spec against the CRD schema.
pub fn validate_reference_grant(grant: &ReferenceGrant) -> ErrorList {
    match &grant.spec {
        Some(spec) => validate_spec(spec, &Path::new("spec")),
        // `spec` is not in the top-level `required`.
        None => Vec::new(),
    }
}

/// go-openapi `required` -> `field.Required(errPath, "")`
/// (apiextensions validation.go `kubeOpenAPIResultToFieldErrors`).
fn required(p: &Path) -> Error {
    Error::required(p, "")
}

fn validate_spec(spec: &ReferenceGrantSpec, fld: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    let from = fld.child("from");
    match &spec.from {
        None => errs.push(required(&from)),
        Some(items) => {
            errs.extend(validate_item_count(items.len(), &from));
            for (i, f) in items.iter().enumerate() {
                errs.extend(validate_from(f, &from.index(i)));
            }
        }
    }

    let to = fld.child("to");
    match &spec.to {
        None => errs.push(required(&to)),
        Some(items) => {
            errs.extend(validate_item_count(items.len(), &to));
            for (i, t) in items.iter().enumerate() {
                let p = to.index(i);
                errs.extend(validate_group(t.group.as_deref(), &p.child("group")));
                errs.extend(validate_kind(t.kind.as_deref(), &p.child("kind")));
                if let Some(name) = t.name.as_deref() {
                    let np = p.child("name");
                    if name.is_empty() {
                        errs.push(too_short(&np, name, 1));
                    } else if name.len() > 253 {
                        errs.push(Error::too_long(&np, 253));
                    }
                }
            }
        }
    }
    errs
}

fn validate_item_count(len: usize, path: &Path) -> ErrorList {
    if len == 0 {
        vec![Error::invalid(
            path,
            "",
            format!("{path} in body should have at least 1 items"),
        )]
    } else if len > MAX_ITEMS {
        vec![Error::too_many(path, len, MAX_ITEMS)]
    } else {
        Vec::new()
    }
}

fn validate_from(from: &ReferenceGrantFrom, p: &Path) -> ErrorList {
    let mut errs = validate_group(from.group.as_deref(), &p.child("group"));
    errs.extend(validate_kind(from.kind.as_deref(), &p.child("kind")));
    let np = p.child("namespace");
    match from.namespace.as_deref() {
        None => errs.push(required(&np)),
        Some("") => errs.push(too_short(&np, "", 1)),
        Some(ns) if ns.len() > 63 => errs.push(Error::too_long(&np, 63)),
        Some(ns) if !NAMESPACE_RE.is_match(ns) => errs.push(pattern(&np, ns, NAMESPACE_PATTERN)),
        Some(_) => {}
    }
    errs
}

fn validate_group(group: Option<&str>, p: &Path) -> ErrorList {
    match group {
        None => vec![required(p)],
        Some(g) if g.len() > 253 => vec![Error::too_long(p, 253)],
        Some(g) if !GROUP_RE.is_match(g) => vec![pattern(p, g, GROUP_PATTERN)],
        Some(_) => Vec::new(),
    }
}

fn validate_kind(kind: Option<&str>, p: &Path) -> ErrorList {
    match kind {
        None => vec![required(p)],
        Some("") => vec![too_short(p, "", 1)],
        Some(k) if k.len() > 63 => vec![Error::too_long(p, 63)],
        Some(k) if !KIND_RE.is_match(k) => vec![pattern(p, k, KIND_PATTERN)],
        Some(_) => Vec::new(),
    }
}

/// go-openapi `errors.TooShort`: `"%s in %s should be at least %d chars long"`.
fn too_short(p: &Path, value: &str, min: usize) -> Error {
    Error::invalid(
        p,
        value,
        format!("{p} in body should be at least {min} chars long"),
    )
}

/// go-openapi `errors.FailedPattern`: `"%s in %s should match '%s'"`.
fn pattern(p: &Path, value: &str, pat: &str) -> Error {
    Error::invalid(p, value, format!("{p} in body should match '{pat}'"))
}
