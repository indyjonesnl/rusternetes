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
//! Deviation: the typed struct decodes an absent `from` / `to` / `group` /
//! `kind` / `namespace` to its empty default, so an absent array is reported
//! as the `minItems` failure rather than `Required`, and an absent `kind` or
//! `namespace` as the `minLength` failure. An empty `group` is valid
//! (the core group).

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
    validate_spec(&grant.spec, &Path::new("spec"))
}

fn validate_spec(spec: &ReferenceGrantSpec, fld: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    let from = fld.child("from");
    errs.extend(validate_item_count(spec.from.len(), &from));
    for (i, f) in spec.from.iter().enumerate() {
        errs.extend(validate_from(f, &from.index(i)));
    }

    let to = fld.child("to");
    errs.extend(validate_item_count(spec.to.len(), &to));
    for (i, t) in spec.to.iter().enumerate() {
        let p = to.index(i);
        errs.extend(validate_group(&t.group, &p.child("group")));
        errs.extend(validate_kind(&t.kind, &p.child("kind")));
        if let Some(name) = t.name.as_deref() {
            let np = p.child("name");
            if name.is_empty() {
                errs.push(too_short(&np, name, 1));
            } else if name.len() > 253 {
                errs.push(Error::too_long(&np, 253));
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
    let mut errs = validate_group(&from.group, &p.child("group"));
    errs.extend(validate_kind(&from.kind, &p.child("kind")));
    let np = p.child("namespace");
    if from.namespace.is_empty() {
        errs.push(too_short(&np, &from.namespace, 1));
    } else if from.namespace.len() > 63 {
        errs.push(Error::too_long(&np, 63));
    } else if !NAMESPACE_RE.is_match(&from.namespace) {
        errs.push(pattern(&np, &from.namespace, NAMESPACE_PATTERN));
    }
    errs
}

fn validate_group(group: &str, p: &Path) -> ErrorList {
    if group.len() > 253 {
        vec![Error::too_long(p, 253)]
    } else if !GROUP_RE.is_match(group) {
        vec![pattern(p, group, GROUP_PATTERN)]
    } else {
        Vec::new()
    }
}

fn validate_kind(kind: &str, p: &Path) -> ErrorList {
    if kind.is_empty() {
        vec![too_short(p, kind, 1)]
    } else if kind.len() > 63 {
        vec![Error::too_long(p, 63)]
    } else if !KIND_RE.is_match(kind) {
        vec![pattern(p, kind, KIND_PATTERN)]
    } else {
        Vec::new()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::volume::ReferenceGrantTo;

    fn valid() -> ReferenceGrant {
        ReferenceGrant {
            spec: ReferenceGrantSpec {
                from: vec![ReferenceGrantFrom {
                    group: String::new(),
                    kind: "PersistentVolumeClaim".into(),
                    namespace: "other".into(),
                }],
                to: vec![ReferenceGrantTo {
                    group: "snapshot.storage.k8s.io".into(),
                    kind: "VolumeSnapshot".into(),
                    name: None,
                }],
            },
            ..Default::default()
        }
    }

    #[test]
    fn a_valid_grant_passes() {
        assert!(validate_reference_grant(&valid()).is_empty());
    }

    #[test]
    fn an_empty_core_group_is_valid_but_uppercase_is_not() {
        let mut g = valid();
        g.spec.to[0].group = "Upper.example".into();
        assert_eq!(validate_reference_grant(&g).len(), 1);
    }

    #[test]
    fn name_must_not_be_empty_when_set() {
        let mut g = valid();
        g.spec.to[0].name = Some(String::new());
        assert_eq!(validate_reference_grant(&g).len(), 1);
    }

    /// `validate.MaxItems` -> `field.TooMany(fldPath, len(value), max)`
    /// (`apimachinery/pkg/api/validate/limits.go:43`) carries the actual count.
    #[test]
    fn too_many_from_items_reports_actual_count() {
        let mut g = valid();
        let item = g.spec.from[0].clone();
        g.spec.from = vec![item; 17];
        let errs = validate_reference_grant(&g);
        assert_eq!(
            errs.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
            vec!["spec.from: Too many: 17: must have at most 16 items"]
        );
    }
}
