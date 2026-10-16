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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::volume::ReferenceGrantTo;

    fn valid() -> ReferenceGrant {
        ReferenceGrant {
            spec: Some(ReferenceGrantSpec {
                from: Some(vec![ReferenceGrantFrom {
                    group: Some(String::new()),
                    kind: Some("PersistentVolumeClaim".into()),
                    namespace: Some("other".into()),
                }]),
                to: Some(vec![ReferenceGrantTo {
                    group: Some("snapshot.storage.k8s.io".into()),
                    kind: Some("VolumeSnapshot".into()),
                    name: None,
                }]),
            }),
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
        g.spec.as_mut().unwrap().to.as_mut().unwrap()[0].group = Some("Upper.example".into());
        assert_eq!(validate_reference_grant(&g).len(), 1);
    }

    #[test]
    fn name_must_not_be_empty_when_set() {
        let mut g = valid();
        g.spec.as_mut().unwrap().to.as_mut().unwrap()[0].name = Some(String::new());
        assert_eq!(validate_reference_grant(&g).len(), 1);
    }

    /// `validate.MaxItems` -> `field.TooMany(fldPath, len(value), max)`
    /// (`apimachinery/pkg/api/validate/limits.go:43`) carries the actual count.
    #[test]
    fn too_many_from_items_reports_actual_count() {
        let mut g = valid();
        let spec = g.spec.as_mut().unwrap();
        let item = spec.from.as_ref().unwrap()[0].clone();
        spec.from = Some(vec![item; 17]);
        let errs = validate_reference_grant(&g);
        assert_eq!(
            errs.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
            vec!["spec.from: Too many: 17: must have at most 16 items"]
        );
    }

    fn errs_of(spec: serde_json::Value) -> Vec<String> {
        let g: ReferenceGrant = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "g"},
            "spec": spec,
        }))
        .unwrap();
        validate_reference_grant(&g)
            .iter()
            .map(|e| e.to_string())
            .collect()
    }

    /// go-openapi `required` -> `field.Required(path, "")`
    /// (validation.go:177-178): an absent `from` / `to` is `Required`, not the
    /// `minItems` failure an empty array gets.
    #[test]
    fn absent_from_and_to_are_required() {
        assert_eq!(
            errs_of(serde_json::json!({})),
            vec!["spec.from: Required value", "spec.to: Required value"]
        );
        let empty = errs_of(serde_json::json!({"from": [], "to": []}));
        assert_eq!(empty.len(), 2);
        assert!(
            empty[0].contains("should have at least 1 items"),
            "{empty:?}"
        );
    }

    /// `required: [group, kind, namespace]` on `from[]`, `[group, kind]` on
    /// `to[]`: an absent key is `Required`; an empty `group` is present.
    #[test]
    fn absent_item_fields_are_required() {
        assert_eq!(
            errs_of(serde_json::json!({"from": [{}], "to": [{}]})),
            vec![
                "spec.from[0].group: Required value",
                "spec.from[0].kind: Required value",
                "spec.from[0].namespace: Required value",
                "spec.to[0].group: Required value",
                "spec.to[0].kind: Required value",
            ]
        );
        assert!(errs_of(serde_json::json!({
            "from": [{"group": "", "kind": "PersistentVolumeClaim", "namespace": "n"}],
            "to": [{"group": "", "kind": "Secret"}],
        }))
        .is_empty());
    }

    /// `spec` itself is not in the CRD's top-level `required`, so an object
    /// without one is valid.
    #[test]
    fn absent_spec_is_valid() {
        let g: ReferenceGrant =
            serde_json::from_value(serde_json::json!({"metadata": {"name": "g"}})).unwrap();
        assert!(validate_reference_grant(&g).is_empty());
        assert!(serde_json::to_value(&g).unwrap().get("spec").is_none());
    }
}
