//! Guard: no IntOrString field may be typed as a Rust `String`.
//!
//! Upstream carries an int-or-string value as a struct with a `Type`
//! discriminator (`apimachinery/pkg/util/intstr/intstr.go`), and encodes it by
//! that discriminator: `Int` marshals as a JSON **number**, `String` as a JSON
//! **string**. Storing either form in a Rust `String` erases the discriminator,
//! and serialization then quotes what should have been a number.
//!
//! That is not cosmetic. `intstr.UnmarshalJSON` assigns `Type = String` to
//! anything quoted, so a Go client reading our `"maxUnavailable": "1"` gets a
//! String, and `GetScaledValueFromIntOrPercent` rejects it:
//!
//!   invalid value for IntOrString: invalid type: string is not a percentage
//!
//! Which is exactly how the real DaemonSet controller stopped rolling out
//! kube-proxy in the vanilla-swap api-server leg.
//!
//! **No allowlist, deliberately.** The lossy form shipped on three fields and
//! stayed there through several DaemonSet fixes because nothing measured it. A
//! field that cannot use `IntOrString` is a reason to change the field, not to
//! record an exception here.
//!
//! `serde_json::Value` is *not* flagged: it round-trips a number as a number,
//! so it is imprecise (it also accepts `true`, `[]`, `{}`) but not lossy.
//! Tightening those onto `IntOrString` is tracked separately.

use std::path::{Path, PathBuf};

/// Field names that upstream declares as `intstr.IntOrString`, derived from
/// every `types.go` under `staging/src/k8s.io/api` and `pkg/apis`:
///
/// ```text
/// $ grep -rn "intstr.IntOrString" --include=types.go staging/src/k8s.io/api/ pkg/apis/ \
///     | sed -E 's/.*:\s*([A-Za-z]+)\s+\*?intstr\.IntOrString.*/\1/' | sort -u
/// MaxSurge  MaxUnavailable  MinAvailable  Port  ServicePort  TargetPort
/// ```
///
/// `Port` is omitted from this list on purpose: plenty of unrelated fields are
/// also called `port` and are legitimately `i32` (`ContainerPort.container_port`,
/// `ServicePort.port`, `EndpointPort.port`). Those are caught by the second
/// channel below instead — an explicit `IntOrString` marker in the source.
const INTORSTRING_FIELDS: &[&str] = &[
    "max_surge",
    "max_unavailable",
    "min_available",
    "service_port",
    "target_port",
];

/// The lossy shapes: a bare `String`, or an `Option<String>`.
///
/// A trailing `// ...` comment is stripped first. The lossy fields this guard
/// exists for were all annotated `// IntOrString`, so leaving the comment in
/// the parsed type made the scan miss precisely the declarations it was
/// written to catch.
fn is_string_typed(ty: &str) -> bool {
    let ty = ty
        .split("//")
        .next()
        .unwrap_or("")
        .trim()
        .trim_end_matches(',');
    ty == "String" || ty == "Option<String>"
}

/// Whether a field declaration refers to an upstream `IntOrString` value.
///
/// Two channels, because neither alone is sufficient: the name list covers the
/// fields upstream declares as `intstr.IntOrString`, and the source marker
/// covers everything else (notably `port`, whose name is too common to key on).
fn is_intorstring_field(field: &str, line: &str) -> bool {
    INTORSTRING_FIELDS.contains(&field) || line.contains("IntOrString")
}

fn resource_files() -> Vec<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/resources");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("resources dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "rs"))
        .collect();
    // include resources.rs itself if types live there too
    let top = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/resources.rs");
    if top.exists() {
        files.push(top);
    }
    files.sort();
    files
}

/// Split `pub foo: Bar,` into `("foo", "Bar")`, ignoring anything that is not a
/// field declaration.
fn field_decl(line: &str) -> Option<(&str, &str)> {
    let rest = line.trim().strip_prefix("pub ")?;
    let (name, ty) = rest.split_once(':')?;
    if name.contains('(') || name.contains(' ') {
        return None; // fn / tuple struct / etc.
    }
    Some((name.trim(), ty.trim()))
}

#[test]
fn no_intorstring_field_is_string_typed() {
    let mut offenders = Vec::new();

    for path in resource_files() {
        let src = std::fs::read_to_string(&path).expect("read source");
        let name = path.file_name().unwrap().to_string_lossy().to_string();

        for (n, line) in src.lines().enumerate() {
            let Some((field, ty)) = field_decl(line) else {
                continue;
            };
            if !is_string_typed(ty) {
                continue;
            }

            if is_intorstring_field(field, line) {
                offenders.push(format!(
                    "{name}:{} — `{field}: {ty}` is an IntOrString field held as a String; \
                     use `IntOrString` so an integer serializes as a JSON number",
                    n + 1
                ));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "IntOrString fields must not be String-typed — a quoted integer breaks \
         every Go client that calls GetScaledValueFromIntOrPercent:\n  {}",
        offenders.join("\n  ")
    );
}

/// The helper that produced the lossy form must stay gone. It took a JSON
/// number and returned `Option<String>`, so every caller silently lost the
/// discriminator; reintroducing it would reopen the whole class.
#[test]
fn the_lossy_int_or_string_deserializer_is_not_reintroduced() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut hits = Vec::new();
    let mut stack = vec![dir];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).expect("read dir").flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|e| e == "rs") {
                let src = std::fs::read_to_string(&p).expect("read source");
                if src.contains("deserialize_int_or_string_opt") {
                    hits.push(p.display().to_string());
                }
            }
        }
    }
    assert!(
        hits.is_empty(),
        "`deserialize_int_or_string_opt` normalised a JSON number into a String \
         and was deleted; decode into `IntOrString` instead:\n  {}",
        hits.join("\n  ")
    );
}

/// The guard above is a source scan, so a green run proves nothing unless the
/// detector itself is known to fire. These cases pin both channels and both
/// directions; without them the scan could silently match nothing and pass.
#[test]
fn the_detector_flags_the_lossy_shape_and_only_that() {
    let flagged = |line: &str| {
        let (field, ty) = field_decl(line).expect("parses as a field");
        is_string_typed(ty) && is_intorstring_field(field, line)
    };

    // The exact shape this change removed — caught by name.
    assert!(flagged("    pub max_surge: Option<String>,"));
    assert!(flagged(
        "    pub max_unavailable: Option<String>, // IntOrString"
    ));
    assert!(flagged("    pub target_port: String,"));
    // Caught by the source marker, where the name alone is too common.
    assert!(flagged("    pub port: Option<String>, // IntOrString"));

    // The fixed shape.
    assert!(!flagged("    pub max_surge: Option<IntOrString>,"));
    // Legitimately numeric ports.
    assert!(!flagged("    pub port: i32,"));
    assert!(!flagged("    pub container_port: i32,"));
    // Lossless, deliberately out of scope.
    assert!(!flagged(
        "    pub max_surge: Option<serde_json::Value>, // IntOrString"
    ));
    // An ordinary string field must never be flagged.
    assert!(!flagged("    pub name: Option<String>,"));
    assert!(!flagged("    pub service_name: String,"));

    // Non-field lines must not parse as fields at all.
    assert!(field_decl("pub fn to_json(&self) -> serde_json::Value {").is_none());
}
