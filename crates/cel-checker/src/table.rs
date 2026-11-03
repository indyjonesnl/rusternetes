//! Function declarations: the cel-go standard library
//! (`common/stdlib/standard.go`) plus the `ext` and Kubernetes library
//! overloads of the environment a DRA device selector is compiled in.
//!
//! Upstream builds that environment with
//! `environment.MustBaseEnvSet(DefaultCompatibilityVersion())`
//! (`staging/src/k8s.io/dynamic-resource-allocation/cel/compile.go:302`,
//! `newCompiler`): the standard library plus the libraries of
//! `staging/src/k8s.io/apiserver/pkg/cel/environment/base.go:60-207`:
//! `ext.Strings(2)`, `ext.Sets()`, `ext.Lists(3)`, `cel.OptionalTypes()`,
//! `cel.CrossTypeNumericComparisons(true)` and the Kubernetes `URLs`, `Regex`,
//! `Lists`, `Authz`, `AuthzSelectors`, `Quantity`, `IP`, `CIDR`, `Format` and
//! `SemverLib` libraries, plus `ext.Bindings` (`compile.go:337`). Not in that
//! environment, hence undeclared here as upstream: `ext.Math()` (`math.*`),
//! `ext.Encoders()` (`base64.*`), `ext.Protos()`.
//!
//! Splitting the table into per-library declarations with versioned
//! environments is a later slice of #3440.

use crate::types::{DType, Overload};
use std::collections::HashMap;
use std::sync::OnceLock;

/// A tiny parser for the type syntax used by the table below: `bool`, `int`,
/// `uint`, `double`, `string`, `bytes`, `dyn`, `null`, `timestamp`,
/// `duration`, a single capital letter (type parameter), `list(T)`,
/// `map(K, V)`, `type(T)`, `optional(T)`, `s:Name` (object type) and
/// `o:Name` (opaque type).
fn parse_type(s: &str) -> DType {
    let s = s.trim();
    if let Some(n) = s.strip_prefix("s:") {
        return DType::Struct(n.to_string());
    }
    if let Some(n) = s.strip_prefix("o:") {
        return DType::Opaque(n.to_string(), vec![]);
    }
    if let Some(open) = s.find('(') {
        let inner = &s[open + 1..s.len() - 1];
        let ps = split_top(inner)
            .iter()
            .map(|p| parse_type(p))
            .collect::<Vec<_>>();
        return match &s[..open] {
            "list" => DType::List(Box::new(ps[0].clone())),
            "map" => DType::Map(Box::new(ps[0].clone()), Box::new(ps[1].clone())),
            "type" => DType::Type(Some(Box::new(ps[0].clone()))),
            "optional" => DType::Opaque("optional_type".into(), ps),
            other => panic!("cel-checker table: unknown type constructor {other}"),
        };
    }
    match s {
        "bool" => DType::Bool,
        "int" => DType::Int,
        "uint" => DType::Uint,
        "double" => DType::Double,
        "string" => DType::Str,
        "bytes" => DType::Bytes,
        "dyn" => DType::Dyn,
        "null" => DType::Null,
        "timestamp" => DType::Timestamp,
        "duration" => DType::Duration,
        p if p.len() == 1 && p.chars().all(|c| c.is_ascii_uppercase()) => DType::Param(p.into()),
        other => panic!("cel-checker table: unknown type {other}"),
    }
}

/// Split on top-level commas.
fn split_top(s: &str) -> Vec<&str> {
    let (mut out, mut depth, mut start) = (Vec::new(), 0, 0);
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                out.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if !s[start..].trim().is_empty() {
        out.push(&s[start..]);
    }
    out
}

pub type Table = HashMap<String, Vec<Overload>>;

fn add(t: &mut Table, name: &str, member: bool, args: &str, result: &str) {
    let args: Vec<DType> = split_top(args).iter().map(|a| parse_type(a)).collect();
    let names = |a: &[DType]| a.iter().map(DType::name).collect::<Vec<_>>().join(",");
    let id = match (member, args.split_first()) {
        (true, Some((recv, rest))) => format!("{}.{name}({})", recv.name(), names(rest)),
        _ => format!("{name}({})", names(&args)),
    };
    let cel_id = crate::overload_ids::cel_go_overload_id(name, member, &args);
    t.entry(name.to_string()).or_default().push(Overload {
        id,
        cel_id,
        member,
        args,
        result: parse_type(result),
    });
}

/// The comparable / summable element types of the Kubernetes `Lists` library
/// (`apiserver/pkg/cel/library/lists.go:117-137`) and `ext.Lists`.
const COMPARABLE: [&str; 8] = [
    "int",
    "uint",
    "double",
    "bool",
    "duration",
    "timestamp",
    "string",
    "bytes",
];
const SUMMABLE: [&str; 4] = ["int", "uint", "double", "duration"];

fn g(t: &mut Table, n: &str, a: &str, r: &str) {
    add(t, n, false, a, r)
}

fn m(t: &mut Table, n: &str, a: &str, r: &str) {
    add(t, n, true, a, r)
}

/// `stdlib.Functions()` (`common/stdlib/standard.go`).
pub fn stdlib(t: &mut Table) {
    g(t, "_?_:_", "bool, A, A", "A");
    g(t, "_&&_", "bool, bool", "bool");
    g(t, "_||_", "bool, bool", "bool");
    g(t, "!_", "bool", "bool");
    g(t, "@not_strictly_false", "bool", "bool");
    g(t, "_==_", "A, A", "bool");
    g(t, "_!=_", "A, A", "bool");

    g(t, "_+_", "bytes, bytes", "bytes");
    g(t, "_+_", "double, double", "double");
    g(t, "_+_", "duration, duration", "duration");
    g(t, "_+_", "duration, timestamp", "timestamp");
    g(t, "_+_", "timestamp, duration", "timestamp");
    g(t, "_+_", "int, int", "int");
    g(t, "_+_", "list(A), list(A)", "list(A)");
    g(t, "_+_", "string, string", "string");
    g(t, "_+_", "uint, uint", "uint");
    g(t, "_/_", "double, double", "double");
    g(t, "_/_", "int, int", "int");
    g(t, "_/_", "uint, uint", "uint");
    g(t, "_%_", "int, int", "int");
    g(t, "_%_", "uint, uint", "uint");
    g(t, "_*_", "double, double", "double");
    g(t, "_*_", "int, int", "int");
    g(t, "_*_", "uint, uint", "uint");
    g(t, "-_", "double", "double");
    g(t, "-_", "int", "int");
    g(t, "_-_", "double, double", "double");
    g(t, "_-_", "duration, duration", "duration");
    g(t, "_-_", "int, int", "int");
    g(t, "_-_", "timestamp, duration", "timestamp");
    g(t, "_-_", "timestamp, timestamp", "duration");
    g(t, "_-_", "uint, uint", "uint");

    for op in ["_<_", "_<=_", "_>_", "_>=_"] {
        for pair in [
            "bool, bool",
            "int, int",
            "int, double",
            "int, uint",
            "uint, uint",
            "uint, double",
            "uint, int",
            "double, double",
            "double, int",
            "double, uint",
            "string, string",
            "bytes, bytes",
            "timestamp, timestamp",
            "duration, duration",
        ] {
            g(t, op, pair, "bool");
        }
    }

    // `_[_]` is resolved by the selector type pass itself (it needs the
    // container's element type); it is declared here for `LookupFunction`.
    g(t, "_[_]", "list(A), int", "A");
    g(t, "_[_]", "map(A, B), A", "B");
    g(t, "@in", "A, list(A)", "bool");
    g(t, "@in", "A, map(A, B)", "bool");

    for (a, r) in [
        ("bytes", "int"),
        ("list(A)", "int"),
        ("map(A, B)", "int"),
        ("string", "int"),
    ] {
        g(t, "size", a, r);
        m(t, "size", a, r);
    }

    g(t, "type", "A", "type(A)");
    g(t, "dyn", "A", "dyn");
    for (f, from, to) in [
        ("bool", "bool", "bool"),
        ("bool", "string", "bool"),
        ("bytes", "bytes", "bytes"),
        ("bytes", "string", "bytes"),
        ("double", "double", "double"),
        ("double", "int", "double"),
        ("double", "string", "double"),
        ("double", "uint", "double"),
        ("duration", "duration", "duration"),
        ("duration", "int", "duration"),
        ("duration", "string", "duration"),
        ("int", "int", "int"),
        ("int", "double", "int"),
        ("int", "duration", "int"),
        ("int", "string", "int"),
        ("int", "timestamp", "int"),
        ("int", "uint", "int"),
        ("string", "string", "string"),
        ("string", "bool", "string"),
        ("string", "bytes", "string"),
        ("string", "double", "string"),
        ("string", "duration", "string"),
        ("string", "int", "string"),
        ("string", "timestamp", "string"),
        ("string", "uint", "string"),
        ("timestamp", "timestamp", "timestamp"),
        ("timestamp", "int", "timestamp"),
        ("timestamp", "string", "timestamp"),
        ("uint", "uint", "uint"),
        ("uint", "double", "uint"),
        ("uint", "int", "uint"),
        ("uint", "string", "uint"),
    ] {
        g(t, f, from, to);
    }

    for f in ["contains", "endsWith", "startsWith"] {
        m(t, f, "string, string", "bool");
    }
    g(t, "matches", "string, string", "bool");
    m(t, "matches", "string, string", "bool");

    for (f, dur) in [
        ("getFullYear", false),
        ("getMonth", false),
        ("getDayOfYear", false),
        ("getDayOfMonth", false),
        ("getDate", false),
        ("getDayOfWeek", false),
        ("getHours", true),
        ("getMinutes", true),
        ("getSeconds", true),
        ("getMilliseconds", true),
    ] {
        m(t, f, "timestamp", "int");
        m(t, f, "timestamp, string", "int");
        if dur {
            m(t, f, "duration", "int");
        }
    }
}

/// `ext.Strings(ext.StringsVersion(0))`.
pub fn strings_v0(t: &mut Table) {
    m(t, "charAt", "string, int", "string");
    m(t, "indexOf", "string, string", "int");
    m(t, "indexOf", "string, string, int", "int");
    m(t, "lastIndexOf", "string, string", "int");
    m(t, "lastIndexOf", "string, string, int", "int");
    m(t, "lowerAscii", "string", "string");
    m(t, "replace", "string, string, string", "string");
    m(t, "replace", "string, string, string, int", "string");
    m(t, "split", "string, string", "list(string)");
    m(t, "split", "string, string, int", "list(string)");
    m(t, "substring", "string, int", "string");
    m(t, "substring", "string, int, int", "string");
    m(t, "trim", "string", "string");
    m(t, "upperAscii", "string", "string");
    // `join` exists at every version with the same declaration; only the
    // binding differs (`ext/strings.go:498-534`).
    m(t, "join", "list(string)", "string");
    m(t, "join", "list(string), string", "string");
}

/// The Kubernetes `JSONPatch` library (`apiserver/pkg/cel/library/jsonpatch.go:46-52`):
/// `jsonpatch.escapeKey(string) string`, enabled with the patch types of a
/// MutatingAdmissionPolicy (`plugin/cel/compile.go` `hasPatchTypes`).
pub fn jsonpatch(t: &mut Table) {
    g(t, "jsonpatch.escapeKey", "string", "string");
}

/// What `ext.Strings(ext.StringsVersion(2))` adds over version 0: `format` and `strings.quote` (`ext/strings.go:373-540`).
pub fn strings_v2_additions(t: &mut Table) {
    m(t, "format", "string, list(dyn)", "string");
    g(t, "strings.quote", "string", "string");
}

/// `ext.Sets()`.
pub fn sets(t: &mut Table) {
    for f in ["sets.contains", "sets.equivalent", "sets.intersects"] {
        g(t, f, "list(T), list(T)", "bool");
    }
}

/// `ext.Lists(ext.ListsVersion(3))`.
pub fn ext_lists(t: &mut Table) {
    m(t, "slice", "list(T), int, int", "list(T)");
    m(t, "flatten", "list(list(T))", "list(T)");
    m(t, "flatten", "list(dyn), int", "list(dyn)");
    for c in COMPARABLE {
        m(t, "sort", &format!("list({c})"), &format!("list({c})"));
        m(
            t,
            "@sortByAssociatedKeys",
            &format!("list(T), list({c})"),
            "list(T)",
        );
    }
    g(t, "lists.range", "int", "list(int)");
    m(t, "reverse", "list(T)", "list(T)");
    m(t, "distinct", "list(T)", "list(T)");
}

/// `library.Lists`.
pub fn k8s_lists(t: &mut Table) {
    for c in COMPARABLE {
        m(t, "isSorted", &format!("list({c})"), "bool");
        m(t, "max", &format!("list({c})"), c);
        m(t, "min", &format!("list({c})"), c);
    }
    for c in SUMMABLE {
        m(t, "sum", &format!("list({c})"), c);
    }
    m(t, "indexOf", "list(A), A", "int");
    m(t, "lastIndexOf", "list(A), A", "int");
}

/// `library.URLs`.
pub fn urls(t: &mut Table) {
    g(t, "url", "string", "s:kubernetes.URL");
    for f in [
        "getScheme",
        "getHost",
        "getHostname",
        "getPort",
        "getEscapedPath",
    ] {
        m(t, f, "s:kubernetes.URL", "string");
    }
    m(
        t,
        "getQuery",
        "s:kubernetes.URL",
        "map(string, list(string))",
    );
    g(t, "isURL", "string", "bool");
}

/// `library.Regex`.
pub fn regex(t: &mut Table) {
    m(t, "find", "string, string", "string");
    m(t, "findAll", "string, string", "list(string)");
    m(t, "findAll", "string, string, int", "list(string)");
}

/// `library.Quantity`.
pub fn quantity(t: &mut Table) {
    const Q: &str = "s:apiserver.cel.Quantity";
    g(t, "quantity", "string", Q);
    g(t, "isQuantity", "string", "bool");
    g(t, "sign", Q, "int");
    m(t, "isGreaterThan", &format!("{Q}, {Q}"), "bool");
    m(t, "isLessThan", &format!("{Q}, {Q}"), "bool");
    m(t, "compareTo", &format!("{Q}, {Q}"), "int");
    m(t, "asApproximateFloat", Q, "double");
    m(t, "asInteger", Q, "int");
    m(t, "isInteger", Q, "bool");
    m(t, "add", &format!("{Q}, {Q}"), Q);
    m(t, "add", &format!("{Q}, int"), Q);
    m(t, "sub", &format!("{Q}, {Q}"), Q);
    m(t, "sub", &format!("{Q}, int"), Q);
}

/// `library.IP`.
pub fn ip(t: &mut Table) {
    g(t, "ip", "string", "o:net.IP");
    m(t, "family", "o:net.IP", "int");
    g(t, "ip.isCanonical", "string", "bool");
    for f in [
        "isUnspecified",
        "isLoopback",
        "isLinkLocalMulticast",
        "isLinkLocalUnicast",
        "isGlobalUnicast",
    ] {
        m(t, f, "o:net.IP", "bool");
    }
    g(t, "isIP", "string", "bool");
    g(t, "string", "o:net.IP", "string");
}

/// `library.CIDR`.
pub fn cidr(t: &mut Table) {
    g(t, "cidr", "string", "o:net.CIDR");
    m(t, "containsIP", "o:net.CIDR, string", "bool");
    m(t, "containsIP", "o:net.CIDR, o:net.IP", "bool");
    m(t, "containsCIDR", "o:net.CIDR, string", "bool");
    m(t, "containsCIDR", "o:net.CIDR, o:net.CIDR", "bool");
    m(t, "ip", "o:net.CIDR", "o:net.IP");
    m(t, "prefixLength", "o:net.CIDR", "int");
    m(t, "masked", "o:net.CIDR", "o:net.CIDR");
    g(t, "isCIDR", "string", "bool");
    g(t, "string", "o:net.CIDR", "string");
}

/// `library.SemverLib(library.SemverVersion(1))`.
pub fn semver(t: &mut Table) {
    const S: &str = "s:kubernetes.Semver";
    g(t, "semver", "string", S);
    g(t, "semver", "string, bool", S);
    g(t, "isSemver", "string", "bool");
    g(t, "isSemver", "string, bool", "bool");
    m(t, "isGreaterThan", &format!("{S}, {S}"), "bool");
    m(t, "isLessThan", &format!("{S}, {S}"), "bool");
    m(t, "compareTo", &format!("{S}, {S}"), "int");
    for f in ["major", "minor", "patch"] {
        m(t, f, S, "int");
    }
}

/// `library.Format`.
pub fn format(t: &mut Table) {
    const F: &str = "s:kubernetes.NamedFormat";
    m(
        t,
        "validate",
        &format!("{F}, string"),
        "optional(list(string))",
    );
    g(t, "format.named", "string", &format!("optional({F})"));
    for f in [
        "dns1123Label",
        "dns1123Subdomain",
        "dns1035Label",
        "qualifiedName",
        "dns1123LabelPrefix",
        "dns1123SubdomainPrefix",
        "dns1035LabelPrefix",
        "labelValue",
        "uri",
        "uuid",
        "byte",
        "date",
        "datetime",
    ] {
        g(t, &format!("format.{f}"), "", F);
    }
}

/// `library.Authz`.
pub fn authz(t: &mut Table) {
    const A: &str = "s:kubernetes.authorization.Authorizer";
    const P: &str = "s:kubernetes.authorization.PathCheck";
    const GC: &str = "s:kubernetes.authorization.GroupCheck";
    const R: &str = "s:kubernetes.authorization.ResourceCheck";
    const D: &str = "s:kubernetes.authorization.Decision";
    m(t, "path", &format!("{A}, string"), P);
    m(t, "group", &format!("{A}, string"), GC);
    m(t, "serviceAccount", &format!("{A}, string, string"), A);
    m(t, "resource", &format!("{GC}, string"), R);
    m(t, "subresource", &format!("{R}, string"), R);
    m(t, "namespace", &format!("{R}, string"), R);
    m(t, "name", &format!("{R}, string"), R);
    m(t, "check", &format!("{P}, string"), D);
    m(t, "check", &format!("{R}, string"), D);
    m(t, "errored", D, "bool");
    m(t, "error", D, "string");
    m(t, "allowed", D, "bool");
    m(t, "reason", D, "string");
}

/// `library.AuthzSelectors`.
pub fn authz_selectors(t: &mut Table) {
    const R: &str = "s:kubernetes.authorization.ResourceCheck";
    m(t, "fieldSelector", &format!("{R}, string"), R);
    m(t, "labelSelector", &format!("{R}, string"), R);
}

/// `cel.OptionalTypes()`.
pub fn optional(t: &mut Table) {
    g(t, "optional.of", "V", "optional(V)");
    g(t, "optional.ofNonZeroValue", "V", "optional(V)");
    g(t, "optional.none", "", "optional(V)");
    m(t, "value", "optional(V)", "V");
    m(t, "hasValue", "optional(V)", "bool");
    m(t, "or", "optional(V), optional(V)", "optional(V)");
    m(t, "orValue", "optional(V), V", "V");
    m(t, "last", "list(V)", "optional(V)");
    m(t, "first", "list(V)", "optional(V)");
    g(t, "optional.unwrap", "list(optional(V))", "list(V)");
    m(t, "unwrapOpt", "list(optional(V))", "list(V)");
    g(t, "_?._", "dyn, string", "optional(V)");
    g(t, "_[?_]", "list(V), int", "optional(V)");
    g(t, "_[?_]", "optional(list(V)), int", "optional(V)");
    g(t, "_[?_]", "map(K, V), K", "optional(V)");
    g(t, "_[?_]", "optional(map(K, V)), K", "optional(V)");
}

/// Every library's declarations at their newest version: the environment of a
/// DRA device selector (`environment.MustBaseEnvSet` at the newest version,
/// plus authz selectors).
fn build() -> Table {
    let mut t = Table::new();
    for f in [
        stdlib,
        strings_v0,
        strings_v2_additions,
        sets,
        ext_lists,
        k8s_lists,
        urls,
        regex,
        quantity,
        ip,
        cidr,
        semver,
        format,
        authz,
        authz_selectors,
        optional,
    ] {
        f(&mut t);
    }
    t
}

pub fn table() -> &'static Table {
    static TABLE: OnceLock<Table> = OnceLock::new();
    TABLE.get_or_init(build)
}

/// `Env.LookupFunction`.
pub fn lookup(name: &str) -> Option<&'static [Overload]> {
    table().get(name).map(Vec::as_slice)
}

/// Macros the parser expands (or that `cel_dra` handles itself), which are
/// never looked up as functions: `parser/macro.go` `AllMacros`,
/// `ext/bindings.go`, `ext/comprehensions.go`, `cel/library.go`
/// (`optMap`, `optFlatMap`).
pub fn is_macro(name: &str) -> bool {
    matches!(
        name,
        "has"
            | "all"
            | "exists"
            | "exists_one"
            | "existsOne"
            | "map"
            | "filter"
            | "bind"
            | "cel.bind"
            | "transformList"
            | "transformMap"
            | "transformMapEntry"
            | "optMap"
            | "optFlatMap"
            | "sortBy"
    )
}
