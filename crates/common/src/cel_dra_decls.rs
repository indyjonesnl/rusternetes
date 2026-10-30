//! Function declarations and overload resolution of the CEL environment a DRA
//! device selector is compiled in (`cel_dra`).
//!
//! Upstream builds the environment with
//! `environment.MustBaseEnvSet(DefaultCompatibilityVersion())`
//! (`staging/src/k8s.io/dynamic-resource-allocation/cel/compile.go:302`,
//! `newCompiler`), i.e. the cel-go standard library
//! (`vendor/github.com/google/cel-go/common/stdlib/standard.go`) plus the
//! libraries of `staging/src/k8s.io/apiserver/pkg/cel/environment/base.go:60-207`:
//! `ext.Strings(2)`, `ext.Sets()`, `ext.Lists(3)`, `cel.OptionalTypes()`,
//! `cel.CrossTypeNumericComparisons(true)` and the Kubernetes `URLs`, `Regex`,
//! `Lists`, `Authz`, `AuthzSelectors`, `Quantity`, `IP`, `CIDR`, `Format` and
//! `SemverLib` libraries, plus `ext.Bindings` (`compile.go:337`).
//!
//! This module holds those declarations and the checker's
//! `resolveOverload` (`vendor/github.com/google/cel-go/checker/checker.go:261-348`)
//! with its type-parameter unification (`checker/types.go`). Not in the
//! environment, hence undeclared here as upstream: `ext.Math()` (`math.*`),
//! `ext.Encoders()` (`base64.*`), `ext.Protos()`.

use std::collections::HashMap;
use std::sync::OnceLock;

/// `types.Type`: the kinds the declarations and the selector type pass use.
#[derive(Clone, Debug, PartialEq)]
pub enum DType {
    Bool,
    Int,
    Uint,
    Double,
    Str,
    Bytes,
    Null,
    /// `types.DynType`.
    Dyn,
    /// `types.AnyKind`: `isDyn` treats it as `dyn`, it only prints differently.
    Any,
    Timestamp,
    Duration,
    /// `types.TypeKind`, with its optional type parameter.
    Type(Option<Box<DType>>),
    List(Box<DType>),
    Map(Box<DType>, Box<DType>),
    /// `types.TypeParamKind`.
    Param(String),
    /// `types.OpaqueKind` (`optional_type`, `net.IP`, ...).
    Opaque(String, Vec<DType>),
    /// `types.StructKind` (`cel.ObjectType`).
    Struct(String),
}

impl DType {
    fn is_dyn(&self) -> bool {
        matches!(self, DType::Dyn | DType::Any)
    }

    /// `checker/format.go` `FormatCELType`.
    pub(crate) fn name(&self) -> String {
        match self {
            DType::Bool => "bool".into(),
            DType::Int => "int".into(),
            DType::Uint => "uint".into(),
            DType::Double => "double".into(),
            DType::Str => "string".into(),
            DType::Bytes => "bytes".into(),
            DType::Null => "null".into(),
            DType::Dyn => "dyn".into(),
            DType::Any => "any".into(),
            DType::Timestamp => "timestamp".into(),
            DType::Duration => "duration".into(),
            DType::Type(None) => "type".into(),
            DType::Type(Some(p)) => format!("type({})", p.name()),
            DType::List(t) => format!("list({})", t.name()),
            DType::Map(k, v) => format!("map({}, {})", k.name(), v.name()),
            DType::Param(n) => n.clone(),
            DType::Opaque(n, ps) if ps.is_empty() => n.clone(),
            DType::Opaque(n, ps) => format!(
                "{n}({})",
                ps.iter().map(DType::name).collect::<Vec<_>>().join(", ")
            ),
            DType::Struct(n) => n.clone(),
        }
    }

    /// `types.Type.TypeName` for the kinds that compare by name.
    fn kind_name(&self) -> String {
        match self {
            DType::Opaque(n, _) | DType::Struct(n) => n.clone(),
            DType::List(_) => "list".into(),
            DType::Map(..) => "map".into(),
            DType::Type(_) => "type".into(),
            other => other.name(),
        }
    }

    fn params(&self) -> Vec<&DType> {
        match self {
            DType::List(t) => vec![t],
            DType::Map(k, v) => vec![k, v],
            DType::Opaque(_, ps) => ps.iter().collect(),
            DType::Type(Some(p)) => vec![p],
            _ => vec![],
        }
    }

    /// `types.Kind` equality (a type parameter is one kind whatever its name).
    fn same_kind(&self, o: &DType) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(o)
    }
}

/// One `decls.OverloadDecl`.
#[derive(Debug)]
pub struct Overload {
    pub member: bool,
    /// Argument types; for a member overload the first is the receiver.
    pub args: Vec<DType>,
    pub result: DType,
}

// ---------------------------------------------------------------------------
// Declarations
// ---------------------------------------------------------------------------

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
            other => panic!("cel_dra_decls: unknown type constructor {other}"),
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
        other => panic!("cel_dra_decls: unknown type {other}"),
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

type Table = HashMap<String, Vec<Overload>>;

fn add(t: &mut Table, name: &str, member: bool, args: &str, result: &str) {
    t.entry(name.to_string()).or_default().push(Overload {
        member,
        args: split_top(args).iter().map(|a| parse_type(a)).collect(),
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

fn build() -> Table {
    let mut t = Table::new();
    let g = |t: &mut Table, n: &str, a: &str, r: &str| add(t, n, false, a, r);
    let m = |t: &mut Table, n: &str, a: &str, r: &str| add(t, n, true, a, r);

    // --- cel-go standard library (common/stdlib/standard.go) -------------
    g(&mut t, "_?_:_", "bool, A, A", "A");
    g(&mut t, "_&&_", "bool, bool", "bool");
    g(&mut t, "_||_", "bool, bool", "bool");
    g(&mut t, "!_", "bool", "bool");
    g(&mut t, "@not_strictly_false", "bool", "bool");
    g(&mut t, "_==_", "A, A", "bool");
    g(&mut t, "_!=_", "A, A", "bool");

    g(&mut t, "_+_", "bytes, bytes", "bytes");
    g(&mut t, "_+_", "double, double", "double");
    g(&mut t, "_+_", "duration, duration", "duration");
    g(&mut t, "_+_", "duration, timestamp", "timestamp");
    g(&mut t, "_+_", "timestamp, duration", "timestamp");
    g(&mut t, "_+_", "int, int", "int");
    g(&mut t, "_+_", "list(A), list(A)", "list(A)");
    g(&mut t, "_+_", "string, string", "string");
    g(&mut t, "_+_", "uint, uint", "uint");
    g(&mut t, "_/_", "double, double", "double");
    g(&mut t, "_/_", "int, int", "int");
    g(&mut t, "_/_", "uint, uint", "uint");
    g(&mut t, "_%_", "int, int", "int");
    g(&mut t, "_%_", "uint, uint", "uint");
    g(&mut t, "_*_", "double, double", "double");
    g(&mut t, "_*_", "int, int", "int");
    g(&mut t, "_*_", "uint, uint", "uint");
    g(&mut t, "-_", "double", "double");
    g(&mut t, "-_", "int", "int");
    g(&mut t, "_-_", "double, double", "double");
    g(&mut t, "_-_", "duration, duration", "duration");
    g(&mut t, "_-_", "int, int", "int");
    g(&mut t, "_-_", "timestamp, duration", "timestamp");
    g(&mut t, "_-_", "timestamp, timestamp", "duration");
    g(&mut t, "_-_", "uint, uint", "uint");

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
            g(&mut t, op, pair, "bool");
        }
    }

    // `_[_]` is resolved by the selector type pass itself (it needs the
    // container's element type); it is declared here for `LookupFunction`.
    g(&mut t, "_[_]", "list(A), int", "A");
    g(&mut t, "_[_]", "map(A, B), A", "B");
    g(&mut t, "@in", "A, list(A)", "bool");
    g(&mut t, "@in", "A, map(A, B)", "bool");

    for (a, r) in [
        ("bytes", "int"),
        ("list(A)", "int"),
        ("map(A, B)", "int"),
        ("string", "int"),
    ] {
        g(&mut t, "size", a, r);
        m(&mut t, "size", a, r);
    }

    g(&mut t, "type", "A", "type(A)");
    g(&mut t, "dyn", "A", "dyn");
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
        g(&mut t, f, from, to);
    }

    for f in ["contains", "endsWith", "startsWith"] {
        m(&mut t, f, "string, string", "bool");
    }
    g(&mut t, "matches", "string, string", "bool");
    m(&mut t, "matches", "string, string", "bool");

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
        m(&mut t, f, "timestamp", "int");
        m(&mut t, f, "timestamp, string", "int");
        if dur {
            m(&mut t, f, "duration", "int");
        }
    }

    // --- ext.Strings(2) (ext/strings.go:373-540) --------------------------
    m(&mut t, "charAt", "string, int", "string");
    m(&mut t, "indexOf", "string, string", "int");
    m(&mut t, "indexOf", "string, string, int", "int");
    m(&mut t, "lastIndexOf", "string, string", "int");
    m(&mut t, "lastIndexOf", "string, string, int", "int");
    m(&mut t, "lowerAscii", "string", "string");
    m(&mut t, "replace", "string, string, string", "string");
    m(&mut t, "replace", "string, string, string, int", "string");
    m(&mut t, "split", "string, string", "list(string)");
    m(&mut t, "split", "string, string, int", "list(string)");
    m(&mut t, "substring", "string, int", "string");
    m(&mut t, "substring", "string, int, int", "string");
    m(&mut t, "trim", "string", "string");
    m(&mut t, "upperAscii", "string", "string");
    m(&mut t, "format", "string, list(dyn)", "string");
    g(&mut t, "strings.quote", "string", "string");
    m(&mut t, "join", "list(string)", "string");
    m(&mut t, "join", "list(string), string", "string");

    // --- ext.Sets (ext/sets.go:112-119) -----------------------------------
    for f in ["sets.contains", "sets.equivalent", "sets.intersects"] {
        g(&mut t, f, "list(T), list(T)", "bool");
    }

    // --- ext.Lists(3) (ext/lists.go:197-345) ------------------------------
    m(&mut t, "slice", "list(T), int, int", "list(T)");
    m(&mut t, "flatten", "list(list(T))", "list(T)");
    m(&mut t, "flatten", "list(dyn), int", "list(dyn)");
    for c in COMPARABLE {
        m(&mut t, "sort", &format!("list({c})"), &format!("list({c})"));
        m(
            &mut t,
            "@sortByAssociatedKeys",
            &format!("list(T), list({c})"),
            "list(T)",
        );
    }
    g(&mut t, "lists.range", "int", "list(int)");
    m(&mut t, "reverse", "list(T)", "list(T)");
    m(&mut t, "distinct", "list(T)", "list(T)");

    // --- Kubernetes Lists (apiserver/pkg/cel/library/lists.go:138-176) ----
    for c in COMPARABLE {
        m(&mut t, "isSorted", &format!("list({c})"), "bool");
        m(&mut t, "max", &format!("list({c})"), c);
        m(&mut t, "min", &format!("list({c})"), c);
    }
    for c in SUMMABLE {
        m(&mut t, "sum", &format!("list({c})"), c);
    }
    m(&mut t, "indexOf", "list(A), A", "int");
    m(&mut t, "lastIndexOf", "list(A), A", "int");

    // --- Kubernetes URLs / Regex -------------------------------------------
    g(&mut t, "url", "string", "s:kubernetes.URL");
    for f in [
        "getScheme",
        "getHost",
        "getHostname",
        "getPort",
        "getEscapedPath",
    ] {
        m(&mut t, f, "s:kubernetes.URL", "string");
    }
    m(
        &mut t,
        "getQuery",
        "s:kubernetes.URL",
        "map(string, list(string))",
    );
    g(&mut t, "isURL", "string", "bool");
    m(&mut t, "find", "string, string", "string");
    m(&mut t, "findAll", "string, string", "list(string)");
    m(&mut t, "findAll", "string, string, int", "list(string)");

    // --- Kubernetes Quantity (library/quantity.go:156-190) ------------------
    const Q: &str = "s:apiserver.cel.Quantity";
    g(&mut t, "quantity", "string", Q);
    g(&mut t, "isQuantity", "string", "bool");
    g(&mut t, "sign", Q, "int");
    m(&mut t, "isGreaterThan", &format!("{Q}, {Q}"), "bool");
    m(&mut t, "isLessThan", &format!("{Q}, {Q}"), "bool");
    m(&mut t, "compareTo", &format!("{Q}, {Q}"), "int");
    m(&mut t, "asApproximateFloat", Q, "double");
    m(&mut t, "asInteger", Q, "int");
    m(&mut t, "isInteger", Q, "bool");
    m(&mut t, "add", &format!("{Q}, {Q}"), Q);
    m(&mut t, "add", &format!("{Q}, int"), Q);
    m(&mut t, "sub", &format!("{Q}, {Q}"), Q);
    m(&mut t, "sub", &format!("{Q}, int"), Q);

    // --- Kubernetes IP / CIDR (library/ip.go:147-188, cidr.go:124-160) ------
    g(&mut t, "ip", "string", "o:net.IP");
    m(&mut t, "family", "o:net.IP", "int");
    g(&mut t, "ip.isCanonical", "string", "bool");
    for f in [
        "isUnspecified",
        "isLoopback",
        "isLinkLocalMulticast",
        "isLinkLocalUnicast",
        "isGlobalUnicast",
    ] {
        m(&mut t, f, "o:net.IP", "bool");
    }
    g(&mut t, "isIP", "string", "bool");
    g(&mut t, "string", "o:net.IP", "string");
    g(&mut t, "cidr", "string", "o:net.CIDR");
    m(&mut t, "containsIP", "o:net.CIDR, string", "bool");
    m(&mut t, "containsIP", "o:net.CIDR, o:net.IP", "bool");
    m(&mut t, "containsCIDR", "o:net.CIDR, string", "bool");
    m(&mut t, "containsCIDR", "o:net.CIDR, o:net.CIDR", "bool");
    m(&mut t, "ip", "o:net.CIDR", "o:net.IP");
    m(&mut t, "prefixLength", "o:net.CIDR", "int");
    m(&mut t, "masked", "o:net.CIDR", "o:net.CIDR");
    g(&mut t, "isCIDR", "string", "bool");
    g(&mut t, "string", "o:net.CIDR", "string");

    // --- Kubernetes Semver (library/semverlib.go:135-162) -------------------
    const S: &str = "s:kubernetes.Semver";
    g(&mut t, "semver", "string", S);
    g(&mut t, "semver", "string, bool", S);
    g(&mut t, "isSemver", "string", "bool");
    g(&mut t, "isSemver", "string, bool", "bool");
    m(&mut t, "isGreaterThan", &format!("{S}, {S}"), "bool");
    m(&mut t, "isLessThan", &format!("{S}, {S}"), "bool");
    m(&mut t, "compareTo", &format!("{S}, {S}"), "int");
    for f in ["major", "minor", "patch"] {
        m(&mut t, f, S, "int");
    }

    // --- Kubernetes Format (library/format.go:142-262) ----------------------
    const F: &str = "s:kubernetes.NamedFormat";
    m(
        &mut t,
        "validate",
        &format!("{F}, string"),
        "optional(list(string))",
    );
    g(&mut t, "format.named", "string", &format!("optional({F})"));
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
        g(&mut t, &format!("format.{f}"), "", F);
    }

    // --- Kubernetes Authz / AuthzSelectors (library/authz.go:252-363) -------
    const A: &str = "s:kubernetes.authorization.Authorizer";
    const P: &str = "s:kubernetes.authorization.PathCheck";
    const GC: &str = "s:kubernetes.authorization.GroupCheck";
    const R: &str = "s:kubernetes.authorization.ResourceCheck";
    const D: &str = "s:kubernetes.authorization.Decision";
    m(&mut t, "path", &format!("{A}, string"), P);
    m(&mut t, "group", &format!("{A}, string"), GC);
    m(&mut t, "serviceAccount", &format!("{A}, string, string"), A);
    m(&mut t, "resource", &format!("{GC}, string"), R);
    m(&mut t, "subresource", &format!("{R}, string"), R);
    m(&mut t, "namespace", &format!("{R}, string"), R);
    m(&mut t, "name", &format!("{R}, string"), R);
    m(&mut t, "check", &format!("{P}, string"), D);
    m(&mut t, "check", &format!("{R}, string"), D);
    m(&mut t, "errored", D, "bool");
    m(&mut t, "error", D, "string");
    m(&mut t, "allowed", D, "bool");
    m(&mut t, "reason", D, "string");
    m(&mut t, "fieldSelector", &format!("{R}, string"), R);
    m(&mut t, "labelSelector", &format!("{R}, string"), R);

    // --- cel.OptionalTypes() (cel/library.go:411-620) ------------------------
    g(&mut t, "optional.of", "V", "optional(V)");
    g(&mut t, "optional.ofNonZeroValue", "V", "optional(V)");
    g(&mut t, "optional.none", "", "optional(V)");
    m(&mut t, "value", "optional(V)", "V");
    m(&mut t, "hasValue", "optional(V)", "bool");
    m(&mut t, "or", "optional(V), optional(V)", "optional(V)");
    m(&mut t, "orValue", "optional(V), V", "V");
    m(&mut t, "last", "list(V)", "optional(V)");
    m(&mut t, "first", "list(V)", "optional(V)");
    g(&mut t, "optional.unwrap", "list(optional(V))", "list(V)");
    m(&mut t, "unwrapOpt", "list(optional(V))", "list(V)");
    g(&mut t, "_?._", "dyn, string", "optional(V)");
    g(&mut t, "_[?_]", "list(V), int", "optional(V)");
    g(&mut t, "_[?_]", "optional(list(V)), int", "optional(V)");
    g(&mut t, "_[?_]", "map(K, V), K", "optional(V)");
    g(&mut t, "_[?_]", "optional(map(K, V)), K", "optional(V)");

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
pub(crate) fn is_macro(name: &str) -> bool {
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

// ---------------------------------------------------------------------------
// checker/types.go: assignability with type-parameter substitution
// ---------------------------------------------------------------------------

type Mapping = HashMap<String, DType>;

fn find(m: &Mapping, t: &DType) -> Option<DType> {
    match t {
        DType::Param(n) => m.get(n).cloned(),
        _ => None,
    }
}

/// `types.Type.IsAssignableType` (`defaultIsAssignableType`): `t` accepts
/// `from` structurally.
fn is_assignable_type(t: &DType, from: &DType) -> bool {
    if t == from || t.is_dyn() || matches!(t, DType::Param(_)) {
        return true;
    }
    if !t.same_kind(from) || t.kind_name() != from.kind_name() {
        return false;
    }
    let (tp, fp) = (t.params(), from.params());
    tp.len() == fp.len()
        && tp
            .iter()
            .zip(fp.iter())
            .all(|(a, b)| is_assignable_type(a, b))
}

/// `isLegacyNullable` or assignable from `null`.
fn assignable_null(t: &DType) -> bool {
    matches!(
        t,
        DType::Opaque(..)
            | DType::Struct(_)
            | DType::Any
            | DType::Dyn
            | DType::Duration
            | DType::Timestamp
            | DType::Null
            | DType::Param(_)
    )
}

/// `isEqualOrLessSpecific`.
fn equal_or_less_specific(t1: &DType, t2: &DType) -> bool {
    if t1.is_dyn() || matches!(t1, DType::Param(_)) {
        return true;
    }
    if t2.is_dyn() || matches!(t2, DType::Param(_)) {
        return false;
    }
    if !t1.same_kind(t2) {
        return false;
    }
    match t1 {
        DType::Opaque(n1, p1) => {
            let DType::Opaque(n2, p2) = t2 else {
                return false;
            };
            n1 == n2
                && p1.len() == p2.len()
                && p1
                    .iter()
                    .zip(p2.iter())
                    .all(|(a, b)| equal_or_less_specific(a, b))
        }
        DType::List(a) => {
            let DType::List(b) = t2 else { return false };
            equal_or_less_specific(a, b)
        }
        DType::Map(k1, v1) => {
            let DType::Map(k2, v2) = t2 else {
                return false;
            };
            equal_or_less_specific(k1, k2) && equal_or_less_specific(v1, v2)
        }
        DType::Type(_) => true,
        _ => t1 == t2,
    }
}

/// `mostGeneral`.
fn most_general(t1: &DType, t2: &DType) -> DType {
    if equal_or_less_specific(t1, t2) {
        t1.clone()
    } else {
        t2.clone()
    }
}

/// `notReferencedIn` (the occurs check).
fn not_referenced_in(m: &Mapping, t: &DType, within: &DType) -> bool {
    if t == within {
        return false;
    }
    match within {
        DType::Param(_) => match find(m, within) {
            None => true,
            Some(sub) => not_referenced_in(m, t, &sub),
        },
        DType::Opaque(..) | DType::List(_) | DType::Map(..) | DType::Type(_) => {
            within.params().iter().all(|p| not_referenced_in(m, t, p))
        }
        _ => true,
    }
}

/// `isValidTypeSubstitution`: (valid, t2 already has a substitution).
fn valid_substitution(m: &mut Mapping, t1: &DType, t2: &DType) -> (bool, bool) {
    if t1.same_kind(t2) && t1 == t2 {
        return (true, true);
    }
    let name = match t2 {
        DType::Param(n) => n.clone(),
        _ => return (false, false),
    };
    if let Some(sub) = find(m, t2) {
        if t1.same_kind(&sub) && *t1 == sub {
            return (true, true);
        }
        if assignable(m, t1, &sub) {
            let new = most_general(t1, &sub);
            if not_referenced_in(m, t2, &new) {
                m.insert(name, new);
            }
            return (true, true);
        }
        return (false, true);
    }
    if not_referenced_in(m, t2, t1) {
        m.insert(name, t1.clone());
        return (true, false);
    }
    (false, false)
}

/// `internalIsAssignable`: is `t1` assignable to `t2`.
fn assignable(m: &mut Mapping, t1: &DType, t2: &DType) -> bool {
    if matches!(t2, DType::Param(_)) {
        let (valid, has_sub) = valid_substitution(m, t1, t2);
        if valid {
            return true;
        }
        if !valid && has_sub {
            return false;
        }
    }
    if matches!(t1, DType::Param(_)) {
        let (valid, _) = valid_substitution(m, t2, t1);
        return valid;
    }
    if t1.is_dyn() || t2.is_dyn() {
        return true;
    }
    if matches!(t1, DType::Null) {
        return assignable_null(t2);
    }
    if matches!(t2, DType::Null) {
        return assignable_null(t1);
    }
    match t1 {
        DType::Bool
        | DType::Bytes
        | DType::Double
        | DType::Int
        | DType::Str
        | DType::Uint
        | DType::Duration
        | DType::Timestamp
        | DType::Struct(_) => is_assignable_type(t2, t1),
        DType::Type(_) => matches!(t2, DType::Type(_)),
        DType::Opaque(..) | DType::List(_) | DType::Map(..) => {
            t1.same_kind(t2) && t1.kind_name() == t2.kind_name() && {
                let (p1, p2) = (t1.params(), t2.params());
                p1.len() == p2.len() && p1.iter().zip(p2.iter()).all(|(a, b)| assignable(m, a, b))
            }
        }
        _ => false,
    }
}

/// `internalIsAssignableList` on a copy of the mapping, committing it only on
/// success (`isAssignableList`).
fn assignable_list(m: &mut Mapping, l1: &[DType], l2: &[DType]) -> bool {
    if l1.len() != l2.len() {
        return false;
    }
    let mut copy = m.clone();
    if l1
        .iter()
        .zip(l2.iter())
        .all(|(a, b)| assignable(&mut copy, a, b))
    {
        *m = copy;
        true
    } else {
        false
    }
}

/// `checker.substitute` with `typeParamToDyn`.
fn substitute(m: &Mapping, t: &DType, param_to_dyn: bool) -> DType {
    if let Some(sub) = find(m, t) {
        return substitute(m, &sub, param_to_dyn);
    }
    match t {
        DType::Param(_) if param_to_dyn => DType::Dyn,
        DType::Opaque(n, ps) => DType::Opaque(
            n.clone(),
            ps.iter().map(|p| substitute(m, p, param_to_dyn)).collect(),
        ),
        DType::List(e) => DType::List(Box::new(substitute(m, e, param_to_dyn))),
        DType::Map(k, v) => DType::Map(
            Box::new(substitute(m, k, param_to_dyn)),
            Box::new(substitute(m, v, param_to_dyn)),
        ),
        DType::Type(Some(p)) => DType::Type(Some(Box::new(substitute(m, p, param_to_dyn)))),
        other => other.clone(),
    }
}

/// `checker.isAssignable(argType, target)` outside a call: used for the
/// logical operators' `typeMismatch`.
pub(crate) fn is_assignable(t1: &DType, t2: &DType) -> bool {
    assignable(&mut Mapping::new(), t1, t2)
}

/// The outcome of `resolveOverload`.
pub enum Resolution {
    /// The result type of the first matching overload.
    #[allow(dead_code)] // the pass keeps its own call result types, see `cel_dra`
    Resolved(DType),
    /// No overload accepted the arguments: the substituted argument types for
    /// `noMatchingOverload` (the receiver first for a member call).
    NoMatch(Vec<DType>),
}

/// `checker.resolveOverload` for the ordinary (non-logical) functions.
/// `arg_types` holds the receiver first when `member`.
pub fn resolve(decls: &[Overload], member: bool, arg_types: &[DType]) -> Resolution {
    let mut result: Option<DType> = None;
    let mut mapping = Mapping::new();
    for o in decls {
        if o.member != member {
            continue;
        }
        // Each overload gets its own fresh type variables; the parameters of
        // different overloads therefore never interact, so a per-call mapping
        // reproduces `substitutions.add(typePar, c.newTypeVar())`.
        let mut local = Mapping::new();
        if !assignable_list(&mut local, arg_types, &o.args) {
            continue;
        }
        let r = substitute(&local, &o.result, false);
        match &result {
            None => result = Some(r),
            Some(prev) if !prev.is_dyn() && *prev != r => result = Some(DType::Dyn),
            Some(_) => {}
        }
        mapping.extend(local);
    }
    match result {
        Some(r) => Resolution::Resolved(r),
        None => Resolution::NoMatch(
            arg_types
                .iter()
                .map(|a| substitute(&mapping, a, true))
                .collect(),
        ),
    }
}

/// `checker/format.go` `formatFunctionDeclType(nil, args, isInstance)`.
pub fn format_signature(args: &[DType], is_instance: bool) -> String {
    let (target, rest) = if is_instance {
        (Some(&args[0]), &args[1..])
    } else {
        (None, args)
    };
    let mut out = String::new();
    if let Some(t) = target {
        out.push_str(&t.name());
        out.push('.');
    }
    out.push('(');
    out.push_str(&rest.iter().map(DType::name).collect::<Vec<_>>().join(", "));
    out.push(')');
    out
}
