//! The Rust checker against the golden corpus generated from the Go oracle
//! (`tools/cel-oracle`, cel-go in the Kubernetes CEL environment).
//!
//! * [`checker_matches_goldens`] always runs: no Go needed, the goldens are
//!   checked in (`tests/corpus/goldens.jsonl`).
//! * [`goldens_match_live_oracle`] is `#[ignore]`d and runs when
//!   `CEL_ORACLE=<path to the celoracle binary>` is set (`cargo test --
//!   --ignored`): it checks that the goldens are what the oracle says today.
//!
//! Regenerate with `tools/cel-oracle/gen-goldens.sh`.

use cel::parser::Parser;
use rusternetes_cel_checker::library_cost::kubernetes_cost;
use rusternetes_cel_checker::{
    check, Container, DType, EnvSet, EnvType, Features, MapProvider, NoLocations, VariableDecl,
    Version,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Arc;

/// Corpus entries whose result is a known, explained divergence from the
/// oracle. Each is a gap to fix, not an expectation: the checker must match
/// cel-go, and an entry here is removed once the gap closes.
const KNOWN_DIVERGENCES: &[(&str, &str)] = &[
    (
        "quantity(s).sign()",
        "the Quantity type is declared as 'apiserver.cel.Quantity' (the name the DRA environment reports) where the Kubernetes library names it 'kubernetes.Quantity'. Only the message differs",
    ),
    (
        "quantity(s).add(quantity(t))",
        "the Quantity type is declared as 'apiserver.cel.Quantity' where the Kubernetes library names it 'kubernetes.Quantity'. Only the output type name differs",
    ),
    (
        "[1, 2].exists(i, v, i < v)",
        "cel 0.15's parser does not expand the two-variable comprehension macros (cel-go parser/macro.go, TwoVarComprehensions), so the call is checked as a plain member call",
    ),
    (
        "{}.map(c, [c, type(c)])",
        "cel-go reports the element types of the rejected heterogeneous list as 'dyn' / 'type(dyn)'; we report the unbound type parameters '_var0' / 'type(_var0)'. Only the message differs",
    ),
];

fn parse_type(s: &str) -> DType {
    let s = s.trim();
    let simple = match s {
        "bool" => Some(DType::Bool),
        "int" => Some(DType::Int),
        "uint" => Some(DType::Uint),
        "double" => Some(DType::Double),
        "string" => Some(DType::Str),
        "bytes" => Some(DType::Bytes),
        "dyn" => Some(DType::Dyn),
        "null" => Some(DType::Null),
        "timestamp" => Some(DType::Timestamp),
        "duration" => Some(DType::Duration),
        _ => None,
    };
    if let Some(t) = simple {
        return t;
    }
    let open = s.find('(').unwrap_or_else(|| panic!("bad type {s}"));
    let inner = &s[open + 1..s.len() - 1];
    let mut args = Vec::new();
    let (mut depth, mut start) = (0, 0);
    for (i, c) in inner.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                args.push(parse_type(&inner[start..i]));
                start = i + 1;
            }
            _ => {}
        }
    }
    args.push(parse_type(&inner[start..]));
    match &s[..open] {
        "list" => DType::List(Box::new(args.remove(0))),
        "map" => {
            let k = args.remove(0);
            DType::Map(Box::new(k), Box::new(args.remove(0)))
        }
        "optional" => DType::optional(args.remove(0)),
        other => panic!("bad type constructor {other}"),
    }
}

/// What our checker says about a request, in the oracle's response shape: the
/// verdict, the errors, the output type and the estimated cost
/// (`env.EstimateCost(ast, &library.CostEstimator{})`, no size hints).
type Ours = (bool, Vec<String>, Option<String>, Option<(u64, u64)>);

fn ours(req: &Value) -> Ours {
    let expr_src = req["expr"].as_str().expect("expr");
    let version = req
        .get("kubeVersion")
        .and_then(Value::as_str)
        .unwrap_or("1.35");
    let (major, minor) = version.split_once('.').expect("kubeVersion");
    let set = EnvSet::must_base_env_set(
        Version::major_minor(major.parse().unwrap(), minor.parse().unwrap()),
        &Features::default(),
    );
    let env_type = match req.get("envType").and_then(Value::as_str) {
        Some("stored") => EnvType::StoredExpressions,
        _ => EnvType::NewExpressions,
    };
    let mut env = set.env(env_type, Container::default(), Arc::new(MapProvider::new()));
    if let Some(vars) = req.get("vars").and_then(Value::as_object) {
        for (n, t) in vars {
            env.add_ident(VariableDecl::new(
                n.clone(),
                parse_type(t.as_str().unwrap()),
            ))
            .unwrap();
        }
    }
    for lib in req
        .get("libs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        match lib.as_str() {
            Some("jsonpatch") => env.add_library(rusternetes_cel_checker::table::jsonpatch),
            other => panic!("unknown library {other:?}"),
        }
    }
    let expr = match Parser::new().enable_optional_syntax(true).parse(expr_src) {
        Ok(e) => e,
        Err(e) => return (false, vec![e.to_string()], None, None),
    };
    let (ast, errs) = check(&expr, &mut env, &NoLocations);
    let msgs: Vec<String> = errs.errors().iter().map(|e| e.message.clone()).collect();
    let ok = msgs.is_empty();
    let out = ok.then(|| ast.output_type().map(DType::name)).flatten();
    let cost = ok.then(|| {
        let c = kubernetes_cost(&ast, &expr, None);
        (c.min, c.max)
    });
    (ok, msgs, out, cost)
}

fn goldens() -> Vec<(Value, Value)> {
    include_str!("../corpus/goldens.jsonl")
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let v: Value = serde_json::from_str(l).unwrap();
            (v["req"].clone(), v["resp"].clone())
        })
        .collect()
}

/// The oracle prints `cel.Type.String()` (`google.protobuf.Duration`), the
/// checker prints `FormatCELType` (`duration`).
fn normalize_output_type(s: &str) -> String {
    s.replace("google.protobuf.Duration", "duration")
        .replace("google.protobuf.Timestamp", "timestamp")
}

fn describe(
    ok: bool,
    errors: &[String],
    out: &Option<String>,
    cost: &Option<(u64, u64)>,
) -> String {
    format!("ok={ok} errors={errors:?} outputType={out:?} cost={cost:?}")
}

#[test]
fn checker_matches_goldens() {
    let known: BTreeMap<&str, &str> = KNOWN_DIVERGENCES.iter().copied().collect();
    let mut mismatches = Vec::new();
    for (req, resp) in goldens() {
        let expr = req["expr"].as_str().unwrap();
        let want_ok = resp["ok"].as_bool().unwrap();
        let want_errors: Vec<String> = resp["errors"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e.as_str().unwrap().to_string())
            .collect();
        let want_out = resp
            .get("outputType")
            .and_then(Value::as_str)
            .map(normalize_output_type);
        let want_cost = resp.get("cost").map(|c| {
            (
                c["min"].as_u64().expect("cost.min"),
                c["max"].as_u64().expect("cost.max"),
            )
        });
        let (ok, mut errors, out, cost) = ours(&req);
        // cel-go reports issues ordered by source position; the checker here
        // has no locations yet, so errors are compared as a sorted set.
        let mut want_errors = want_errors;
        want_errors.sort();
        errors.sort();
        let same = ok == want_ok && errors == want_errors && out == want_out && cost == want_cost;
        let key = format!("{expr} @ {req}");
        match (same, known.contains_key(expr)) {
            (true, false) => {}
            (true, true) => mismatches.push(format!(
                "{key}: listed in KNOWN_DIVERGENCES but now matches, remove it"
            )),
            (false, true) => {}
            (false, false) => mismatches.push(format!(
                "{key}\n    oracle: {}\n    ours:   {}",
                describe(want_ok, &want_errors, &want_out, &want_cost),
                describe(ok, &errors, &out, &cost)
            )),
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} divergences from the oracle:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
#[ignore = "needs CEL_ORACLE=<path to the celoracle binary>; see tools/cel-oracle"]
fn goldens_match_live_oracle() {
    let Ok(bin) = std::env::var("CEL_ORACLE") else {
        panic!("set CEL_ORACLE to the celoracle binary");
    };
    let requests = include_str!("../corpus/requests.jsonl");
    let mut child = Command::new(bin)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start oracle");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(requests.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    let live: Vec<Value> = String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let stored = goldens();
    assert_eq!(live.len(), stored.len());
    for (resp, (req, golden)) in live.iter().zip(stored.iter()) {
        assert_eq!(resp, golden, "stale golden for {req}");
    }
}
