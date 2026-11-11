//! Custom-resource table conversion — port of
//! `staging/src/k8s.io/apiextensions-apiserver/pkg/registry/customresource/
//! tableconvertor/tableconvertor.go` (`New`, `convertor.ConvertToTable`,
//! `cellForJSONValue`), with the column defaulting of
//! `pkg/apis/apiextensions/helpers.go: serveDefaultColumnsIfEmpty` and
//! `pkg/apiserver/helpers.go: getColumnsForVersion`.
//!
//! The JSONPath evaluator is the subset of
//! `k8s.io/client-go/util/jsonpath/jsonpath.go` (`FindResults`, `walk`,
//! `evalField`, `evalArray`, `evalWildcard`, `evalUnion`, `PrintResults`) a
//! printer column can use, run over the parse tree of
//! [`k3rs_common::jsonpath`] with `AllowMissingKeys(true)`, including
//! filters (`[?(...)]`, with the comparisons of
//! `client-go/third_party/forked/golang/template/funcs.go`), recursive descent
//! (`..`) and the `range`/`end` block state machine of `FindResults`. An
//! evaluation error is rendered by `ConvertToTable` as a `null` cell.
//!
//! Not ported: the `Desired`/`Actual` columns upstream serves for the scale
//! subresource (`getScaleColumnsForVersion`), and `TableOptions.includeObject`
//! (upstream applies it in the response writer, `handlers/response.go asTable`,
//! not in this convertor; rows keep the full object like every other kind).

use k3rs_common::jsonpath::{self, ListNode, Node};
use k3rs_common::resources::{CustomResourceColumnDefinition, CustomResourceDefinition};
use k3rs_middleware::table::{
    humanize_age, ColumnDefinition, Table, TableMetadata, TableRow,
    OBJECT_META_CREATION_TIMESTAMP_DOC, OBJECT_META_NAME_DOC,
};
use serde_json::Value;

/// `getColumnsForVersion` + `serveDefaultColumnsIfEmpty`: the version's
/// `additionalPrinterColumns`, or the single default `Age` column when it
/// declares none. `None` is upstream's "version not found" error.
pub fn columns_for_version(
    crd: &CustomResourceDefinition,
    version: &str,
) -> Option<Vec<CustomResourceColumnDefinition>> {
    let v = crd.spec.versions.iter().find(|v| v.name == version)?;
    match v.additional_printer_columns.as_ref() {
        Some(cols) if !cols.is_empty() => Some(cols.clone()),
        _ => Some(vec![CustomResourceColumnDefinition {
            name: "Age".to_string(),
            type_: "date".to_string(),
            format: None,
            description: Some(OBJECT_META_CREATION_TIMESTAMP_DOC.to_string()),
            priority: None,
            json_path: ".metadata.creationTimestamp".to_string(),
        }]),
    }
}

/// `convertor`: the table headers and one parsed JSONPath per additional
/// column.
pub struct TableConvertor {
    headers: Vec<ColumnDefinition>,
    additional_columns: Vec<ListNode>,
}

impl TableConvertor {
    /// `New`. A column whose JSONPath does not parse stops the build: the
    /// convertor holding the columns before it is returned with the error
    /// (`unrecognized column definition "<path>"`), as upstream returns
    /// `c, err`, and the caller serves that convertor.
    pub fn new(crd_columns: &[CustomResourceColumnDefinition]) -> (Self, Option<String>) {
        let mut c = TableConvertor {
            headers: vec![ColumnDefinition {
                name: "Name".to_string(),
                column_type: "string".to_string(),
                format: "name".to_string(),
                description: OBJECT_META_NAME_DOC.to_string(),
                priority: 0,
            }],
            additional_columns: Vec::new(),
        };
        for col in crd_columns {
            let parsed = jsonpath::parse(&col.name, &format!("{{{}}}", col.json_path));
            let Ok(parser) = parsed else {
                return (
                    c,
                    Some(format!(
                        "unrecognized column definition {:?}",
                        col.json_path
                    )),
                );
            };
            let desc = match col.description.as_deref() {
                Some(d) if !d.is_empty() => d.to_string(),
                _ => format!(
                    "Custom resource definition column (in JSONPath format): {}",
                    col.json_path
                ),
            };
            c.additional_columns.push(parser.root);
            c.headers.push(ColumnDefinition {
                name: col.name.clone(),
                column_type: col.type_.clone(),
                format: col.format.clone().unwrap_or_default(),
                description: desc,
                priority: col.priority.unwrap_or(0),
            });
        }
        (c, None)
    }

    /// The headers a table carries (`Name` first).
    pub fn headers(&self) -> &[ColumnDefinition] {
        &self.headers
    }

    /// `ConvertToTable`. `obj` is a custom resource or a list of them;
    /// `no_headers` is `TableOptions.NoHeaders`.
    pub fn convert_to_table(&self, obj: &Value, no_headers: bool) -> Table {
        let mut table = Table::new();
        if !no_headers {
            table.column_definitions = self.headers.clone();
        }

        // `meta.ListAccessor` succeeds for a list; otherwise the object's own
        // resourceVersion is used.
        let is_list = obj.get("items").is_some_and(Value::is_array);
        let (rv, cont, remaining) = if is_list {
            let m = obj.get("metadata");
            (
                m.and_then(|m| m.get("resourceVersion")),
                m.and_then(|m| m.get("continue")),
                m.and_then(|m| m.get("remainingItemCount")),
            )
        } else {
            (
                obj.get("metadata").and_then(|m| m.get("resourceVersion")),
                None,
                None,
            )
        };
        table.metadata = TableMetadata {
            resource_version: rv
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(String::from),
            continue_token: cont
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(String::from),
            remaining_item_count: remaining.and_then(Value::as_i64),
        };

        if is_list {
            let items = obj["items"].as_array().cloned().unwrap_or_default();
            table.rows = items.into_iter().map(|item| self.row(item)).collect();
        } else {
            table.rows = vec![self.row(obj.clone())];
        }
        table
    }

    /// One `rowFn` call of `ConvertToTable`.
    fn row(&self, item: Value) -> TableRow {
        let name = item
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let mut cells: Vec<Value> = Vec::with_capacity(1 + self.additional_columns.len());
        cells.push(Value::String(name));
        let custom_headers = &self.headers[1..];
        for (i, column) in self.additional_columns.iter().enumerate() {
            let results = match find_results(column, &item) {
                Ok(r) if !r.is_empty() && !r[0].is_empty() => r,
                _ => {
                    cells.push(Value::Null);
                    continue;
                }
            };
            // Only simple JSON paths are supported, so at most one result is
            // expected; extra values are ignored, as upstream's
            // `results[0][0]` does.
            let value = &results[0][0];
            let ty = custom_headers[i].column_type.as_str();
            if ty == "string" {
                cells.push(print_result(value).map_or(Value::Null, Value::String));
            } else {
                cells.push(cell_for_json_value(ty, value));
            }
        }
        TableRow {
            cells,
            object: Some(item),
        }
    }
}

/// `cellForJSONValue`.
pub fn cell_for_json_value(header_type: &str, value: &Value) -> Value {
    if value.is_null() {
        return Value::Null;
    }
    match header_type {
        "integer" => match value {
            Value::Number(n) => match n.as_i64() {
                Some(i) => Value::from(i),
                // float64 -> int64(typed); an out-of-range uint64 decodes to
                // float64 upstream too.
                None => Value::from(n.as_f64().unwrap_or(0.0) as i64),
            },
            _ => Value::Null,
        },
        "number" => match value {
            Value::Number(n) => n
                .as_f64()
                .and_then(serde_json::Number::from_f64)
                .map_or(Value::Null, Value::Number),
            _ => Value::Null,
        },
        "boolean" => match value {
            Value::Bool(_) => value.clone(),
            _ => Value::Null,
        },
        "string" => match value {
            Value::String(_) => value.clone(),
            _ => Value::Null,
        },
        "date" => match value {
            // `metav1.Time.UnmarshalQueryParameter`: "" is the zero time,
            // otherwise RFC3339; `ConvertToHumanReadableDateType` renders the
            // zero time as `<unknown>`.
            Value::String(s) if s.is_empty() => Value::String("<unknown>".to_string()),
            Value::String(s) => match chrono::DateTime::parse_from_rfc3339(s) {
                Ok(t) => {
                    let t = t.with_timezone(&chrono::Utc);
                    let zero = t.timestamp() == -62_135_596_800;
                    Value::String(humanize_age(if zero { None } else { Some(t) }))
                }
                Err(_) => Value::String("<invalid>".to_string()),
            },
            _ => Value::Null,
        },
        _ => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// JSONPath evaluation (jsonpath.go) with AllowMissingKeys(true).
// ---------------------------------------------------------------------------

type EvalResult = Result<Vec<Value>, String>;

/// The `JSONPath` evaluator state of jsonpath.go: the `range`/`end` block
/// counters and the last `end` node reached (identified by its absolute index
/// in the template, since upstream compares `&nodes[k]` pointers into one
/// backing array).
#[derive(Default)]
struct Evaluator {
    begin_range: usize,
    in_range: usize,
    end_range: usize,
    last_end_node: Option<usize>,
}

/// `FindResults`: one result set per top-level node of the template.
pub fn find_results(root: &ListNode, data: &Value) -> Result<Vec<Vec<Value>>, String> {
    Evaluator::default().find(&root.nodes, 0, vec![data.clone()])
}

impl Evaluator {
    /// `FindResults` (jsonpath.go:81-135). `nodes` is the remaining template
    /// (upstream reassigns `parser.Root.Nodes = nodes[i+1:]` before recursing);
    /// `base` is its offset in the original template.
    fn find(
        &mut self,
        nodes: &[Node],
        base: usize,
        cur: Vec<Value>,
    ) -> Result<Vec<Vec<Value>>, String> {
        let mut full = Vec::new();
        let mut i = 0;
        while i < nodes.len() {
            let results = self.walk(&cur, &nodes[i])?;

            // encounter an end node, break the current block
            if self.end_range > 0 && self.end_range <= self.in_range {
                self.end_range -= 1;
                self.last_end_node = Some(base + i);
                break;
            }
            // encounter a range node, start a range loop
            if self.begin_range > 0 {
                self.begin_range -= 1;
                self.in_range += 1;
                if !results.is_empty() {
                    for value in results {
                        full.extend(self.find(&nodes[i + 1..], base + i + 1, vec![value])?);
                    }
                } else {
                    // No results: still process the nodes within the range so
                    // the position advances to the end node.
                    self.find(&nodes[i + 1..], base + i + 1, vec![Value::Null])?;
                }
                self.in_range -= 1;

                // Fast forward to resume after the most recent end node.
                for k in i + 1..nodes.len() {
                    if self.last_end_node == Some(base + k) {
                        i = k;
                        break;
                    }
                }
                i += 1;
                continue;
            }
            full.push(results);
            i += 1;
        }
        Ok(full)
    }

    /// `walk`.
    fn walk(&mut self, input: &[Value], node: &Node) -> EvalResult {
        match node {
            Node::List(l) => self.eval_list(input, l),
            Node::Text(t) => Ok(vec![Value::String(t.clone())]),
            Node::Field(name) => eval_field(input, name),
            Node::Array(p) => eval_array(input, p),
            Node::Filter {
                left,
                right,
                operator,
            } => self.eval_filter(input, left, right, operator),
            Node::Wildcard => Ok(eval_wildcard(input)),
            Node::Recursive => Ok(eval_recursive(input)),
            Node::Union(lists) => {
                let mut out = Vec::new();
                for l in lists {
                    out.extend(self.eval_list(input, l)?);
                }
                Ok(out)
            }
            // `evalInt` / `evalFloat` / `evalBool`: one literal per input.
            Node::Int(i) => Ok(input.iter().map(|_| Value::from(*i)).collect()),
            Node::Float(f) => Ok(input
                .iter()
                .map(|_| serde_json::Number::from_f64(*f).map_or(Value::Null, Value::Number))
                .collect()),
            Node::Bool(b) => Ok(input.iter().map(|_| Value::Bool(*b)).collect()),
            Node::Identifier(id) => self.eval_identifier(input, id),
        }
    }

    /// `evalList`: each node runs over the previous node's results.
    fn eval_list(&mut self, input: &[Value], list: &ListNode) -> EvalResult {
        let mut cur = input.to_vec();
        for node in &list.nodes {
            cur = self.walk(&cur, node)?;
        }
        Ok(cur)
    }

    /// `evalIdentifier`: only `range` and `end` are recognised.
    fn eval_identifier(&mut self, input: &[Value], name: &str) -> EvalResult {
        match name {
            "range" => {
                self.begin_range += 1;
                Ok(input.to_vec())
            }
            "end" => {
                if self.in_range > 0 {
                    self.end_range += 1;
                    Ok(Vec::new())
                } else {
                    Err("not in range, nothing to end".to_string())
                }
            }
            _ => Err(format!("unrecognized identifier {name}")),
        }
    }

    /// `evalFilter`: keeps the elements of each input array for which
    /// `left <operator> right` holds (or, for `exists`, `left` is non-empty).
    fn eval_filter(
        &mut self,
        input: &[Value],
        left: &ListNode,
        right: &ListNode,
        operator: &str,
    ) -> EvalResult {
        let mut results = Vec::new();
        for value in input {
            let Value::Array(items) = value else {
                return Err(format!(
                    "{value} is not array or slice and cannot be filtered"
                ));
            };
            for item in items {
                let temp = std::slice::from_ref(item);
                let lefts = self.eval_list(temp, left);

                // case exists
                if operator == "exists" {
                    if lefts.is_ok_and(|l| !l.is_empty()) {
                        results.push(item.clone());
                    }
                    continue;
                }
                let lefts = lefts?;
                if lefts.is_empty() {
                    continue;
                }
                if lefts.len() > 1 {
                    return Err("can only compare one element at a time".to_string());
                }
                let rights = self.eval_list(temp, right)?;
                if rights.is_empty() {
                    continue;
                }
                if rights.len() > 1 {
                    return Err("can only compare one element at a time".to_string());
                }
                let (l, r) = (&lefts[0], &rights[0]);
                let pass = match operator {
                    "<" => template_less(l, r)?,
                    ">" => template_greater(l, r)?,
                    "==" => template_equal(l, r)?,
                    "!=" => !template_equal(l, r)?,
                    "<=" => template_less_equal(l, r)?,
                    ">=" => template_greater_equal(l, r)?,
                    _ => return Err(format!("unrecognized filter operator {operator}")),
                };
                if pass {
                    results.push(item.clone());
                }
            }
        }
        Ok(results)
    }
}

// ---------------------------------------------------------------------------
// `k8s.io/client-go/third_party/forked/golang/template/funcs.go` comparisons.
// ---------------------------------------------------------------------------

/// `kind` of `basicKind`: JSON decoded into `interface{}` by the apiserver
/// holds `int64` (intKind), `uint64` beyond `i64::MAX` (uintKind) and
/// `float64` (floatKind); parser literals are `int`, `float64`, `bool`,
/// `string`. Anything else is `errBadComparisonType`.
enum Basic<'a> {
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    Str(&'a str),
}

fn basic_kind(v: &Value) -> Result<Basic<'_>, String> {
    match v {
        Value::Bool(b) => Ok(Basic::Bool(*b)),
        Value::String(s) => Ok(Basic::Str(s)),
        Value::Number(n) => Ok(if let Some(i) = n.as_i64() {
            Basic::Int(i)
        } else if let Some(u) = n.as_u64() {
            Basic::Uint(u)
        } else {
            Basic::Float(n.as_f64().unwrap_or(0.0))
        }),
        _ => Err("invalid type for comparison".to_string()),
    }
}

const BAD_COMPARISON: &str = "incompatible types for comparison";

/// `Equal` (one comparand).
fn template_equal(a: &Value, b: &Value) -> Result<bool, String> {
    use Basic::*;
    Ok(match (basic_kind(a)?, basic_kind(b)?) {
        (Bool(x), Bool(y)) => x == y,
        (Float(x), Float(y)) => x == y,
        (Int(x), Int(y)) => x == y,
        (Str(x), Str(y)) => x == y,
        (Uint(x), Uint(y)) => x == y,
        (Int(x), Uint(y)) => x >= 0 && x as u64 == y,
        (Uint(x), Int(y)) => y >= 0 && x == y as u64,
        _ => return Err(BAD_COMPARISON.to_string()),
    })
}

/// `Less`.
fn template_less(a: &Value, b: &Value) -> Result<bool, String> {
    use Basic::*;
    Ok(match (basic_kind(a)?, basic_kind(b)?) {
        (Bool(_), Bool(_)) => return Err("invalid type for comparison".to_string()),
        (Float(x), Float(y)) => x < y,
        (Int(x), Int(y)) => x < y,
        (Str(x), Str(y)) => x < y,
        (Uint(x), Uint(y)) => x < y,
        (Int(x), Uint(y)) => x < 0 || (x as u64) < y,
        (Uint(x), Int(y)) => y >= 0 && x < y as u64,
        _ => return Err(BAD_COMPARISON.to_string()),
    })
}

/// `LessEqual`.
fn template_less_equal(a: &Value, b: &Value) -> Result<bool, String> {
    if template_less(a, b)? {
        return Ok(true);
    }
    template_equal(a, b)
}

/// `Greater`.
fn template_greater(a: &Value, b: &Value) -> Result<bool, String> {
    Ok(!template_less_equal(a, b)?)
}

/// `GreaterEqual`.
fn template_greater_equal(a: &Value, b: &Value) -> Result<bool, String> {
    Ok(!template_less(a, b)?)
}

/// The children `evalWildcard` / `evalRecursive` enumerate: a map's values, an
/// array's elements, or a string's bytes (`value.Index(i)` on a string is a
/// `uint8`).
fn children(v: &Value) -> Vec<Value> {
    match v {
        Value::Object(m) => m.values().cloned().collect(),
        Value::Array(a) => a.clone(),
        Value::String(s) => s.bytes().map(Value::from).collect(),
        _ => Vec::new(),
    }
}

/// `evalRecursive`: every value that has children, depth first, each followed
/// by the recursion over its children. Leaf scalars are not included.
fn eval_recursive(input: &[Value]) -> Vec<Value> {
    let mut result = Vec::new();
    for value in input {
        let kids = children(value);
        if !kids.is_empty() {
            result.push(value.clone());
            result.extend(eval_recursive(&kids));
        }
    }
    result
}

/// `evalField` with `allowMissingKeys`: a missing key (or a non-object input)
/// yields no results; a JSON `null` input is skipped like a nil pointer.
fn eval_field(input: &[Value], name: &str) -> EvalResult {
    let mut out = Vec::new();
    for v in input {
        if let Value::Object(m) = v {
            if let Some(found) = m.get(name) {
                out.push(found.clone());
            }
        }
    }
    Ok(out)
}

/// `evalWildcard`.
fn eval_wildcard(input: &[Value]) -> Vec<Value> {
    input.iter().flat_map(children).collect()
}

/// `evalArray`: `[i]`, `[a:b]`, `[a:b:step]` with Go's bounds errors.
fn eval_array(input: &[Value], node: &[jsonpath::ParamsEntry; 3]) -> EvalResult {
    let mut result = Vec::new();
    for v in input {
        let arr = match v {
            Value::Null => continue,
            Value::Array(a) => a,
            _ => return Err("value is not array or slice".to_string()),
        };
        let len = arr.len() as i64;
        let mut p = *node;
        if !p[0].known {
            p[0].value = 0;
        }
        if p[0].value < 0 {
            p[0].value += len;
        }
        if !p[1].known {
            p[1].value = len;
        }
        if p[1].value < 0 || (p[1].value == 0 && p[1].derived) {
            p[1].value += len;
        }
        if p[1].value != p[0].value {
            if p[0].value >= len || p[0].value < 0 {
                return Err(format!(
                    "array index out of bounds: index {}, length {}",
                    p[0].value, len
                ));
            }
            if p[1].value > len || p[1].value < 0 {
                return Err(format!(
                    "array index out of bounds: index {}, length {}",
                    p[1].value - 1,
                    len
                ));
            }
            if p[0].value > p[1].value {
                return Err(format!(
                    "starting index {} is greater than ending index {}",
                    p[0].value, p[1].value
                ));
            }
        } else {
            return Ok(result);
        }
        let step = if p[2].known {
            if p[2].value <= 0 {
                return Err("step must be > 0".to_string());
            }
            p[2].value as usize
        } else {
            1
        };
        let slice = &arr[p[0].value as usize..p[1].value as usize];
        result.extend(slice.iter().step_by(step).cloned());
    }
    Ok(result)
}

/// `PrintResults` for one value (`evalToText` / `json.Marshal`): maps and
/// slices print as compact JSON, scalars through `fmt.Fprint`. `None` is a
/// print error (the cell becomes `null`).
fn print_result(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(match n.as_i64() {
            Some(i) => i.to_string(),
            None => go_float(n.as_f64().unwrap_or(0.0)),
        }),
        Value::Array(_) | Value::Object(_) => {
            let s = serde_json::to_string(v).ok()?;
            // `encoding/json` escapes HTML and the JS line separators.
            Some(
                s.replace('<', "\\u003c")
                    .replace('>', "\\u003e")
                    .replace('&', "\\u0026")
                    .replace('\u{2028}', "\\u2028")
                    .replace('\u{2029}', "\\u2029"),
            )
        }
    }
}

/// Go's `%v` for a `float64`: `strconv.FormatFloat(f, 'g', -1, 64)`
/// (`ftoa.go formatDigits`), which uses exponent form when the decimal
/// exponent is below -4 or at least 6 (`eprec = 6` for the shortest form).
fn go_float(f: f64) -> String {
    if f == 0.0 {
        return "0".to_string();
    }
    let e = format!("{f:e}");
    let (mant, exp) = e.split_once('e').unwrap_or((&e, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    if !(-4..6).contains(&exp) {
        let sign = if exp < 0 { '-' } else { '+' };
        return format!("{mant}e{sign}{:02}", exp.abs());
    }
    format!("{f}")
}
