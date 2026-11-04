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
//! [`rusternetes_common::jsonpath`] with `AllowMissingKeys(true)`. Filters
//! (`[?(...)]`), recursive descent (`..`) and `range`/`end` evaluate to an
//! error, which `ConvertToTable` renders as a `null` cell, exactly as it does
//! for any other evaluation error.
//!
//! Not ported: the `Desired`/`Actual` columns upstream serves for the scale
//! subresource (`getScaleColumnsForVersion`), and `TableOptions.includeObject`
//! (upstream applies it in the response writer, `handlers/response.go asTable`,
//! not in this convertor; rows keep the full object like every other kind).

use rusternetes_common::jsonpath::{self, ListNode, Node};
use rusternetes_common::resources::{CustomResourceColumnDefinition, CustomResourceDefinition};
use rusternetes_middleware::table::{
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

/// `FindResults`: one result set per top-level node of the template.
pub fn find_results(root: &ListNode, data: &Value) -> Result<Vec<Vec<Value>>, String> {
    let mut full = Vec::new();
    for node in &root.nodes {
        full.push(walk(std::slice::from_ref(data), node)?);
    }
    Ok(full)
}

/// `walk`.
fn walk(input: &[Value], node: &Node) -> EvalResult {
    match node {
        Node::List(l) => eval_list(input, l),
        Node::Text(t) => Ok(vec![Value::String(t.clone())]),
        Node::Field(name) => eval_field(input, name),
        Node::Array(p) => eval_array(input, p),
        Node::Wildcard => Ok(eval_wildcard(input)),
        Node::Union(lists) => {
            let mut out = Vec::new();
            for l in lists {
                out.extend(eval_list(input, l)?);
            }
            Ok(out)
        }
        Node::Int(i) => Ok(vec![Value::from(*i)]),
        Node::Float(f) => Ok(vec![
            serde_json::Number::from_f64(*f).map_or(Value::Null, Value::Number)
        ]),
        Node::Bool(b) => Ok(vec![Value::Bool(*b)]),
        Node::Identifier(id) => Err(format!("unrecognized identifier {id}")),
        Node::Filter { .. } => Err("filter expressions are not supported".to_string()),
        Node::Recursive => Err("recursive descent is not supported".to_string()),
    }
}

/// `evalList`: each node runs over the previous node's results.
fn eval_list(input: &[Value], list: &ListNode) -> EvalResult {
    let mut cur = input.to_vec();
    for node in &list.nodes {
        cur = walk(&cur, node)?;
    }
    Ok(cur)
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
    let mut out = Vec::new();
    for v in input {
        match v {
            Value::Object(m) => out.extend(m.values().cloned()),
            Value::Array(a) => out.extend(a.iter().cloned()),
            _ => {}
        }
    }
    out
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
