//! `ConvertV2ToV3`: converts an OpenAPI v2 (Swagger) document into OpenAPI v3,
//! over JSON values.
//!
//! Ported from `k8s.io/kube-openapi/pkg/openapiconv/convert.go`
//! (`ConvertV2ToV3` and every `Convert*` helper, in the same order) and
//! `pkg/builder3/util/util.go` (`WrapRefs`). The output is the JSON the Go
//! `spec3.OpenAPI` marshals to: `omitempty` / `omitzero` fields are left out
//! when empty, vendor extensions (`x-*`) of the source objects are kept where
//! the Go code copies `VendorExtensible`.

use serde_json::{json, Map, Value};

use crate::kube_openapi::Walker;

/// `OpenAPIV2DefPrefix`.
pub const OPENAPI_V2_DEF_PREFIX: &str = "#/definitions/";
/// `OpenAPIV3DefPrefix`.
pub const OPENAPI_V3_DEF_PREFIX: &str = "#/components/schemas/";

fn s(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn truthy(v: &Value, key: &str) -> bool {
    v.get(key).and_then(Value::as_bool).unwrap_or(false)
}

/// Sets `key` only when `value` is a non-empty string (`omitempty`).
fn put_str(m: &mut Map<String, Value>, key: &str, value: String) {
    if !value.is_empty() {
        m.insert(key.to_string(), Value::String(value));
    }
}

/// The `x-` keys of `v` (`spec.VendorExtensible`).
fn copy_extensions(from: &Value, to: &mut Map<String, Value>) {
    if let Some(o) = from.as_object() {
        for (k, v) in o {
            if k.len() > 2 && k[..2].eq_ignore_ascii_case("x-") {
                to.insert(k.clone(), v.clone());
            }
        }
    }
}

/// `ConvertV2ToV3`.
pub fn convert_v2_to_v3(v2_spec: &Value) -> Value {
    let mut out = Map::new();
    out.insert("openapi".to_string(), json!("3.0.0"));
    out.insert(
        "info".to_string(),
        v2_spec.get("info").cloned().unwrap_or(Value::Null),
    );
    if let Some(p) = convert_paths(v2_spec.get("paths")) {
        out.insert("paths".to_string(), p);
    }
    out.insert(
        "components".to_string(),
        convert_components(
            v2_spec.get("securityDefinitions"),
            v2_spec.get("definitions"),
            v2_spec.get("responses"),
            v2_spec.get("produces"),
        ),
    );
    if let Some(ed) = convert_external_documentation(v2_spec.get("externalDocs")) {
        out.insert("externalDocs".to_string(), ed);
    }
    Value::Object(out)
}

/// `ConvertExternalDocumentation`.
pub fn convert_external_documentation(v2_ed: Option<&Value>) -> Option<Value> {
    let ed = v2_ed.filter(|v| v.is_object())?;
    let mut m = Map::new();
    put_str(&mut m, "description", s(ed, "description"));
    m.insert("url".to_string(), Value::String(s(ed, "url")));
    Some(Value::Object(m))
}

/// `ConvertComponents`.
pub fn convert_components(
    v2_security_definitions: Option<&Value>,
    v2_definitions: Option<&Value>,
    v2_responses: Option<&Value>,
    produces: Option<&Value>,
) -> Value {
    let mut components = Map::new();
    if let Some(defs) = v2_definitions.and_then(Value::as_object) {
        let schemas: Map<String, Value> = defs
            .iter()
            .map(|(k, v)| (k.clone(), convert_schema(v)))
            .collect();
        if !schemas.is_empty() {
            components.insert("schemas".to_string(), Value::Object(schemas));
        }
    }
    if let Some(defs) = v2_security_definitions.and_then(Value::as_object) {
        let schemes: Map<String, Value> = defs
            .iter()
            .map(|(k, v)| (k.clone(), convert_security_scheme(v)))
            .collect();
        if !schemes.is_empty() {
            components.insert("securitySchemes".to_string(), Value::Object(schemes));
        }
    }
    if let Some(resps) = v2_responses.and_then(Value::as_object) {
        let responses: Map<String, Value> = resps
            .iter()
            .map(|(k, v)| (k.clone(), convert_response(Some(v), produces)))
            .collect();
        if !responses.is_empty() {
            components.insert("responses".to_string(), Value::Object(responses));
        }
    }
    Value::Object(components)
}

/// `ConvertSchema`.
pub fn convert_schema(v2_schema: &Value) -> Value {
    let Some(obj) = v2_schema.as_object() else {
        return v2_schema.clone();
    };
    let mut v3 = obj.clone();

    if let Some(ref_string) = obj
        .get("$ref")
        .and_then(Value::as_str)
        .filter(|r| !r.is_empty())
    {
        if let Some(idx) = ref_string.find(OPENAPI_V2_DEF_PREFIX) {
            v3.insert(
                "$ref".to_string(),
                Value::String(format!(
                    "{OPENAPI_V3_DEF_PREFIX}{}",
                    &ref_string[idx + OPENAPI_V2_DEF_PREFIX.len()..]
                )),
            );
        } else {
            tracing::error!("Error: Swagger V2 Ref {ref_string} does not contain #/definitions");
        }
    }

    if let Some(props) = obj.get("properties").and_then(Value::as_object) {
        let converted: Map<String, Value> = props
            .iter()
            .map(|(k, v)| (k.clone(), convert_schema(v)))
            .collect();
        v3.insert("properties".to_string(), Value::Object(converted));
    }
    match obj.get("items") {
        Some(items) if items.is_object() => {
            v3.insert("items".to_string(), convert_schema(items));
        }
        Some(Value::Array(list)) => {
            v3.insert("items".to_string(), convert_schema_list(list));
        }
        _ => {}
    }
    for key in ["additionalProperties", "additionalItems"] {
        if let Some(v) = obj.get(key).filter(|v| v.is_object()) {
            v3.insert(key.to_string(), convert_schema(v));
        }
    }

    let mut out = Value::Object(v3);
    wrap_refs(&mut out);
    out
}

/// `ConvertSchemaList`.
pub fn convert_schema_list(v2_schema_list: &[Value]) -> Value {
    Value::Array(v2_schema_list.iter().map(convert_schema).collect())
}

/// `WrapRefs` (builder3/util): "wraps OpenAPI V3 Schema refs that contain
/// sibling elements. AllOf is used to wrap the Ref to prevent references from
/// having sibling elements".
pub fn wrap_refs(schema: &mut Value) {
    let mut wrap = |sc: &mut Value| {
        let Some(obj) = sc.as_object_mut() else {
            return;
        };
        let Some(r) = obj
            .get("$ref")
            .and_then(Value::as_str)
            .filter(|r| !r.is_empty())
            .map(str::to_string)
        else {
            return;
        };
        // `!reflect.DeepEqual(*schema, spec.Schema{Ref: schema.Ref})`
        if obj.len() == 1 {
            return;
        }
        obj.remove("$ref");
        obj.insert("allOf".to_string(), json!([{ "$ref": r }]));
    };
    let mut w = Walker {
        schema_callback: Some(&mut wrap),
        ref_callback: None,
    };
    w.walk_schema(schema);
}

/// `ConvertSecurityScheme`.
pub fn convert_security_scheme(v2: &Value) -> Value {
    let mut m = Map::new();
    copy_extensions(v2, &mut m);
    put_str(&mut m, "type", s(v2, "type"));
    put_str(&mut m, "description", s(v2, "description"));
    put_str(&mut m, "name", s(v2, "name"));
    put_str(&mut m, "in", s(v2, "in"));
    let flow = s(v2, "flow");
    if !flow.is_empty() {
        let mut f = Map::new();
        put_str(&mut f, "authorizationUrl", s(v2, "authorizationUrl"));
        put_str(&mut f, "tokenUrl", s(v2, "tokenUrl"));
        if let Some(scopes) = v2
            .get("scopes")
            .and_then(Value::as_object)
            .filter(|o| !o.is_empty())
        {
            f.insert("scopes".to_string(), Value::Object(scopes.clone()));
        }
        m.insert("flows".to_string(), json!({ flow: Value::Object(f) }));
    }
    Value::Object(m)
}

/// `ConvertPaths`.
pub fn convert_paths(v2_paths: Option<&Value>) -> Option<Value> {
    let paths = v2_paths.and_then(Value::as_object)?;
    let mut out = Map::new();
    copy_extensions(v2_paths.unwrap(), &mut out);
    for (k, v) in paths {
        if k.starts_with('/') {
            out.insert(k.clone(), convert_path_item(v));
        }
    }
    Some(Value::Object(out))
}

/// `ConvertPathItem`.
pub fn convert_path_item(v2_path_item: &Value) -> Value {
    let mut path = Map::new();
    copy_extensions(v2_path_item, &mut path);
    // `ConvertRefableParameter`-less: `Refable: v2pathItem.Refable`.
    put_str(&mut path, "$ref", s(v2_path_item, "$ref"));
    for op in ["get", "put", "post", "delete", "options", "head", "patch"] {
        if let Some(o) = convert_operation(v2_path_item.get(op)) {
            path.insert(op.to_string(), o);
        }
    }
    if let Some(params) = v2_path_item.get("parameters").and_then(Value::as_array) {
        let converted: Vec<Value> = params.iter().map(convert_parameter).collect();
        if !converted.is_empty() {
            path.insert("parameters".to_string(), Value::Array(converted));
        }
    }
    Value::Object(path)
}

fn string_list(v: Option<&Value>) -> Option<Vec<String>> {
    v.and_then(Value::as_array).map(|a| {
        a.iter()
            .filter_map(|x| x.as_str().map(str::to_string))
            .collect()
    })
}

/// `ConvertOperation`.
pub fn convert_operation(v2_operation: Option<&Value>) -> Option<Value> {
    let v2 = v2_operation.filter(|v| v.is_object())?;
    let mut op = Map::new();
    copy_extensions(v2, &mut op);
    put_str(&mut op, "description", s(v2, "description"));
    if let Some(ed) = convert_external_documentation(v2.get("externalDocs")) {
        op.insert("externalDocs".to_string(), ed);
    }
    if let Some(tags) = v2
        .get("tags")
        .and_then(Value::as_array)
        .filter(|t| !t.is_empty())
    {
        op.insert("tags".to_string(), Value::Array(tags.clone()));
    }
    put_str(&mut op, "summary", s(v2, "summary"));
    if truthy(v2, "deprecated") {
        op.insert("deprecated".to_string(), Value::Bool(true));
    }
    put_str(&mut op, "operationId", s(v2, "operationId"));

    let consumes = string_list(v2.get("consumes"));
    let produces = v2.get("produces");
    let mut parameters = Vec::new();
    for param in v2
        .get("parameters")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let schema = param.get("schema").filter(|v| !v.is_null());
        if let (true, Some(schema)) = (s(param, "name") == "body", schema) {
            let mut content = Map::new();
            for consumer in consumes.iter().flatten() {
                content.insert(
                    consumer.clone(),
                    json!({ "schema": convert_schema(schema) }),
                );
            }
            let mut body = Map::new();
            if !content.is_empty() {
                body.insert("content".to_string(), Value::Object(content));
            }
            op.insert("requestBody".to_string(), Value::Object(body));
        } else {
            parameters.push(convert_parameter(param));
        }
    }
    if !parameters.is_empty() {
        op.insert("parameters".to_string(), Value::Array(parameters));
    }

    let empty = Value::Object(Map::new());
    let v2_responses = v2.get("responses").unwrap_or(&empty);
    let mut responses = Map::new();
    copy_extensions(v2_responses, &mut responses);
    if let Some(d) = convert_response_opt(v2_responses.get("default"), produces) {
        responses.insert("default".to_string(), d);
    }
    if let Some(m) = v2_responses.as_object() {
        for (k, v) in m {
            if k.parse::<i64>().is_ok() {
                responses.insert(k.clone(), convert_response(Some(v), produces));
            }
        }
    }
    op.insert("responses".to_string(), Value::Object(responses));
    Some(Value::Object(op))
}

fn convert_response_opt(v: Option<&Value>, produces: Option<&Value>) -> Option<Value> {
    v.filter(|v| !v.is_null())
        .map(|v| convert_response(Some(v), produces))
}

/// `ConvertResponse`.
pub fn convert_response(v2_response: Option<&Value>, produces: Option<&Value>) -> Value {
    let Some(v2) = v2_response.filter(|v| v.is_object()) else {
        return Value::Null;
    };
    let mut response = Map::new();
    copy_extensions(v2, &mut response);
    let r = convert_refable(s(v2, "$ref"), "#/responses/", "#/components/responses/");
    put_str(&mut response, "$ref", r);
    put_str(&mut response, "description", s(v2, "description"));
    if let Some(schema) = v2.get("schema").filter(|v| !v.is_null()) {
        let mut content = Map::new();
        for producer in string_list(produces).iter().flatten() {
            content.insert(
                producer.clone(),
                json!({ "schema": convert_schema(schema) }),
            );
        }
        if !content.is_empty() {
            response.insert("content".to_string(), Value::Object(content));
        }
    }
    Value::Object(response)
}

/// `ConvertParameter`.
pub fn convert_parameter(v2_param: &Value) -> Value {
    let mut param = Map::new();
    copy_extensions(v2_param, &mut param);
    let r = convert_refable(
        s(v2_param, "$ref"),
        "#/parameters/",
        "#/components/parameters/",
    );
    put_str(&mut param, "$ref", r);
    put_str(&mut param, "name", s(v2_param, "name"));
    put_str(&mut param, "description", s(v2_param, "description"));
    put_str(&mut param, "in", s(v2_param, "in"));
    if truthy(v2_param, "required") {
        param.insert("required".to_string(), Value::Bool(true));
    }
    if truthy(v2_param, "allowEmptyValue") {
        param.insert("allowEmptyValue".to_string(), Value::Bool(true));
    }
    match v2_param.get("schema").filter(|v| !v.is_null()) {
        Some(schema) => {
            param.insert("schema".to_string(), convert_schema(schema));
        }
        None => {
            // "Convert SimpleSchema into Schema"
            let mut schema = Map::new();
            schema.insert("type".to_string(), Value::String(s(v2_param, "type")));
            put_str(&mut schema, "format", s(v2_param, "format"));
            if truthy(v2_param, "uniqueItems") {
                schema.insert("uniqueItems".to_string(), Value::Bool(true));
            }
            param.insert("schema".to_string(), Value::Object(schema));
        }
    }
    Value::Object(param)
}

/// `ConvertRefableParameter` / `ConvertRefableResponse`:
/// `strings.Replace(ref, from, to, 1)` when the reference is set.
fn convert_refable(r: String, from: &str, to: &str) -> String {
    if r.is_empty() {
        return r;
    }
    r.replacen(from, to, 1)
}
