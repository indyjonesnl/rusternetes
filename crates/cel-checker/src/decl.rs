//! OpenAPI-schema-based declaration types: the checker half of
//! `k8s.io/apiserver/pkg/cel/types.go` (`DeclType`, `DeclField`,
//! `DeclTypeProvider`, `FieldTypeMap`, `MaybeAssignTypeName`) and the identifier
//! escaping of `k8s.io/apiserver/pkg/cel/escaping.go` (`Escape`).
//!
//! `MinSerializedSize` / `MaxElements` are carried for the cost estimator.
//! Not ported, because only the interpreter uses them: enum values and
//! defaults as values, the `ref.Val` default values and trait masks.

use crate::env::Provider;
use crate::types::DType;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

const CEL_RESERVED_SYMBOLS: &[&str] = &[
    "true",
    "false",
    "null",
    "in",
    "as",
    "break",
    "const",
    "continue",
    "else",
    "for",
    "function",
    "if",
    "import",
    "let",
    "loop",
    "package",
    "namespace",
    "return",
    "var",
    "void",
    "while",
];

/// `cel.Escape`: the CEL identifier a schema property name is exposed as, or
/// `None` when the name cannot be expressed (`escaping.go:Escape`).
pub fn escape(ident: &str) -> Option<String> {
    let first = ident.chars().next()?;
    if first.is_ascii_digit() {
        return None;
    }
    if CEL_RESERVED_SYMBOLS.contains(&ident) {
        return Some(format!("__{ident}__"));
    }
    // The `expandMatcher` `(__|[-./]|[^a-zA-Z0-9-./_])`, left to right.
    let mut out = String::with_capacity(ident.len());
    let chars: Vec<char> = ident.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '_' && chars.get(i + 1) == Some(&'_') {
            out.push_str("__underscores__");
            i += 2;
            continue;
        }
        match c {
            '.' => out.push_str("__dot__"),
            '-' => out.push_str("__dash__"),
            '/' => out.push_str("__slash__"),
            c if c.is_ascii_alphanumeric() || c == '_' => out.push(c),
            _ => return None,
        }
        i += 1;
    }
    Some(out)
}

/// `apiservercel.DeclField`. Enum values and defaults are not kept: the static
/// estimator only needs what `SchemaDeclType` derives from them (the string
/// enum length and whether a required field has a default), computed there.
#[derive(Clone, Debug, PartialEq)]
pub struct DeclField {
    pub name: String,
    pub ty: DeclType,
    pub required: bool,
}

/// What a [`DeclType`] is.
#[derive(Clone, Debug, PartialEq)]
pub enum DeclKind {
    /// A scalar (`bool`, `int`, `double`, `string`, `bytes`, `timestamp`,
    /// `duration`) or `dyn` (`x-kubernetes-int-or-string`).
    Simple(DType),
    /// `NewListType`.
    List(Box<DeclType>),
    /// `NewMapType(StringType, elem, ...)`.
    Map(Box<DeclType>),
    /// `NewObjectType(name, fields)`. Schema objects start out named
    /// `"object"`; [`DeclType::maybe_assign_type_name`] makes the names unique.
    Object {
        name: String,
        fields: BTreeMap<String, DeclField>,
    },
}

/// `apiservercel.DeclType`: the type of an OpenAPI schema node, with the size
/// bounds the cost estimator reads.
#[derive(Clone, Debug, PartialEq)]
pub struct DeclType {
    pub kind: DeclKind,
    /// `MinSerializedSize`: the smallest number of bytes a value serializes to
    /// in JSON.
    pub min_serialized_size: i64,
    /// `MaxElements`: the most elements of a list / map, or the longest string
    /// / bytes value (in characters), a request can hold. `-1` where upstream
    /// leaves it unset (the hand-built types), which `EstimateSize` reads as
    /// `MaxElements` of 0 for the unset field.
    pub max_elements: i64,
}

/// `limits.go`: `MinStringSize`, `MinBoolSize`, `MinNumberSize`, ... .
pub mod limits {
    /// `celconfig.MaxRequestSizeBytes`.
    pub const MAX_REQUEST_SIZE_BYTES: i64 = 3 * 1024 * 1024;
    pub const MAX_DURATION_SIZE_JSON: i64 = 32;
    pub const MAX_DATETIME_SIZE_JSON: i64 = 32;
    pub const MIN_DURATION_SIZE_JSON: i64 = 3;
    pub const JSON_DATE_SIZE: i64 = 12;
    pub const MIN_DATETIME_SIZE_JSON: i64 = 21;
    pub const MIN_STRING_SIZE: i64 = 2;
    pub const MIN_BOOL_SIZE: i64 = 4;
    pub const MIN_NUMBER_SIZE: i64 = 1;
}

impl DeclType {
    /// `NewSimpleTypeWithMinSize`.
    pub fn simple_with_min_size(t: DType, min_serialized_size: i64) -> DeclType {
        DeclType {
            kind: DeclKind::Simple(t),
            min_serialized_size,
            max_elements: 0,
        }
    }

    /// The package-level simple types of `types.go` (`BoolType`, `IntType`,
    /// `StringType`, `DynType`, ...) with their `MinSerializedSize`.
    pub fn simple(t: DType) -> DeclType {
        let min = match &t {
            DType::Bool => limits::MIN_BOOL_SIZE,
            DType::Bytes | DType::Str => limits::MIN_STRING_SIZE,
            DType::Double | DType::Int | DType::Uint => limits::MIN_NUMBER_SIZE,
            DType::Duration => limits::MIN_DURATION_SIZE_JSON,
            DType::Timestamp => limits::MIN_DATETIME_SIZE_JSON,
            DType::Null => 4,
            _ => 1,
        };
        DeclType::simple_with_min_size(t, min)
    }

    /// `NewListType`: a list is always at least `[]`.
    pub fn list(elem: DeclType, max_items: i64) -> DeclType {
        DeclType {
            kind: DeclKind::List(Box::new(elem)),
            min_serialized_size: 2,
            max_elements: max_items,
        }
    }

    /// `NewMapType(StringType, elem, maxProperties)`: a map is always at least
    /// `{}`.
    pub fn map(elem: DeclType, max_properties: i64) -> DeclType {
        DeclType {
            kind: DeclKind::Map(Box::new(elem)),
            min_serialized_size: 2,
            max_elements: max_properties,
        }
    }

    /// `NewObjectType`: at least `{}`; the caller raises `min_serialized_size`
    /// for the required properties it adds.
    pub fn object(name: &str, fields: BTreeMap<String, DeclField>) -> DeclType {
        DeclType {
            kind: DeclKind::Object {
                name: name.to_string(),
                fields,
            },
            min_serialized_size: 2,
            max_elements: 0,
        }
    }

    /// `DeclType.CelType`.
    pub fn dtype(&self) -> DType {
        match &self.kind {
            DeclKind::Simple(t) => t.clone(),
            DeclKind::List(e) => DType::List(Box::new(e.dtype())),
            DeclKind::Map(e) => DType::Map(Box::new(DType::Str), Box::new(e.dtype())),
            DeclKind::Object { name, .. } => DType::Struct(name.clone()),
        }
    }

    /// `ElemType` of a list or map.
    pub fn elem_type(&self) -> Option<&DeclType> {
        match &self.kind {
            DeclKind::List(e) | DeclKind::Map(e) => Some(e),
            _ => None,
        }
    }

    /// `KeyType`: `string` for a map.
    pub fn key_type(&self) -> Option<DeclType> {
        matches!(self.kind, DeclKind::Map(_)).then(|| DeclType::simple(DType::Str))
    }

    /// `Fields` of an object.
    pub fn fields(&self) -> Option<&BTreeMap<String, DeclField>> {
        match &self.kind {
            DeclKind::Object { fields, .. } => Some(fields),
            _ => None,
        }
    }

    /// `DeclType.MaybeAssignTypeName`: gives every `"object"` below this type a
    /// unique, path-derived name.
    pub fn maybe_assign_type_name(&self, name: &str) -> DeclType {
        let kind = match &self.kind {
            DeclKind::Object { name: own, fields } => {
                let name = if own != "object" { own.as_str() } else { name };
                let fields = fields
                    .iter()
                    .map(|(n, f)| {
                        let ty = f.ty.maybe_assign_type_name(&format!("{name}.{n}"));
                        (
                            n.clone(),
                            DeclField {
                                name: n.clone(),
                                ty,
                                required: f.required,
                            },
                        )
                    })
                    .collect();
                DeclKind::Object {
                    name: name.to_string(),
                    fields,
                }
            }
            DeclKind::Map(e) => {
                DeclKind::Map(Box::new(e.maybe_assign_type_name(&format!("{name}.@elem"))))
            }
            DeclKind::List(e) => {
                DeclKind::List(Box::new(e.maybe_assign_type_name(&format!("{name}.@idx"))))
            }
            DeclKind::Simple(_) => return self.clone(),
        };
        DeclType {
            kind,
            min_serialized_size: self.min_serialized_size,
            max_elements: self.max_elements,
        }
    }

    fn type_name(&self) -> Option<&str> {
        match &self.kind {
            DeclKind::Object { name, .. } => Some(name),
            _ => None,
        }
    }
}

/// `FieldTypeMap` / `buildDeclTypes`: every named object type below `t`.
fn field_type_map(path: &str, t: &DeclType) -> HashMap<String, DeclType> {
    let path = match t.type_name() {
        Some(name) if name != "object" => name,
        _ => path,
    };
    let mut out = HashMap::new();
    build_decl_types(path, t, &mut out);
    out
}

fn build_decl_types(path: &str, t: &DeclType, out: &mut HashMap<String, DeclType>) {
    match &t.kind {
        DeclKind::Object { name, fields } => {
            out.insert(name.clone(), t.clone());
            for (n, f) in fields {
                build_decl_types(&format!("{path}.{n}"), &f.ty, out);
            }
        }
        DeclKind::Map(e) => {
            build_decl_types(&format!("{path}.@elem"), e, out);
            out.insert(path.to_string(), t.clone());
        }
        DeclKind::List(e) => {
            build_decl_types(&format!("{path}.@idx"), e, out);
            out.insert(path.to_string(), t.clone());
        }
        DeclKind::Simple(_) => {}
    }
}

/// `apiservercel.DeclTypeProvider`, as a checker [`Provider`].
pub struct DeclTypeProvider {
    registered: HashMap<String, DeclType>,
    /// `WithTypeProvider`: where names this provider does not know go.
    fallback: Arc<dyn Provider>,
    /// `SetRecognizeKeywordAsFieldName` (Kubernetes 1.31 and later).
    recognize_keyword_as_field_name: bool,
}

impl DeclTypeProvider {
    /// `NewDeclTypeProvider(rootTypes...)` + `WithTypeProvider(fallback)`.
    pub fn new(
        root_types: &[DeclType],
        fallback: Arc<dyn Provider>,
        recognize_keyword_as_field_name: bool,
    ) -> Self {
        let mut registered = HashMap::new();
        for t in root_types {
            let path = t.type_name().unwrap_or("object").to_string();
            registered.extend(field_type_map(&path, t));
        }
        Self {
            registered,
            fallback,
            recognize_keyword_as_field_name,
        }
    }
}

impl DeclTypeProvider {
    /// Makes the fields of the registered object type `name` resolvable
    /// without registering the types below them.
    ///
    /// Upstream's `variables` composition type (`composition.go`
    /// `NewCompositionEnv` / `AddField`) is registered with no fields and gets
    /// them added later, through the pointer the provider shares: field
    /// lookups see them, but the name registry (`allTypesForDecl`, computed
    /// once) never holds `variables.<name>`. Registering those names would
    /// make `variables.x` resolve as a type identifier.
    pub fn with_late_fields(mut self, name: &str, t: DeclType) -> Self {
        self.registered.insert(name.to_string(), t);
        self
    }
}

impl Provider for DeclTypeProvider {
    fn find_struct_type(&self, name: &str) -> Option<DType> {
        match self.registered.get(name) {
            Some(t) => Some(t.dtype()),
            None => self.fallback.find_struct_type(name),
        }
    }

    /// `DeclTypeProvider.FindStructFieldType`.
    fn find_struct_field_type(&self, struct_name: &str, field: &str) -> Option<DType> {
        let Some(st) = self.registered.get(struct_name) else {
            return self.fallback.find_struct_field_type(struct_name, field);
        };
        if let DeclKind::Object { fields, .. } = &st.kind {
            let mut found = fields.get(field);
            if self.recognize_keyword_as_field_name
                && found.is_none()
                && CEL_RESERVED_SYMBOLS.contains(&field)
            {
                found = fields.get(&format!("__{field}__"));
            }
            if let Some(f) = found {
                return Some(f.ty.dtype());
            }
        }
        // This could be a dynamic map.
        if let DeclKind::Map(e) = &st.kind {
            return Some(e.dtype());
        }
        None
    }

    fn find_ident(&self, name: &str) -> Option<DType> {
        self.fallback.find_ident(name)
    }
}
