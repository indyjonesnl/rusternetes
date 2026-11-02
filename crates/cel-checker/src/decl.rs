//! OpenAPI-schema-based declaration types: the checker half of
//! `k8s.io/apiserver/pkg/cel/types.go` (`DeclType`, `DeclField`,
//! `DeclTypeProvider`, `FieldTypeMap`, `MaybeAssignTypeName`) and the identifier
//! escaping of `k8s.io/apiserver/pkg/cel/escaping.go` (`Escape`).
//!
//! Not ported here, because only cost estimation and the interpreter use
//! them: `MinSerializedSize` / `MaxElements`, enum values and defaults, the
//! `ref.Val` default values and trait masks.

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

/// `apiservercel.DeclField`.
#[derive(Clone, Debug, PartialEq)]
pub struct DeclField {
    pub name: String,
    pub ty: DeclType,
    pub required: bool,
}

/// `apiservercel.DeclType`: the type of an OpenAPI schema node.
#[derive(Clone, Debug, PartialEq)]
pub enum DeclType {
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

impl DeclType {
    /// `NewObjectType`.
    pub fn object(name: &str, fields: BTreeMap<String, DeclField>) -> DeclType {
        DeclType::Object {
            name: name.to_string(),
            fields,
        }
    }

    /// `DeclType.CelType`.
    pub fn dtype(&self) -> DType {
        match self {
            DeclType::Simple(t) => t.clone(),
            DeclType::List(e) => DType::List(Box::new(e.dtype())),
            DeclType::Map(e) => DType::Map(Box::new(DType::Str), Box::new(e.dtype())),
            DeclType::Object { name, .. } => DType::Struct(name.clone()),
        }
    }

    /// `DeclType.MaybeAssignTypeName`: gives every `"object"` below this type a
    /// unique, path-derived name.
    pub fn maybe_assign_type_name(&self, name: &str) -> DeclType {
        match self {
            DeclType::Object { name: own, fields } => {
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
                DeclType::Object {
                    name: name.to_string(),
                    fields,
                }
            }
            DeclType::Map(e) => {
                DeclType::Map(Box::new(e.maybe_assign_type_name(&format!("{name}.@elem"))))
            }
            DeclType::List(e) => {
                DeclType::List(Box::new(e.maybe_assign_type_name(&format!("{name}.@idx"))))
            }
            simple => simple.clone(),
        }
    }

    fn type_name(&self) -> Option<&str> {
        match self {
            DeclType::Object { name, .. } => Some(name),
            _ => None,
        }
    }
}

/// `FieldTypeMap` / `buildDeclTypes`: every named object type below `t`.
fn field_type_map(path: &str, t: &DeclType) -> HashMap<String, DeclType> {
    let path = match t {
        DeclType::Object { name, .. } if name != "object" => name.as_str(),
        _ => path,
    };
    let mut out = HashMap::new();
    build_decl_types(path, t, &mut out);
    out
}

fn build_decl_types(path: &str, t: &DeclType, out: &mut HashMap<String, DeclType>) {
    match t {
        DeclType::Object { name, fields } => {
            out.insert(name.clone(), t.clone());
            for (n, f) in fields {
                build_decl_types(&format!("{path}.{n}"), &f.ty, out);
            }
        }
        DeclType::Map(e) => {
            build_decl_types(&format!("{path}.@elem"), e, out);
            out.insert(path.to_string(), t.clone());
        }
        DeclType::List(e) => {
            build_decl_types(&format!("{path}.@idx"), e, out);
            out.insert(path.to_string(), t.clone());
        }
        DeclType::Simple(_) => {}
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
        if let DeclType::Object { fields, .. } = st {
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
        if let DeclType::Map(e) = st {
            return Some(e.dtype());
        }
        None
    }

    fn find_ident(&self, name: &str) -> Option<DType> {
        self.fallback.find_ident(name)
    }
}
