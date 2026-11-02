//! The type model: `common/types/types.go` (`types.Type` and its kinds) and
//! `checker/format.go` (`FormatCELType`).

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
    pub fn is_dyn(&self) -> bool {
        matches!(self, DType::Dyn | DType::Any)
    }

    /// `checker/format.go` `FormatCELType`.
    pub fn name(&self) -> String {
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
    pub fn kind_name(&self) -> String {
        match self {
            DType::Opaque(n, _) | DType::Struct(n) => n.clone(),
            DType::List(_) => "list".into(),
            DType::Map(..) => "map".into(),
            DType::Type(_) => "type".into(),
            other => other.name(),
        }
    }

    pub fn params(&self) -> Vec<&DType> {
        match self {
            DType::List(t) => vec![t],
            DType::Map(k, v) => vec![k, v],
            DType::Opaque(_, ps) => ps.iter().collect(),
            DType::Type(Some(p)) => vec![p],
            _ => vec![],
        }
    }

    /// `types.Kind` equality (a type parameter is one kind whatever its name).
    pub fn same_kind(&self, o: &DType) -> bool {
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
