//! The type model: `common/types/types.go` (`types.Type` and its kinds) and
//! `checker/format.go` (`FormatCELType`).

/// `types.Type`: the kinds the declarations and the selector type pass use.
#[derive(Clone, Debug, PartialEq)]
pub enum DType {
    /// `types.ErrorType`: the type recorded for an expression that failed to
    /// check, so that checking can make forward progress.
    Error,
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

    /// `isError`.
    pub fn is_error(&self) -> bool {
        matches!(self, DType::Error)
    }

    /// `isDynOrError`.
    pub fn is_dyn_or_error(&self) -> bool {
        self.is_dyn() || self.is_error()
    }

    /// `maybeUnwrapOptional`: the parameter of `optional_type(T)`, if any.
    pub fn maybe_unwrap_optional(&self) -> (&DType, bool) {
        match self {
            DType::Opaque(n, ps) if n == "optional_type" && ps.len() == 1 => (&ps[0], true),
            other => (other, false),
        }
    }

    /// `types.NewOptionalType`.
    pub fn optional(t: DType) -> DType {
        DType::Opaque("optional_type".into(), vec![t])
    }

    /// `checker/format.go` `FormatCELType`.
    pub fn name(&self) -> String {
        match self {
            DType::Error => "!error!".into(),
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
#[derive(Clone, Debug)]
pub struct Overload {
    /// Overload id. cel-go uses hand-written ids (`add_int64`); this port
    /// synthesizes `<function>(<arg types>)` since no consumer of the
    /// reference map exists yet.
    pub id: String,
    /// The cel-go overload id (`common/overloads`) when the cost estimator
    /// has a rule for this overload, see [`crate::overload_ids`].
    pub cel_id: Option<&'static str>,
    pub member: bool,
    /// Argument types; for a member overload the first is the receiver.
    pub args: Vec<DType>,
    pub result: DType,
}

impl Overload {
    /// The id `checker.Cost` sees: the cel-go id where mapped, else [`Self::id`].
    pub fn cost_id(&self) -> String {
        self.cel_id.map_or_else(|| self.id.clone(), str::to_string)
    }
}
