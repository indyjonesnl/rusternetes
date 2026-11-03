//! `checker/env.go`, `checker/scopes.go`, `checker/options.go` and the
//! identifier/function resolution of `common/containers`: the environment an
//! expression is checked in.

use crate::table;
use crate::types::{DType, Overload};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// `decls.FunctionDecl`: a function name and its overloads.
#[derive(Clone, Debug)]
pub struct FunctionDecl {
    pub name: String,
    pub overloads: Vec<Overload>,
}

/// `decls.VariableDecl`.
#[derive(Clone, Debug, PartialEq)]
pub struct VariableDecl {
    pub name: String,
    pub ty: DType,
}

impl VariableDecl {
    pub fn new(name: impl Into<String>, ty: DType) -> Self {
        Self {
            name: name.into(),
            ty,
        }
    }

    /// `decls.TypeVariable`: the identifier `t.TypeName()` of type `type(t)`.
    pub fn type_variable(t: DType) -> Self {
        Self {
            name: t.kind_name(),
            ty: DType::Type(Some(Box::new(t))),
        }
    }
}

/// `types.Provider`, reduced to what the checker uses: struct types and their
/// field types (`FindStructType`, `FindStructFieldType`) and type identifiers
/// (`FindIdent`). Enum values (`EnumValue`) are not modelled: no Kubernetes
/// environment declares protobuf enums.
pub trait Provider: Send + Sync {
    /// The struct type named `name`.
    fn find_struct_type(&self, name: &str) -> Option<DType>;
    /// The type of `field` on the struct type `struct_name`.
    fn find_struct_field_type(&self, struct_name: &str, field: &str) -> Option<DType>;
    /// A type identifier the provider knows (e.g. a well-known type name).
    fn find_ident(&self, _name: &str) -> Option<DType> {
        None
    }
}

/// A [`Provider`] with no struct types: the `types.NewEmptyRegistry` analogue.
#[derive(Default)]
pub struct EmptyProvider;

impl Provider for EmptyProvider {
    fn find_struct_type(&self, _: &str) -> Option<DType> {
        None
    }
    fn find_struct_field_type(&self, _: &str, _: &str) -> Option<DType> {
        None
    }
}

/// `mutation.DynamicTypeResolver` (`cel/mutation/typeresolver.go:32-42`):
/// `JSONPatch` (`JSONPatchType.Field`, `mutation/jsonpatch.go:100-112`: `op`,
/// `path` and `from` are strings, `value` is `dyn`) and `Object` /
/// `Object.<path>` (`dynamic.ObjectType.Field`, `dynamic/objects.go:58-75`:
/// every field is `dyn`) resolve; nothing else does. A `JSONPatch` field that
/// is not one of the four is an undefined field here; upstream's `Field`
/// answers with a nil type and the construction fails at evaluation
/// (`JSONPatchType.Val`: `unexpected JSONPatchType field`).
pub struct PatchTypeProvider;

impl Provider for PatchTypeProvider {
    fn find_struct_type(&self, name: &str) -> Option<DType> {
        let is_object = name == "Object"
            || name
                .strip_prefix("Object.")
                .is_some_and(|rest| !rest.is_empty());
        (name == "JSONPatch" || is_object).then(|| DType::Struct(name.to_string()))
    }

    fn find_struct_field_type(&self, struct_name: &str, field: &str) -> Option<DType> {
        match struct_name {
            "JSONPatch" => match field {
                "op" | "path" | "from" => Some(DType::Str),
                "value" => Some(DType::Dyn),
                _ => None,
            },
            _ => self.find_struct_type(struct_name).map(|_| DType::Dyn),
        }
    }
}

/// A [`Provider`] over an explicit set of struct types.
#[derive(Default)]
pub struct MapProvider {
    structs: HashMap<String, HashMap<String, DType>>,
}

impl MapProvider {
    pub fn new() -> Self {
        Self::default()
    }

    /// Declare a struct type with its fields.
    pub fn with_struct(mut self, name: &str, fields: &[(&str, DType)]) -> Self {
        self.structs.insert(
            name.to_string(),
            fields
                .iter()
                .map(|(n, t)| (n.to_string(), t.clone()))
                .collect(),
        );
        self
    }
}

impl Provider for MapProvider {
    fn find_struct_type(&self, name: &str) -> Option<DType> {
        self.structs
            .contains_key(name)
            .then(|| DType::Struct(name.to_string()))
    }
    fn find_struct_field_type(&self, s: &str, field: &str) -> Option<DType> {
        self.structs.get(s)?.get(field).cloned()
    }
}

/// `containers.Container`, without aliases and abbreviations (the Kubernetes
/// environments use neither).
#[derive(Clone, Debug, Default)]
pub struct Container {
    name: String,
}

impl Container {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// `Container.ResolveCandidateNames`.
    pub fn resolve_candidate_names(&self, name: &str) -> Vec<String> {
        if let Some(qn) = name.strip_prefix('.') {
            return vec![qn.to_string()];
        }
        if self.name.is_empty() {
            return vec![name.to_string()];
        }
        let mut next = self.name.as_str();
        let mut out = vec![format!("{next}.{name}")];
        while let Some(i) = next.rfind('.') {
            next = &next[..i];
            out.push(format!("{next}.{name}"));
        }
        out.push(name.to_string());
        out
    }
}

/// `checker.Option`s that `NewEnv` reads.
#[derive(Clone, Debug, Default)]
pub struct Options {
    /// `HomogeneousAggregateLiterals`: a list/map literal with mixed element
    /// types is an error instead of `list(dyn)`.
    pub homogeneous_aggregate_literals: bool,
    /// `cel.HomogeneousAggregateLiterals()`: since cel-go 0.2x an AST
    /// validator run after a clean type check (`validator.rs`), not a checker
    /// option; the checker joins mixed element types to `dyn`.
    pub validate_homogeneous_literals: bool,
    /// `HomogeneousAggregateLiteralExemptFunctions`.
    pub homogeneous_exempt_functions: Vec<String>,
    /// `CrossTypeNumericComparisons`.
    pub cross_type_numeric_comparisons: bool,
}

/// One `checker.Group`.
#[derive(Clone, Default)]
struct Group {
    idents: HashMap<String, VariableDecl>,
    functions: HashMap<String, Arc<FunctionDecl>>,
}

/// `checker.Env`. The scope stack is a `Vec` because the checker only ever
/// enters and exits scopes in strict nesting (`Scopes.Push` / `Pop`).
pub struct Env {
    container: Container,
    provider: Arc<dyn Provider>,
    scopes: Vec<Group>,
    pub(crate) homogeneous_literals: bool,
    pub(crate) homogeneous_validator: Option<crate::validator::HomogeneousConfig>,
    cross_type_numeric: bool,
}

/// Macros of `parser.AllMacros` as (function, receiver style, arg count).
const MACROS: [(&str, bool, usize); 8] = [
    ("has", false, 1),
    ("all", true, 2),
    ("exists", true, 2),
    ("exists_one", true, 2),
    ("existsOne", true, 2),
    ("map", true, 2),
    ("map", true, 3),
    ("filter", true, 2),
];

impl Env {
    /// `NewEnv`.
    pub fn new(container: Container, provider: Arc<dyn Provider>, opts: Options) -> Self {
        Self {
            container,
            provider,
            scopes: vec![Group::default()],
            homogeneous_literals: opts.homogeneous_aggregate_literals,
            homogeneous_validator: opts.validate_homogeneous_literals.then(|| {
                crate::validator::HomogeneousConfig {
                    exempt_functions: opts.homogeneous_exempt_functions.clone(),
                }
            }),
            cross_type_numeric: opts.cross_type_numeric_comparisons,
        }
    }

    /// An environment declaring the standard type identifiers
    /// (`stdlib.Types()`) and every function of [`table`] (the standard
    /// library plus the extension and Kubernetes libraries at their newest
    /// version, the environment of a DRA device selector). Versioned
    /// environments are built by [`crate::environment::EnvSet`].
    pub fn with_default_declarations(
        container: Container,
        provider: Arc<dyn Provider>,
        opts: Options,
    ) -> Self {
        let mut env = Self::new(container, provider, opts);
        env.add_std_types();
        env.add_optional_type_ident();
        let mut names: Vec<&String> = table::table().keys().collect();
        names.sort();
        for name in names {
            let f = FunctionDecl {
                name: name.clone(),
                overloads: table::table()[name].clone(),
            };
            env.add_function(f).ok();
        }
        env
    }

    /// `stdlib.Types()`: the type identifiers `int`, `list`, `type`, ...
    pub fn add_std_types(&mut self) {
        let any_list = DType::List(Box::new(DType::Param("A".into())));
        let any_map = DType::Map(
            Box::new(DType::Param("A".into())),
            Box::new(DType::Param("B".into())),
        );
        for t in [
            DType::Bool,
            DType::Bytes,
            DType::Double,
            DType::Duration,
            DType::Int,
            any_list,
            any_map,
            DType::Null,
            DType::Str,
            DType::Timestamp,
            DType::Type(None),
            DType::Uint,
        ] {
            let mut d = VariableDecl::type_variable(t);
            if d.name == "null" {
                d.name = "null_type".into();
            }
            if d.name == "duration" {
                d.name = "google.protobuf.Duration".into();
            }
            if d.name == "timestamp" {
                d.name = "google.protobuf.Timestamp".into();
            }
            self.add_ident(d).ok();
        }
    }

    /// `cel.OptionalTypes()`: the `optional_type` type identifier
    /// (`cel/library.go` `Variable(OptionalType.TypeName(), ...)`).
    pub fn add_optional_type_ident(&mut self) {
        self.add_ident(VariableDecl::type_variable(DType::Opaque(
            "optional_type".into(),
            vec![],
        )))
        .ok();
    }

    /// Declare the functions one library builds (`cel.Lib` /
    /// `cel.Function`): overloads of an already declared function are
    /// merged, as `Env.AddFunctions` does.
    pub fn add_library(&mut self, build: fn(&mut table::Table)) {
        let mut t = table::Table::new();
        build(&mut t);
        let mut names: Vec<String> = t.keys().cloned().collect();
        names.sort();
        for name in names {
            let overloads = t.remove(&name).unwrap_or_default();
            self.add_function(FunctionDecl { name, overloads }).ok();
        }
    }

    pub fn container(&self) -> &Container {
        &self.container
    }

    pub fn provider(&self) -> &dyn Provider {
        self.provider.as_ref()
    }

    /// `Env.AddIdents` for one declaration. Declaring an equivalent
    /// declaration again is accepted; a different one is
    /// `overlapping identifier for name '%s'`.
    pub fn add_ident(&mut self, decl: VariableDecl) -> Result<(), String> {
        let scope = self.scopes.last_mut().expect("scope");
        if let Some(cur) = scope.idents.get(&decl.name) {
            return if *cur == decl {
                Ok(())
            } else {
                Err(format!("overlapping identifier for name '{}'", decl.name))
            };
        }
        scope.idents.insert(decl.name.clone(), decl);
        Ok(())
    }

    /// `Env.AddFunctions` / `setFunction`: overloads of an existing function
    /// are merged; an overload that overlaps a macro is an error.
    pub fn add_function(&mut self, mut f: FunctionDecl) -> Result<(), String> {
        if let Some(cur) = self.find_function_decl(&f.name) {
            let mut merged = (*cur).clone();
            for o in f.overloads.drain(..) {
                if merged.overloads.iter().any(|m| m.id == o.id) {
                    continue;
                }
                merged.overloads.push(o);
            }
            f = merged;
        }
        for o in &f.overloads {
            let args = if o.member {
                o.args.len().saturating_sub(1)
            } else {
                o.args.len()
            };
            if MACROS
                .iter()
                .any(|(n, recv, c)| *n == f.name && *recv == o.member && *c == args)
            {
                return Err(format!(
                    "overlapping macro for name '{}' with {} args",
                    f.name, args
                ));
            }
        }
        let scope = self.scopes.last_mut().expect("scope");
        scope.functions.insert(f.name.clone(), Arc::new(f));
        Ok(())
    }

    fn find_function_decl(&self, name: &str) -> Option<Arc<FunctionDecl>> {
        self.scopes
            .iter()
            .rev()
            .find_map(|g| g.functions.get(name).cloned())
    }

    fn find_ident_decl(&self, name: &str) -> Option<VariableDecl> {
        self.scopes
            .iter()
            .rev()
            .find_map(|g| g.idents.get(name).cloned())
    }

    /// `Env.LookupIdent`: the first container candidate that is a declared
    /// identifier, a struct type (imported as a `type(T)` variable) or a
    /// provider type identifier.
    pub fn lookup_ident(&mut self, name: &str) -> Option<VariableDecl> {
        for cand in self.container.resolve_candidate_names(name) {
            if let Some(d) = self.find_ident_decl(&cand) {
                return Some(d);
            }
            if let Some(t) = self.provider.find_struct_type(&cand) {
                let d = VariableDecl::new(cand, DType::Type(Some(Box::new(t))));
                self.scopes
                    .last_mut()
                    .expect("scope")
                    .idents
                    .insert(d.name.clone(), d.clone());
                return Some(d);
            }
            if let Some(t) = self.provider.find_ident(&cand) {
                let d = VariableDecl::new(cand, DType::Type(Some(Box::new(t))));
                self.scopes
                    .last_mut()
                    .expect("scope")
                    .idents
                    .insert(d.name.clone(), d.clone());
                return Some(d);
            }
        }
        None
    }

    /// `Env.LookupFunction`.
    pub fn lookup_function(&self, name: &str) -> Option<Arc<FunctionDecl>> {
        self.container
            .resolve_candidate_names(name)
            .iter()
            .find_map(|c| self.find_function_decl(c))
    }

    /// `Env.isOverloadDisabled`: without `CrossTypeNumericComparisons` the
    /// mixed int/uint/double ordering overloads
    /// (`crossTypeNumericComparisonOverloads`) are not considered.
    pub(crate) fn is_overload_disabled(&self, function: &str, o: &Overload) -> bool {
        if self.cross_type_numeric || !matches!(function, "_<_" | "_<=_" | "_>_" | "_>=_") {
            return false;
        }
        let numeric = |t: &DType| matches!(t, DType::Int | DType::Uint | DType::Double);
        o.args.len() == 2 && o.args.iter().all(numeric) && o.args[0] != o.args[1]
    }

    /// `Env.enterScope`.
    pub fn enter_scope(&mut self) {
        self.scopes.push(Group::default());
    }

    /// `Env.exitScope`.
    pub fn exit_scope(&mut self) {
        if self.scopes.len() > 1 {
            self.scopes.pop();
        }
    }
}

/// `typeParams` of an overload: the distinct type parameter names of its
/// argument and result types.
pub(crate) fn type_params(o: &Overload) -> Vec<String> {
    fn walk(t: &DType, out: &mut HashSet<String>) {
        if let DType::Param(n) = t {
            out.insert(n.clone());
        }
        for p in t.params() {
            walk(p, out);
        }
    }
    let mut set = HashSet::new();
    for t in o.args.iter().chain(std::iter::once(&o.result)) {
        walk(t, &mut set);
    }
    let mut v: Vec<String> = set.into_iter().collect();
    v.sort();
    v
}
