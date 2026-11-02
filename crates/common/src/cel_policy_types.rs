//! The typed CEL environment of a ValidatingAdmissionPolicy, for the shadow
//! checker: `buildEnvSet` / `CheckExpression` of
//! `plugin/policy/validating/typechecking.go`, the hand-built `request` and
//! namespace types of `plugin/cel/compile.go` (`BuildRequestType`,
//! `BuildNamespaceType`) and the `variables` composition of
//! `plugin/cel/composition.go` (`CompositionEnv.AddField`,
//! `convertCelTypeToDeclType`), all release-1.35.
//!
//! Upstream type-checks an expression once per GVK its `matchConstraints`
//! select, with `object` / `oldObject` typed by that GVK and `params` typed by
//! the `paramKind`. This module checks one expression in one such context;
//! resolving the GVKs and schemas is the caller's.

use crate::cel_checker_shadow::checker_env_set;
use crate::cel_versions::{EnvSet, EnvType};
use cel::parser::Parser;
use rusternetes_cel_checker as checker;
pub use rusternetes_cel_checker::decl::DeclType;
use rusternetes_cel_checker::decl::{DeclField, DeclTypeProvider};
use rusternetes_cel_checker::DType;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

fn field(name: &str, ty: DeclType, required: bool) -> (String, DeclField) {
    (
        name.to_string(),
        DeclField {
            name: name.to_string(),
            ty,
            required,
        },
    )
}

fn string() -> DeclType {
    DeclType::simple(DType::Str)
}

fn list_of(t: DeclType) -> DeclType {
    DeclType::list(t, -1)
}

/// `BuildRequestType` (`plugin/cel/compile.go:48-89`).
pub fn request_type() -> DeclType {
    let obj = |name: &str, fields: Vec<(String, DeclField)>| {
        DeclType::object(name, fields.into_iter().collect())
    };
    let gvk = obj(
        "kubernetes.GroupVersionKind",
        vec![
            field("group", string(), true),
            field("version", string(), true),
            field("kind", string(), true),
        ],
    );
    let gvr = obj(
        "kubernetes.GroupVersionResource",
        vec![
            field("group", string(), true),
            field("version", string(), true),
            field("resource", string(), true),
        ],
    );
    let user_info = obj(
        "kubernetes.UserInfo",
        vec![
            field("username", string(), false),
            field("uid", string(), false),
            field("groups", list_of(string()), false),
            field("extra", DeclType::map(list_of(string()), -1), false),
        ],
    );
    obj(
        "kubernetes.AdmissionRequest",
        vec![
            field("kind", gvk.clone(), true),
            field("resource", gvr.clone(), true),
            field("subResource", string(), false),
            field("requestKind", gvk, true),
            field("requestResource", gvr, true),
            field("requestSubResource", string(), false),
            field("name", string(), true),
            field("namespace", string(), false),
            field("operation", string(), true),
            field("userInfo", user_info, true),
            field("dryRun", DeclType::simple(DType::Bool), false),
            field("options", DeclType::simple(DType::Dyn), false),
        ],
    )
}

/// `BuildNamespaceType` (`plugin/cel/compile.go:94-131`).
pub fn namespace_type() -> DeclType {
    let obj = |name: &str, fields: Vec<(String, DeclField)>| {
        DeclType::object(name, fields.into_iter().collect())
    };
    let ts = || DeclType::simple(DType::Timestamp);
    let int = || DeclType::simple(DType::Int);
    let str_map = || DeclType::map(string(), -1);
    let spec = obj(
        "kubernetes.NamespaceSpec",
        vec![field("finalizers", list_of(string()), true)],
    );
    let condition = obj(
        "kubernetes.NamespaceCondition",
        vec![
            field("status", string(), true),
            field("type", string(), true),
            field("lastTransitionTime", ts(), true),
            field("message", string(), true),
            field("reason", string(), true),
        ],
    );
    let status = obj(
        "kubernetes.NamespaceStatus",
        vec![
            field("conditions", list_of(condition), true),
            field("phase", string(), true),
        ],
    );
    let metadata = obj(
        "kubernetes.NamespaceMetadata",
        vec![
            field("name", string(), true),
            field("generateName", string(), true),
            field("namespace", string(), true),
            field("labels", str_map(), true),
            field("annotations", str_map(), true),
            field("UID", string(), true),
            field("creationTimestamp", ts(), true),
            field("deletionGracePeriodSeconds", int(), true),
            field("deletionTimestamp", ts(), true),
            field("generation", int(), true),
            field("resourceVersion", string(), true),
            field("finalizers", list_of(string()), true),
        ],
    );
    obj(
        "kubernetes.Namespace",
        vec![
            field("metadata", metadata, true),
            field("spec", spec, true),
            field("status", status, true),
        ],
    )
}

/// `convertCelTypeToDeclType` (`composition.go:236-276`): the declaration of a
/// variable from the output type of its expression. Anything that is not a
/// scalar, list or map (structs, optionals, errors) is `dyn`.
pub fn convert_output_type(t: Option<&DType>) -> DeclType {
    let Some(t) = t else {
        return DeclType::simple(DType::Dyn);
    };
    match t {
        DType::Any
        | DType::Bool
        | DType::Bytes
        | DType::Double
        | DType::Duration
        | DType::Int
        | DType::Null
        | DType::Str
        | DType::Timestamp
        | DType::Uint => DeclType::simple(t.clone()),
        DType::List(e) => list_of(convert_output_type(Some(e))),
        DType::Map(k, v) if **k == DType::Str => DeclType::map(convert_output_type(Some(v)), -1),
        DType::Map(k, v) => DeclType::simple(DType::Map(
            k.clone(),
            Box::new(convert_output_type(Some(v)).dtype()),
        )),
        _ => DeclType::simple(DType::Dyn),
    }
}

/// How `params` is declared.
#[derive(Clone, Debug)]
pub enum ParamsType {
    /// No `paramKind`: `params` is undeclared.
    None,
    /// A `paramKind` whose schema is not resolvable. Upstream's type checker
    /// leaves `params` undeclared here (`buildEnvSet`: `hasParams &&
    /// types.params != nil`); the admission compiler declares it `dyn`
    /// (`compile.go`), and that is what the shadow run follows, so an
    /// unresolvable kind does not report every use of `params`.
    Dyn,
    /// `declType(paramsGVK)`.
    Typed(DeclType),
}

/// What an expression of a policy is checked against, for one matched GVK.
#[derive(Clone, Debug)]
pub struct PolicyTypeContext {
    /// `declType(gvk)`: `SchemaDeclType(schema, true)`.
    pub object: DeclType,
    pub params: ParamsType,
    /// `spec.variables` as `(name, expression)`, in order.
    pub variables: Vec<(String, String)>,
}

/// `generateUniqueTypeName` (`typechecking.go`).
fn unique_type_name(kind: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!("{kind}{}", NEXT.fetch_add(1, Ordering::Relaxed))
}

impl PolicyTypeContext {
    /// Names the object types of a resolved schema uniquely
    /// (`MaybeAssignTypeName(generateUniqueTypeName(gvk.Kind))`).
    pub fn named(kind: &str, decl: &DeclType) -> DeclType {
        decl.maybe_assign_type_name(&unique_type_name(kind))
    }
}

fn parse(expression: &str) -> Option<cel::common::ast::IdedExpr> {
    let source = expression.to_string();
    // The antlr4rust parser panics on some invalid expressions.
    std::panic::catch_unwind(|| Parser::new().enable_optional_syntax(true).parse(&source))
        .ok()?
        .ok()
}

/// `buildEnvSet` plus the `variables` composition for one expression, then the
/// check: `variables` have the fields of the variables compiled before it
/// (`CompileAndStoreVariable`), typed by their output.
///
/// `None` when `expression` does not parse. `has_authorizer` is
/// `OptionalVariableDeclarations.HasAuthorizer`.
pub fn check_policy_expression(
    set: &EnvSet,
    env_type: EnvType,
    ctx: &PolicyTypeContext,
    has_authorizer: bool,
    expression: &str,
) -> Option<Vec<String>> {
    let parsed = parse(expression)?;
    let mut fields: BTreeMap<String, DeclField> = BTreeMap::new();
    for (name, expr) in &ctx.variables {
        // The output type of a variable that does not compile is `nil`: `dyn`.
        let out = parse(expr).and_then(|p| {
            let (ast, errs) = run(set, env_type, ctx, has_authorizer, &fields, &p);
            errs.is_empty()
                .then(|| ast.and_then(|a| a.output_type().cloned()))?
        });
        fields.insert(
            name.clone(),
            DeclField {
                name: name.clone(),
                ty: convert_output_type(out.as_ref()),
                required: true,
            },
        );
    }
    let (_, errs) = run(set, env_type, ctx, has_authorizer, &fields, &parsed);
    Some(errs)
}

/// One check in the environment with the given `variables` fields.
fn run(
    set: &EnvSet,
    env_type: EnvType,
    ctx: &PolicyTypeContext,
    has_authorizer: bool,
    variable_fields: &BTreeMap<String, DeclField>,
    parsed: &cel::common::ast::IdedExpr,
) -> (Option<checker::CheckedAst>, Vec<String>) {
    let (env_set, ct) = checker_env_set(set, env_type);
    let version = set.compatibility_version();
    let keywords =
        env_type == EnvType::StoredExpressions || (version.major, version.minor) >= (1, 31);
    let namespace = namespace_type();
    let request = request_type();
    // Registered without fields, fields resolvable (`with_late_fields`).
    let variables = DeclType::object("variables", variable_fields.clone());
    let mut roots = vec![
        namespace.clone(),
        request.clone(),
        ctx.object.clone(),
        DeclType::object("variables", BTreeMap::new()),
    ];
    if let ParamsType::Typed(p) = &ctx.params {
        roots.push(p.clone());
    }
    let provider = DeclTypeProvider::new(&roots, Arc::new(checker::MapProvider::new()), keywords)
        .with_late_fields("variables", variables.clone());
    let mut env = env_set.env(ct, checker::Container::default(), Arc::new(provider));
    let mut declare = |name: &str, t: DType| {
        env.add_ident(checker::VariableDecl::new(name, t)).ok();
    };
    declare("namespaceObject", namespace.dtype());
    declare("request", request.dtype());
    declare("object", ctx.object.dtype());
    declare("oldObject", ctx.object.dtype());
    declare("variables", variables.dtype());
    match &ctx.params {
        ParamsType::None => {}
        ParamsType::Dyn => declare("params", DType::Dyn),
        ParamsType::Typed(p) => declare("params", p.dtype()),
    }
    if has_authorizer {
        declare(
            "authorizer",
            DType::Struct("kubernetes.authorization.Authorizer".into()),
        );
    }
    let (ast, errors) = checker::check(parsed, &mut env, &checker::NoLocations);
    let msgs = errors.errors().iter().map(|e| e.message.clone()).collect();
    (Some(ast), msgs)
}
