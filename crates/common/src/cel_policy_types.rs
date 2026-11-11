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
use k3rs_cel_checker as checker;
pub use k3rs_cel_checker::decl::DeclType;
use k3rs_cel_checker::decl::{DeclField, DeclTypeProvider};
use k3rs_cel_checker::DType;
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

/// The parsed expression with its `SourceInfo`, which locates the checker's
/// errors (`SourceInfo.GetStartLocation`).
type Parsed = (cel::common::ast::IdedExpr, cel::common::ast::SourceInfo);

fn parse(set: &EnvSet, env_type: EnvType, expression: &str) -> Option<Parsed> {
    let source = expression.to_string();
    let (env_set, ct) = checker_env_set(set, env_type);
    // The antlr4rust parser panics on some invalid expressions.
    std::panic::catch_unwind(|| env_set.parser(ct).parse_with_source_info(&source))
        .ok()?
        .ok()
}

/// Which of the optional declarations of the environment an expression is
/// checked in: `plugincel.OptionalVariableDeclarations` (`compile.go:139-143`).
#[derive(Clone, Copy, Debug, Default)]
pub struct CheckOptions {
    /// `HasAuthorizer`: `authorizer` is declared.
    pub has_authorizer: bool,
    /// `HasPatchTypes`: the `Object` / `JSONPatch` types and the `jsonpatch`
    /// library (`hasPatchTypes`, `compile.go:293-300`).
    pub has_patch_types: bool,
    /// `createEnvForOpts` also declares `authorizer.requestResource`
    /// (`compile.go:257-262`); the VAP type checker's `buildEnvSet`
    /// (`typechecking.go:419-423`) does not.
    pub request_resource_authorizer: bool,
    /// The stateless compiler `validateMatchConditionsExpression` uses has no
    /// `variables` (`validation.go:1108`).
    pub no_variables: bool,
}

/// The verdict of one check: the checker's errors and, when it type-checks,
/// the output type of the expression (`ast.OutputType()`).
#[derive(Clone, Debug, Default)]
pub struct PolicyCheck {
    pub errors: Vec<String>,
    /// The errors as `Errors.ToDisplayString` renders them: `line:column`,
    /// the source line and an indicator (`compile.go:186`,
    /// `"compilation failed: "+issues.String()`). Empty when it type-checks.
    pub display: String,
    pub output: Option<DType>,
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
    let opts = CheckOptions {
        has_authorizer,
        ..CheckOptions::default()
    };
    check_policy_expression_with(set, env_type, ctx, &opts, expression).map(|c| c.errors)
}

/// [`check_policy_expression`] with the full set of declarations, and the
/// output type of the expression.
pub fn check_policy_expression_with(
    set: &EnvSet,
    env_type: EnvType,
    ctx: &PolicyTypeContext,
    opts: &CheckOptions,
    expression: &str,
) -> Option<PolicyCheck> {
    let parsed = parse(set, env_type, expression)?;
    let mut fields: BTreeMap<String, DeclField> = BTreeMap::new();
    // `CompileAndStoreVariables` compiles the variables with the plain
    // options, never with the patch types (`compilePolicy`,
    // `mutating/compilation.go:36-46`).
    let variable_opts = CheckOptions {
        has_patch_types: false,
        ..*opts
    };
    for (name, expr) in &ctx.variables {
        // The output type of a variable that does not compile is `nil`: `dyn`.
        let out = parse(set, env_type, expr).and_then(|p| {
            let (ast, errs, _) = run(set, env_type, ctx, &variable_opts, &fields, &p, expr);
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
    let (ast, errors, display) = run(set, env_type, ctx, opts, &fields, &parsed, expression);
    let output = errors
        .is_empty()
        .then(|| ast.and_then(|a| a.output_type().cloned()))
        .flatten();
    Some(PolicyCheck {
        errors,
        display,
        output,
    })
}

/// One check in the environment with the given `variables` fields.
fn run(
    set: &EnvSet,
    env_type: EnvType,
    ctx: &PolicyTypeContext,
    opts: &CheckOptions,
    variable_fields: &BTreeMap<String, DeclField>,
    (parsed, source_info): &Parsed,
    source: &str,
) -> (Option<checker::CheckedAst>, Vec<String>, String) {
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
    // `common.ResolverEnvOption(&mutation.DynamicTypeResolver{})` resolves the
    // names the registered types do not.
    let fallback: Arc<dyn checker::Provider> = if opts.has_patch_types {
        Arc::new(checker::PatchTypeProvider)
    } else {
        Arc::new(checker::MapProvider::new())
    };
    let provider = DeclTypeProvider::new(&roots, fallback, keywords)
        .with_late_fields("variables", variables.clone());
    let mut env = env_set.env(ct, checker::Container::default(), Arc::new(provider));
    let mut declare = |name: &str, t: DType| {
        env.add_ident(checker::VariableDecl::new(name, t)).ok();
    };
    declare("namespaceObject", namespace.dtype());
    declare("request", request.dtype());
    declare("object", ctx.object.dtype());
    declare("oldObject", ctx.object.dtype());
    if !opts.no_variables {
        declare("variables", variables.dtype());
    }
    match &ctx.params {
        ParamsType::None => {}
        ParamsType::Dyn => declare("params", DType::Dyn),
        ParamsType::Typed(p) => declare("params", p.dtype()),
    }
    if opts.has_authorizer {
        declare(
            "authorizer",
            DType::Struct("kubernetes.authorization.Authorizer".into()),
        );
        if opts.request_resource_authorizer {
            declare(
                "authorizer.requestResource",
                DType::Struct("kubernetes.authorization.ResourceCheck".into()),
            );
        }
    }
    if opts.has_patch_types {
        env.add_library(checker::table::jsonpatch);
    }
    let locator = checker::SourceLocator::new(source_info, source);
    let (ast, errors) = checker::check(parsed, &mut env, &locator);
    let msgs = errors.errors().iter().map(|e| e.message.clone()).collect();
    (Some(ast), msgs, errors.to_display_string(Some(source)))
}
