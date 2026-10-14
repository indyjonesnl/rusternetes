//! The CEL `kubernetes.authz` / `kubernetes.authzSelectors` libraries and the
//! `authorizer` / `authorizer.requestResource` activation values (#3300).
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/cel/library/authz.go`
//! (`Authz`, `AuthzSelectors`, `authzLibraryDecls` :251-290,
//! `authzSelectorsLibraryDecls` :351-358, the bindings :372-596, the value
//! types :622-743) and the activation in
//! `admission/plugin/cel/activation.go:44-81`, which builds
//! `authorizerVal = library.NewAuthorizerVal(versionedAttr.GetUserInfo(),
//! inputs.Authorizer)` and `requestResourceAuthorizerVal =
//! library.NewResourceAuthorizerVal(...)` only when `inputs.Authorizer != nil`.
//!
//! The objects are `cel::Value::Opaque` values that expose receiver functions
//! only. The functions are registered on every context
//! ([`register`], from `cel_struct::new_context`); they are reachable only
//! through a value of the right type, which exists only when the caller bound
//! the authorizer (the `HasAuthorizer` environments).
//!
//! Rust expression of the Go mechanism (not a divergence): `Authorize` is
//! `async` here and a CEL function is synchronous, so a `check` blocks the
//! evaluating thread on the call (`block_in_place` on a multi-thread runtime,
//! as cel-go's `context.TODO()` call blocks its goroutine). On a
//! current-thread runtime the call runs on a helper thread, which can not
//! drive that runtime's IO or timers.
//!
//! Library versioning by compatibility version (`kubernetes.authz` v1.0,
//! `kubernetes.authzSelectors` v1.31, `base.go:131-141`) is `cel_versions`'s
//! table; evaluation here binds the full library.

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use cel::extractors::This;
use cel::objects::Opaque;
use cel::{Context, ExecutionError, Value};

use crate::auth::UserInfo;
use crate::authz::{Authorizer, Opinion, RequestAttributes};
use crate::feature_gates::{self, Feature};
use crate::validation::objectmeta::{name_is_dns_label, name_is_dns_subdomain};
use crate::Error;

/// `AuthorizerVarName` (`admission/plugin/cel/compile.go:39`).
pub const AUTHORIZER_VAR: &str = "authorizer";
/// `RequestResourceAuthorizerVarName` (`compile.go:40`).
pub const REQUEST_RESOURCE_AUTHORIZER_VAR: &str = "authorizer.requestResource";

const AUTHORIZER_TYPE: &str = "kubernetes.authorization.Authorizer";
const PATH_CHECK_TYPE: &str = "kubernetes.authorization.PathCheck";
const GROUP_CHECK_TYPE: &str = "kubernetes.authorization.GroupCheck";
const RESOURCE_CHECK_TYPE: &str = "kubernetes.authorization.ResourceCheck";
const DECISION_TYPE: &str = "kubernetes.authorization.Decision";

/// `authorizer.Authorizer`, compared by identity.
#[derive(Clone)]
struct AuthzHandle(Arc<dyn Authorizer>);

impl PartialEq for AuthzHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for AuthzHandle {}
impl fmt::Debug for AuthzHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Authorizer")
    }
}

/// `user.Info`, compared by identity.
#[derive(Clone, Debug)]
struct UserHandle(Arc<UserInfo>);

impl PartialEq for UserHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for UserHandle {}

/// `authorizerVal`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct AuthorizerVal {
    user: UserHandle,
    authorizer: AuthzHandle,
}

/// `pathCheckVal`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct PathCheckVal {
    authorizer: AuthorizerVal,
    path: String,
}

/// `groupCheckVal`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct GroupCheckVal {
    authorizer: AuthorizerVal,
    group: String,
}

/// `resourceCheckVal`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ResourceCheckVal {
    group_check: GroupCheckVal,
    resource: String,
    subresource: String,
    namespace: String,
    name: String,
    field_selector: String,
    label_selector: String,
}

/// `decisionVal`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct DecisionVal {
    err: Option<String>,
    allowed: bool,
    reason: String,
}

macro_rules! receiver_only {
    ($t:ty, $name:expr) => {
        impl Opaque for $t {
            fn runtime_type_name(&self) -> &str {
                $name
            }
        }
    };
}
receiver_only!(AuthorizerVal, AUTHORIZER_TYPE);
receiver_only!(PathCheckVal, PATH_CHECK_TYPE);
receiver_only!(GroupCheckVal, GROUP_CHECK_TYPE);
receiver_only!(ResourceCheckVal, RESOURCE_CHECK_TYPE);
receiver_only!(DecisionVal, DECISION_TYPE);

fn opaque<T: Opaque>(v: T) -> Value {
    Value::Opaque(Arc::new(v))
}

/// `NewAuthorizerVal`.
pub fn new_authorizer_val(user: &UserInfo, authorizer: Arc<dyn Authorizer>) -> Value {
    opaque(AuthorizerVal {
        user: UserHandle(Arc::new(user.clone())),
        authorizer: AuthzHandle(authorizer),
    })
}

/// `NewResourceAuthorizerVal`: the `Resource` is the request's own resource
/// (`GetResource().Group`, `.Resource`, `GetSubresource`, `GetNamespace`,
/// `GetName`).
pub fn new_resource_authorizer_val(
    user: &UserInfo,
    authorizer: Arc<dyn Authorizer>,
    group: &str,
    resource: &str,
    subresource: &str,
    namespace: &str,
    name: &str,
) -> Value {
    let a = AuthorizerVal {
        user: UserHandle(Arc::new(user.clone())),
        authorizer: AuthzHandle(authorizer),
    };
    opaque(ResourceCheckVal {
        group_check: GroupCheckVal {
            authorizer: a,
            group: group.to_string(),
        },
        resource: resource.to_string(),
        subresource: subresource.to_string(),
        namespace: namespace.to_string(),
        name: name.to_string(),
        field_selector: String::new(),
        label_selector: String::new(),
    })
}

/// Run `f` to completion from synchronous code; see the module docs.
fn block_on<F>(f: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    use tokio::runtime::{Builder, Handle, RuntimeFlavor};
    match Handle::try_current() {
        Ok(h) if h.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| h.block_on(f))
        }
        Ok(h) => std::thread::scope(|s| match s.spawn(|| h.block_on(f)).join() {
            Ok(v) => v,
            Err(p) => std::panic::resume_unwind(p),
        }),
        Err(_) => Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
            .block_on(f),
    }
}

/// `err.Error()` of an `Authorize` error: the authorizer's own message.
fn error_message(e: &Error) -> String {
    match e {
        Error::Authorization(m) => m.clone(),
        other => other.to_string(),
    }
}

/// `authorizer.Authorize` -> `newDecision(decision, err, reason)`
/// (authz.go:673-674, :730-731, :734-736).
fn authorize(a: &AuthorizerVal, attrs: RequestAttributes) -> Value {
    let authorizer = a.authorizer.0.clone();
    let result = block_on(async move { authorizer.authorize_opinion(&attrs).await });
    opaque(match result {
        Ok(Opinion::Allow) => DecisionVal {
            err: None,
            allowed: true,
            reason: String::new(),
        },
        Ok(Opinion::Deny(reason)) => DecisionVal {
            err: None,
            allowed: false,
            reason,
        },
        Ok(Opinion::NoOpinion { reason, error }) => DecisionVal {
            err: error,
            allowed: false,
            reason,
        },
        Err(e) => DecisionVal {
            err: Some(error_message(&e)),
            allowed: false,
            reason: String::new(),
        },
    })
}

impl PathCheckVal {
    /// `pathCheckVal.Authorize` (authz.go:666-675).
    fn authorize(&self, verb: &str) -> Value {
        let attrs = RequestAttributes::new_non_resource(
            (*self.authorizer.user.0).clone(),
            verb,
            self.path.clone(),
        );
        authorize(&self.authorizer, attrs)
    }
}

impl ResourceCheckVal {
    /// `resourceCheckVal.Authorize` (authz.go:698-732).
    fn authorize(&self, verb: &str) -> Value {
        let a = &self.group_check.authorizer;
        let mut attrs = RequestAttributes::new((*a.user.0).clone(), verb, self.resource.clone());
        attrs.api_group = self.group_check.group.clone();
        attrs.api_version = "*".to_string();
        attrs.subresource = Some(self.subresource.clone()).filter(|s| !s.is_empty());
        attrs.namespace = Some(self.namespace.clone()).filter(|s| !s.is_empty());
        attrs.name = Some(self.name.clone()).filter(|s| !s.is_empty());
        if feature_gates::enabled(Feature::AuthorizeWithSelectors) {
            // authz.go:712-727: an empty selector string leaves the
            // requirements unset; a parse error records the parsing error.
            attrs.set_selectors(&self.field_selector, &self.label_selector);
        }
        authorize(a, attrs)
    }
}

fn no_such_overload() -> ExecutionError {
    ExecutionError::NoSuchOverload
}

fn fail(function: &str, msg: &str) -> ExecutionError {
    ExecutionError::function_error(function, msg)
}

fn downcast<T: Opaque>(v: &Value) -> Option<&T> {
    match v {
        Value::Opaque(o) => o.downcast_ref::<T>(),
        _ => None,
    }
}

fn blank(s: &str) -> bool {
    s.trim().is_empty()
}

type R = Result<Value, ExecutionError>;

/// Register the receiver functions of both libraries
/// (`authzLibraryDecls`, `authzSelectorsLibraryDecls`).
pub fn register(ctx: &mut Context<'_>) {
    // authorizerPath (:372-388)
    ctx.add_function("path", |This(this): This<Value>, path: Arc<String>| -> R {
        let a = downcast::<AuthorizerVal>(&this).ok_or_else(no_such_overload)?;
        if blank(&path) {
            return Err(fail("path", "path must not be empty"));
        }
        Ok(opaque(PathCheckVal {
            authorizer: a.clone(),
            path: path.to_string(),
        }))
    });
    // authorizerGroup (:390-402)
    ctx.add_function(
        "group",
        |This(this): This<Value>, group: Arc<String>| -> R {
            let a = downcast::<AuthorizerVal>(&this).ok_or_else(no_such_overload)?;
            Ok(opaque(GroupCheckVal {
                authorizer: a.clone(),
                group: group.to_string(),
            }))
        },
    );
    // authorizerServiceAccount (:404-432)
    ctx.add_function(
        "serviceAccount",
        |This(this): This<Value>, namespace: Arc<String>, name: Arc<String>| -> R {
            let a = downcast::<AuthorizerVal>(&this).ok_or_else(no_such_overload)?;
            if !name_is_dns_subdomain(&name, false).is_empty() {
                return Err(fail("serviceAccount", "Invalid service account name"));
            }
            if !name_is_dns_label(&namespace, false).is_empty() {
                return Err(fail("serviceAccount", "Invalid service account namespace"));
            }
            // `serviceaccount.ServiceAccountInfo.UserInfo()`
            // (authentication/serviceaccount/util.go:132-137).
            let user = UserInfo {
                username: format!("system:serviceaccount:{namespace}:{name}"),
                uid: String::new(),
                groups: vec![
                    "system:serviceaccounts".to_string(),
                    format!("system:serviceaccounts:{namespace}"),
                ],
                extra: Default::default(),
            };
            Ok(opaque(AuthorizerVal {
                user: UserHandle(Arc::new(user)),
                authorizer: a.authorizer.clone(),
            }))
        },
    );
    // groupCheckResource (:434-449)
    ctx.add_function(
        "resource",
        |This(this): This<Value>, resource: Arc<String>| -> R {
            let g = downcast::<GroupCheckVal>(&this).ok_or_else(no_such_overload)?;
            if blank(&resource) {
                return Err(fail("resource", "resource must not be empty"));
            }
            Ok(opaque(ResourceCheckVal {
                group_check: g.clone(),
                resource: resource.to_string(),
                subresource: String::new(),
                namespace: String::new(),
                name: String::new(),
                field_selector: String::new(),
                label_selector: String::new(),
            }))
        },
    );
    // resourceCheckSubresource / Namespace / Name / FieldSelector /
    // LabelSelector (:451-529, :467-497).
    macro_rules! resource_setter {
        ($fn_name:literal, $field:ident) => {
            ctx.add_function($fn_name, |This(this): This<Value>, arg: Arc<String>| -> R {
                let r = downcast::<ResourceCheckVal>(&this).ok_or_else(no_such_overload)?;
                let mut result = r.clone();
                result.$field = arg.to_string();
                Ok(opaque(result))
            });
        };
    }
    resource_setter!("subresource", subresource);
    resource_setter!("namespace", namespace);
    resource_setter!("name", name);
    resource_setter!("fieldSelector", field_selector);
    resource_setter!("labelSelector", label_selector);
    // pathCheckCheck / resourceCheckCheck (:531-557)
    ctx.add_function("check", |This(this): This<Value>, verb: Arc<String>| -> R {
        if let Some(p) = downcast::<PathCheckVal>(&this) {
            return Ok(p.authorize(&verb));
        }
        let r = downcast::<ResourceCheckVal>(&this).ok_or_else(no_such_overload)?;
        Ok(r.authorize(&verb))
    });
    // decisionErrored / Error / Allowed / Reason (:559-596)
    ctx.add_function("errored", |This(this): This<Value>| -> R {
        let d = downcast::<DecisionVal>(&this).ok_or_else(no_such_overload)?;
        Ok(Value::Bool(d.err.is_some()))
    });
    ctx.add_function("error", |This(this): This<Value>| -> R {
        let d = downcast::<DecisionVal>(&this).ok_or_else(no_such_overload)?;
        Ok(Value::String(Arc::new(d.err.clone().unwrap_or_default())))
    });
    ctx.add_function("allowed", |This(this): This<Value>| -> R {
        let d = downcast::<DecisionVal>(&this).ok_or_else(no_such_overload)?;
        Ok(Value::Bool(d.allowed))
    });
    ctx.add_function("reason", |This(this): This<Value>| -> R {
        let d = downcast::<DecisionVal>(&this).ok_or_else(no_such_overload)?;
        Ok(Value::String(Arc::new(d.reason.clone())))
    });
}
