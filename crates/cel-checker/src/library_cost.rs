//! `k8s.io/apiserver/pkg/cel/library/cost.go`: the Kubernetes call-cost and
//! size estimates (`library.CostEstimator`, release-1.35), the
//! [`CostEstimator`] every Kubernetes CEL environment hands to `checker.Cost`.
//!
//! Only the static half is ported (`EstimateCallCost`, `EstimateSize`,
//! `sizeEstimate`, `listElementNode`, `itemsNode`, `selectorCostEstimate`).
//! `CallCost`, `actualSize` and `traversalCost` price a call at run time and
//! need the interpreter's values.
//!
//! cel-go warns of this code: "Any changes to this code impact API
//! compatibility! The estimated cost is used to determine which CEL rules may
//! be written to a CRD and any change (cost increases and cost decreases) are
//! breaking." The arms below follow upstream arm for arm, including its
//! oddities (the URL equality reads the right operand twice).
//!
//! The Kubernetes object types are matched by the names the declaration
//! [`table`](crate::table) registers them under.

use crate::checker::CheckedAst;
use crate::cost::{
    cost, AstNode, CallEstimate, CostEstimate, CostEstimator, CostOptions, OverloadEstimators,
    SizeEstimate, LIST_CREATE_BASE_COST, REGEX_STRING_LENGTH_COST_FACTOR,
    STRING_TRAVERSAL_COST_FACTOR, STRUCT_CREATE_BASE_COST,
};
use crate::overload_ids::{CIDR_CONTAINS_CIDR_STRING, CIDR_CONTAINS_IP_STRING, CIDR_IP};
use crate::types::DType;
use cel::common::ast::{Expr, LiteralValue};

/// `limits.go` `MaxFormatSize`.
const MAX_FORMAT_SIZE: u64 = 64;
/// `limits.go` `MaxNameFormatRegexSize`.
const MAX_NAME_FORMAT_REGEX_SIZE: f64 = 128.0;

/// `selectorLengthToRequirementCount`: the shortest repeatable selector
/// requirement that allocates a values slice is 2 characters.
const SELECTOR_LENGTH_TO_REQUIREMENT_COUNT: f64 = 0.5;
/// `costPerRequirement`: a struct and a values slice.
const COST_PER_REQUIREMENT: f64 = (LIST_CREATE_BASE_COST + STRUCT_CREATE_BASE_COST) as f64;

const QUANTITY: &str = "kubernetes.Quantity";
const SEMVER: &str = "kubernetes.Semver";
const FORMAT: &str = "kubernetes.NamedFormat";
const URL: &str = "kubernetes.URL";
/// `AuthorizerType`, `PathCheckType`, `GroupCheckType`, `ResourceCheckType`,
/// `DecisionType`.
const AUTHZ_O1_EQUALITY: [&str; 5] = [
    "kubernetes.authorization.Authorizer",
    "kubernetes.authorization.PathCheck",
    "kubernetes.authorization.GroupCheck",
    "kubernetes.authorization.ResourceCheck",
    "kubernetes.authorization.Decision",
];

/// `selectorCostEstimate`.
fn selector_cost_estimate(selector_length: SizeEstimate) -> CostEstimate {
    // A selector consists of a list of requirements held in a slice.
    let base = CostEstimate::fixed(LIST_CREATE_BASE_COST);
    let parse_cost = selector_length.multiply_by_cost_factor(STRING_TRAVERSAL_COST_FACTOR);
    let requirement_count =
        selector_length.multiply_by_cost_factor(SELECTOR_LENGTH_TO_REQUIREMENT_COUNT);
    let requirement_cost = requirement_count.multiply_by_cost_factor(COST_PER_REQUIREMENT);
    base.add(parse_cost).add(requirement_cost)
}

fn call(cost: CostEstimate) -> Option<CallEstimate> {
    Some(CallEstimate {
        cost,
        result_size: None,
    })
}

fn call_sized(cost: CostEstimate, size: SizeEstimate) -> Option<CallEstimate> {
    Some(CallEstimate {
        cost,
        result_size: Some(size),
    })
}

fn nominal() -> Option<CallEstimate> {
    call(CostEstimate::fixed(1))
}

fn size_as_cost(s: SizeEstimate) -> CostEstimate {
    CostEstimate {
        min: s.min,
        max: s.max,
    }
}

/// `library.CostEstimator`.
pub struct LibraryCostEstimator<'e> {
    /// `SizeEstimator`: the estimator size estimation is delegated to when
    /// the size is not well known (not a constant).
    pub size_estimator: Option<&'e dyn CostEstimator>,
}

impl LibraryCostEstimator<'_> {
    /// `sizeEstimate`.
    fn size_estimate(&self, t: &AstNode<'_>) -> SizeEstimate {
        if let Some(sz) = t.computed_size {
            return sz;
        }
        if let Some(sz) = self.estimate_size(t) {
            return sz;
        }
        SizeEstimate::UNKNOWN
    }

    /// `listElementNode`: the element node of a list (or the key node of a
    /// map: cel-go reads the first type parameter).
    fn list_element_node<'a>(&self, list: &AstNode<'a>) -> Option<AstNode<'a>> {
        let lt = list.ty.params().first().map(|t| (*t).clone())?;
        // With a path the node gets one, so that an OpenAPIv3 maxLength
        // validation can be looked up; without, just the type, so the worst
        // case size is looked up from the type.
        let path = if list.path.is_empty() {
            Vec::new()
        } else {
            let mut p = list.path.clone();
            p.push("@items".to_string());
            p
        };
        // `itemsNode`.
        Some(AstNode {
            path,
            ty: lt,
            expr: None,
            computed_size: None,
        })
    }
}

impl CostEstimator for LibraryCostEstimator<'_> {
    /// `EstimateSize`.
    fn estimate_size(&self, node: &AstNode<'_>) -> Option<SizeEstimate> {
        self.size_estimator.and_then(|e| e.estimate_size(node))
    }

    /// `EstimateCallCost`.
    fn estimate_call_cost(
        &self,
        function: &str,
        overload_id: &str,
        target: Option<&AstNode<'_>>,
        args: &[AstNode<'_>],
    ) -> Option<CallEstimate> {
        let trav = STRING_TRAVERSAL_COST_FACTOR;
        match function {
            // An authorization check has a fixed cost, set to allow for only
            // two authorization checks per expression.
            "check" => call(CostEstimate::fixed(350000)),
            // All authorization builder and accessor functions have a nominal
            // cost.
            "serviceAccount" | "path" | "group" | "resource" | "subresource" | "namespace"
            | "name" | "allowed" | "reason" | "error" | "errored" => nominal(),
            // Field and label selector parse is a string parse into a
            // structured set of requirements.
            "fieldSelector" | "labelSelector" if args.len() == 1 => {
                call(selector_cost_estimate(self.size_estimate(&args[0])))
            }
            "isSorted" | "sum" | "max" | "min" | "indexOf" | "lastIndexOf" => {
                let target = target?;
                // Charge 1 cost for comparing each element in the list.
                let mut el_cost = CostEstimate::fixed(1);
                if let Some(el) = self.list_element_node(target) {
                    // For strings or bytes, add the cost of traversing all of
                    // them as a way of estimating the comparison cost.
                    if matches!(el.ty, DType::Str | DType::Bytes) {
                        let sz = self.size_estimate(&el);
                        el_cost = el_cost.add(sz.multiply_by_cost_factor(trav));
                    }
                    call(self.size_estimate(target).multiply_by_cost(el_cost))
                } else {
                    // The target is a string, which `indexOf` and
                    // `lastIndexOf` support.
                    call(self.size_estimate(target).multiply_by_cost_factor(trav))
                }
            }
            "url" | "jsonpatch.escapeKey" if args.len() == 1 => {
                let sz = self.size_estimate(&args[0]);
                call_sized(sz.multiply_by_cost_factor(trav), sz)
            }
            "lowerAscii" | "upperAscii" | "substring" | "trim" => {
                let sz = self.size_estimate(target?);
                call_sized(sz.multiply_by_cost_factor(trav), sz)
            }
            "replace" if target.is_some() && args.len() >= 2 => {
                let sz = self.size_estimate(target?);
                let to_replace = self.size_estimate(&args[0]);
                let replace_with = self.size_estimate(&args[1]);
                let mut replace_count = SizeEstimate::fixed(0);
                let mut retained = SizeEstimate::fixed(0);

                // Find the longest replacement.
                if to_replace.min == 0 {
                    // An empty string being replaced surrounds all characters
                    // in the input with the replacement.
                    replace_count.max = if sz.max < u64::MAX {
                        sz.max + 1
                    } else {
                        sz.max
                    };
                    // The longest possible original string length is kept.
                    retained.max = sz.max;
                } else if replace_with.max <= to_replace.min {
                    // The replacement does not make the result longer.
                    replace_count.max = 0;
                    retained.max = sz.max;
                } else {
                    // The smallest possible substrings replaced with the
                    // largest possible replacement as many times as possible.
                    replace_count.max = (sz.max as f64 / to_replace.min as f64).ceil() as u64;
                }

                // Find the shortest replacement.
                if to_replace.max == 0 {
                    replace_count.min = if sz.min < u64::MAX {
                        sz.min + 1
                    } else {
                        sz.min
                    };
                    retained.min = sz.min;
                } else if to_replace.max <= replace_with.min {
                    // The replacement does not make the result shorter.
                    replace_count.min = 0;
                    retained.min = sz.min;
                } else {
                    // The largest possible substrings replaced with the
                    // smallest possible replacement as many times as
                    // possible.
                    replace_count.min = (sz.min as f64 / to_replace.max as f64).ceil() as u64;
                }
                let size = replace_count.multiply(replace_with).add(retained);

                // The cost is the traversal plus the construction of the
                // result.
                call_sized(sz.multiply_by_cost_factor(2.0 * trav), size)
            }
            "split" => {
                let sz = self.size_estimate(target?);
                // The worst case is a separator of "" with each char returned
                // as a list element.
                let mut max = sz.max;
                if args.len() > 1 {
                    if let Some(Expr::Literal(LiteralValue::Int(i))) = args[1].expr.map(|e| &e.expr)
                    {
                        max = *i.inner() as u64;
                    }
                }
                // The traversal plus the construction of the result.
                call_sized(
                    sz.multiply_by_cost_factor(2.0 * trav),
                    SizeEstimate { min: 0, max },
                )
            }
            "join" => {
                let target = target?;
                let mut sz = SizeEstimate::fixed(0);
                let list_size = self.size_estimate(target);
                if let Some(el) = self.list_element_node(target) {
                    let elem_size = self.size_estimate(&el);
                    sz = list_size.multiply(elem_size);
                }
                if let Some(sep) = args.first() {
                    let sep_size = self.size_estimate(sep);
                    let min_separators = list_size.min.saturating_sub(1);
                    let max_separators = list_size.max.saturating_sub(1);
                    sz = sz.add(sep_size.multiply(SizeEstimate {
                        min: min_separators,
                        max: max_separators,
                    }));
                }
                call_sized(sz.multiply_by_cost_factor(trav), sz)
            }
            "find" | "findAll" if target.is_some() && !args.is_empty() => {
                let sz = self.size_estimate(target?);
                // Add one to the string length so the product of string and
                // regex is not 0 for an empty string with an expensive regex.
                let str_cost = sz
                    .add(SizeEstimate { min: 1, max: 1 })
                    .multiply_by_cost_factor(trav);
                // Each expression in a regex is guessed to be at least 4
                // chars long.
                let regex_cost = self
                    .size_estimate(&args[0])
                    .multiply_by_cost_factor(REGEX_STRING_LENGTH_COST_FACTOR);
                // Worst case, every char is returned as a separate result.
                call_sized(
                    str_cost.multiply(regex_cost),
                    SizeEstimate {
                        min: 0,
                        max: sz.max,
                    },
                )
            }
            "cidr" | "isIP" | "isCIDR" if !args.is_empty() => {
                let sz = self.size_estimate(&args[0]);
                call(sz.multiply_by_cost_factor(trav))
            }
            "ip" => {
                if overload_id == CIDR_IP {
                    // The IP member of the CIDR object is a field access.
                    return nominal();
                }
                if !args.is_empty() {
                    let sz = self.size_estimate(&args[0]);
                    return call(sz.multiply_by_cost_factor(trav));
                }
                None
            }
            "ip.isCanonical" if !args.is_empty() => {
                let sz = self.size_estimate(&args[0]);
                // The string is parsed and compared with the original, so
                // the cost of parsing is doubled.
                call(sz.multiply_by_cost_factor(2.0 * trav))
            }
            // IP and CIDR accessors are nominal cost.
            "masked"
            | "prefixLength"
            | "family"
            | "isUnspecified"
            | "isLoopback"
            | "isLinkLocalMulticast"
            | "isLinkLocalUnicast"
            | "isGlobalUnicast" => nominal(),
            "containsIP" if !args.is_empty() => {
                // Comparing two byte lists of an ipv4 or ipv6 address: 4 or
                // 16 bytes.
                let sz = SizeEstimate { min: 4, max: 16 };
                let mut ip_comp_cost = sz.add(sz).multiply_by_cost_factor(trav);
                if overload_id == CIDR_CONTAINS_IP_STRING {
                    // A string is parsed into the right type first.
                    ip_comp_cost = ip_comp_cost.add(
                        size_as_cost(self.size_estimate(&args[0])).multiply_by_cost_factor(trav),
                    );
                }
                call(ip_comp_cost)
            }
            "containsCIDR" if !args.is_empty() => {
                let sz = SizeEstimate { min: 4, max: 16 };
                let mut ip_comp_cost = sz.add(sz).multiply_by_cost_factor(trav);
                // Whether a CIDR is within another first masks the base CIDR
                // and also compares the CIDR bits: the IP traversed again,
                // plus 1.
                ip_comp_cost = ip_comp_cost.add(sz.multiply_by_cost_factor(trav));
                ip_comp_cost = ip_comp_cost.add(CostEstimate::fixed(1));
                if overload_id == CIDR_CONTAINS_CIDR_STRING {
                    ip_comp_cost = ip_comp_cost.add(
                        size_as_cost(self.size_estimate(&args[0])).multiply_by_cost_factor(trav),
                    );
                }
                call(ip_comp_cost)
            }
            "quantity" | "isQuantity" | "semver" | "isSemver" if !args.is_empty() => {
                let sz = self.size_estimate(&args[0]);
                call(sz.multiply_by_cost_factor(trav))
            }
            "validate" if !args.is_empty() => {
                let sz = self.size_estimate(&args[0]);
                call(sz.multiply_by_cost_factor(trav).multiply_by_cost_factor(
                    MAX_NAME_FORMAT_REGEX_SIZE * REGEX_STRING_LENGTH_COST_FACTOR,
                ))
            }
            // A dictionary lookup.
            "format.named" => nominal(),
            "sign" | "asInteger" | "isInteger" | "asApproximateFloat" | "isGreaterThan"
            | "isLessThan" | "compareTo" | "add" | "sub" | "major" | "minor" | "patch" => nominal(),
            // URL accessors.
            "getScheme" | "getHostname" | "getHost" | "getPort" | "getEscapedPath" | "getQuery" => {
                nominal()
            }
            "_==_" if args.len() == 2 => {
                let (lhs, rhs) = (&args[0], &args[1]);
                if lhs.ty != rhs.ty {
                    return None;
                }
                match &lhs.ty {
                    DType::Opaque(n, _) if n == "net.IP" || n == "net.CIDR" => nominal(),
                    DType::Struct(n) if n == QUANTITY || n == SEMVER => nominal(),
                    DType::Struct(n) if AUTHZ_O1_EQUALITY.contains(&n.as_str()) => nominal(),
                    DType::Struct(n) if n == FORMAT => call(
                        CostEstimate {
                            min: 1,
                            max: MAX_FORMAT_SIZE,
                        }
                        .multiply_by_cost_factor(trav),
                    ),
                    DType::Struct(n) if n == URL => {
                        // Upstream reads `rhs.ComputedSize()` for both sides.
                        let mut size = SizeEstimate { min: 1, max: 1 };
                        if let (Some(r), Some(l)) = (rhs.computed_size, rhs.computed_size) {
                            size = r.union(l);
                        }
                        call(
                            CostEstimate {
                                min: 1,
                                max: size.max,
                            }
                            .multiply_by_cost_factor(trav),
                        )
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

/// `cel.Env.EstimateCost(ast, &library.CostEstimator{SizeEstimator: ...})` in a
/// Kubernetes CEL environment: the library estimator, the `ext` overload
/// estimators of [`crate::ext_cost`] and `PresenceTestHasCost(false)`
/// (`environment/base.go:77-79`).
pub fn kubernetes_cost(
    checked: &CheckedAst,
    root: &cel::common::ast::IdedExpr,
    size_estimator: Option<&dyn CostEstimator>,
) -> CostEstimate {
    static EXT: std::sync::OnceLock<OverloadEstimators> = std::sync::OnceLock::new();
    let ext = EXT.get_or_init(crate::ext_cost::ext_overload_estimators);
    let estimator = LibraryCostEstimator { size_estimator };
    cost(
        checked,
        root,
        &estimator,
        CostOptions {
            presence_test_free: true,
            overload_estimators: Some(ext),
        },
    )
}
