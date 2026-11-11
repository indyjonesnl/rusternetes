//! The size estimator of a CRD rule's static cost estimate:
//! `apiextensions-apiserver/pkg/apiserver/schema/cel/compilation.go`
//! `sizeEstimator` and `maxCardinality` (release-1.35), over the
//! [`DeclType`] of [`crate::cel_schema_types`].

use k3rs_cel_checker::cost::{AstNode, CallEstimate, CostEstimator, SizeEstimate};
use k3rs_cel_checker::decl::{limits, DeclType};

/// `sizeEstimator`: `EstimateSize` walks the node's field path down the
/// rule's root type and reads the `MaxElements` of the type it ends at.
pub struct SchemaSizeEstimator {
    root: DeclType,
}

impl SchemaSizeEstimator {
    /// `newCostEstimator(root)`'s `sizeEstimator{root: root}`.
    pub fn new(root: DeclType) -> Self {
        Self { root }
    }
}

impl CostEstimator for SchemaSizeEstimator {
    fn estimate_size(&self, element: &AstNode<'_>) -> Option<SizeEstimate> {
        // Path() can return an empty list: no size estimate can be provided.
        if element.path.is_empty() {
            return None;
        }
        let mut current = &self.root;
        // Cut off "self" from the path, since it always starts there.
        let rest = &element.path[1..];
        for (i, name) in rest.iter().enumerate() {
            current = match name.as_str() {
                "@items" | "@values" => current.elem_type()?,
                "@keys" => {
                    // The key type of a map is `string`: a simple type with
                    // no size of its own and nothing below it.
                    let key = current.key_type()?;
                    return (i + 1 == rest.len()).then_some(SizeEstimate {
                        min: 0,
                        max: key.max_elements as u64,
                    });
                }
                field => &current.fields()?.get(field)?.ty,
            };
        }
        Some(SizeEstimate {
            min: 0,
            max: current.max_elements as u64,
        })
    }

    fn estimate_call_cost(
        &self,
        _function: &str,
        _overload_id: &str,
        _target: Option<&AstNode<'_>>,
        _args: &[AstNode<'_>],
    ) -> Option<CallEstimate> {
        None
    }
}

/// `maxCardinality`: the maximum number of times data conforming to the
/// minimum size given could possibly exist in an object serialized to JSON.
/// For a schema contained under map or array schemas of unbounded size this
/// is the worst case number of times data matching the schema could be
/// repeated. It only assumes a single comma between data elements, so under
/// only maps it estimates a higher cardinality than is possible.
/// `DeclType.MinSerializedSize` is meant to be passed to it.
pub fn max_cardinality(min_size: i64) -> u64 {
    // Assume at least one comma between elements.
    let sz = min_size + 1;
    (limits::MAX_REQUEST_SIZE_BYTES / sz) as u64
}
