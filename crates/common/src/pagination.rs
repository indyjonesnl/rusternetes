//! API pagination support for Kubernetes-compliant list operations
//!
//! Implements limit/continue pagination as per Kubernetes API conventions:
//! - limit: Maximum number of items to return
//! - continue: Continuation token from a previous response
//! - remainingItemCount: Number of items remaining after this page

use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Pagination parameters from query string
#[derive(Debug, Clone, Deserialize)]
pub struct PaginationParams {
    /// Maximum number of items to return per page
    #[serde(default)]
    pub limit: Option<i64>,

    /// Continue token from previous response
    #[serde(default, rename = "continue")]
    pub continue_token: Option<String>,
}

/// Internal continuation token structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuationToken {
    /// Starting index for next page
    pub start: usize,
    /// Resource version for consistency
    pub resource_version: String,
    /// Optional filter parameters to ensure consistency
    pub filters: HashMap<String, String>,
    /// Unique nonce to ensure tokens differ across requests
    #[serde(default)]
    pub nonce: u64,
    /// Total item count at time of token creation (for staleness detection)
    #[serde(default)]
    pub total_at_creation: usize,
    /// Unix timestamp when the token was created (for expiry detection)
    #[serde(default)]
    pub created_at: u64,
}

impl ContinuationToken {
    /// Encode the token to a base64 string
    pub fn encode(&self) -> Result<String, serde_json::Error> {
        let json = serde_json::to_string(self)?;
        Ok(general_purpose::STANDARD.encode(json.as_bytes()))
    }

    /// Decode a token from base64 string
    pub fn decode(token: &str) -> Result<Self, String> {
        let bytes = general_purpose::STANDARD
            .decode(token)
            .map_err(|e| format!("Invalid continue token: {}", e))?;

        let json = String::from_utf8(bytes)
            .map_err(|e| format!("Invalid continue token encoding: {}", e))?;

        serde_json::from_str(&json).map_err(|e| format!("Invalid continue token format: {}", e))
    }
}

/// Result of paginating a list of items
#[derive(Debug)]
pub struct PaginatedResult<T> {
    /// Items for this page
    pub items: Vec<T>,
    /// Continue token for next page (None if this is the last page)
    pub continue_token: Option<String>,
    /// Number of remaining items after this page
    pub remaining_item_count: Option<i64>,
    /// The resource version to use in the response (consistent across pages)
    pub resource_version: String,
}

/// Error returned when pagination fails
#[derive(Debug)]
pub struct PaginationError {
    pub message: String,
    /// A fresh continue token the client can use to restart the list
    pub fresh_continue_token: Option<String>,
}

/// Paginate a list of items according to Kubernetes API conventions.
///
/// Upstream parity:
/// - `limit=0` is treated as "no chunking — return everything" (NOT
///   "empty page"), matching the apiserver contract documented in
///   `staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/types.go::ListOptions.Limit`.
/// - A continue chain is conceptually a snapshot at the original list's
///   resourceVersion. We approximate this by carrying
///   `total_at_creation` forward in every token and truncating later
///   pages to that count, so items appended after the chain started
///   never leak into a subsequent page.
pub fn paginate<T>(
    mut items: Vec<T>,
    params: PaginationParams,
    resource_version: &str,
) -> Result<PaginatedResult<T>, PaginationError> {
    // Parse continue token if provided
    // Use the token's resource_version for consistency across pages
    let (start, effective_rv, snapshot_total) = if let Some(token) = &params.continue_token {
        let cont = ContinuationToken::decode(token).map_err(|e| PaginationError {
            message: e,
            fresh_continue_token: None,
        })?;

        let is_stale =
            // Check if token expired (etcd compaction, 5 min).
            // K8s uses resourceVersion consistency, not item count comparison.
            // Item count can change between pages (controllers creating resources)
            // without invalidating the pagination — items are sorted by key.
            cont.created_at > 0 && {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                now.saturating_sub(cont.created_at) > 300
            };

        if is_stale {
            // Generate a fresh continue token starting from the same offset
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            let fresh = ContinuationToken {
                start: cont.start.min(items.len()),
                // Carry the ORIGINAL snapshot total forward (mirroring the normal
                // continue path's `chain_total`), not the current items.len().
                // The inconsistent continue resumes the same logical chain, so its
                // remaining-item math must be computed against the snapshot the
                // chain started with; re-snapshotting here would inflate the total
                // (and `remainingItemCount`) if the collection changed size between
                // pages — breaking the API-chunking conformance invariant
                // remaining + returned + alreadySeen == originalTotal.
                total_at_creation: if cont.total_at_creation > 0 {
                    cont.total_at_creation
                } else {
                    items.len()
                },
                resource_version: resource_version.to_string(),
                filters: HashMap::new(),
                nonce,
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
            };
            let fresh_token = fresh.encode().ok();
            return Err(PaginationError {
                message: "410 Gone: the resource version in the continue token is too old; the client must restart the list without a continue token".to_string(),
                fresh_continue_token: fresh_token,
            });
        }

        // Carry the original snapshot's total forward so later pages do
        // not see items inserted mid-chain.
        let snap = if cont.total_at_creation > 0 {
            Some(cont.total_at_creation)
        } else {
            None
        };
        (cont.start, cont.resource_version.clone(), snap)
    } else {
        (0, resource_version.to_string(), None)
    };

    // Snapshot pinning: if a snapshot total was carried in, cap the items
    // vector to that count so appended items are invisible.
    if let Some(snap) = snapshot_total {
        if items.len() > snap {
            items.truncate(snap);
        }
    }

    // Resolve the page size. `None` and `Some(0)` both mean "no
    // chunking — return everything from start to end", matching the
    // upstream apiserver contract.
    let limit = match params.limit {
        Some(l) if l > 0 => l as usize,
        _ => {
            // No limit, return all items from start
            let result_items = if start >= items.len() {
                Vec::new()
            } else {
                items.drain(start..).collect()
            };
            return Ok(PaginatedResult {
                items: result_items,
                continue_token: None,
                remaining_item_count: None,
                resource_version: effective_rv,
            });
        }
    };

    let total = items.len();

    // Check if start is beyond the list
    if start >= total {
        return Ok(PaginatedResult {
            items: vec![],
            continue_token: None,
            remaining_item_count: Some(0),
            resource_version: effective_rv,
        });
    }

    // Calculate end index
    let end = (start + limit).min(total);

    // Extract the page of items
    let page_items: Vec<T> = items.drain(start..end).collect();

    // Choose the total to carry forward in the next token: the snapshot
    // total if pagination is mid-chain, otherwise the current total
    // (which becomes the snapshot for the rest of the chain).
    let chain_total = snapshot_total.unwrap_or(total);

    // Check if there are more items
    let (continue_token, remaining_count) = if end < total {
        // Include a nonce so tokens are always unique, even for the same offset
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let next_token = ContinuationToken {
            start: end,
            total_at_creation: chain_total,
            resource_version: effective_rv.clone(),
            filters: HashMap::new(),
            nonce,
            created_at,
        };

        let token = next_token.encode().map_err(|e| PaginationError {
            message: format!("Failed to encode continue token: {}", e),
            fresh_continue_token: None,
        })?;

        (Some(token), Some((total - end) as i64))
    } else {
        // Last page: no continue token, no remainingItemCount
        // Kubernetes expects remainingItemCount to be nil when continue is empty
        (None, None)
    };

    Ok(PaginatedResult {
        items: page_items,
        continue_token,
        remaining_item_count: remaining_count,
        resource_version: effective_rv,
    })
}
