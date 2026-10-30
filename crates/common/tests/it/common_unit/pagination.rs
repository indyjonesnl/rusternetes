//! Unit tests moved from `src/pagination.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use base64::{engine::general_purpose, Engine as _};
    pub use serde::{Deserialize, Serialize};
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::pagination::*;

    #[test]
    fn test_no_pagination() {
        let items = vec![1, 2, 3, 4, 5];
        let params = PaginationParams {
            limit: None,
            continue_token: None,
        };

        let result = paginate(items, params, "v1").unwrap();

        assert_eq!(result.items.len(), 5);
        assert_eq!(result.continue_token, None);
        assert_eq!(result.remaining_item_count, None);
    }

    #[test]
    fn test_first_page() {
        let items = vec![1, 2, 3, 4, 5];
        let params = PaginationParams {
            limit: Some(2),
            continue_token: None,
        };

        let result = paginate(items, params, "v1").unwrap();

        assert_eq!(result.items, vec![1, 2]);
        assert!(result.continue_token.is_some());
        assert_eq!(result.remaining_item_count, Some(3));
    }

    #[test]
    fn test_second_page() {
        let items = vec![1, 2, 3, 4, 5];

        // Get first page
        let params = PaginationParams {
            limit: Some(2),
            continue_token: None,
        };
        let first_result = paginate(items.clone(), params, "v1").unwrap();

        // Get second page using continue token
        let params = PaginationParams {
            limit: Some(2),
            continue_token: first_result.continue_token,
        };
        let result = paginate(items, params, "v1").unwrap();

        assert_eq!(result.items, vec![3, 4]);
        assert!(result.continue_token.is_some());
        assert_eq!(result.remaining_item_count, Some(1));
    }

    #[test]
    fn inconsistent_continue_token_resumes_from_offset_not_zero() {
        // Reproduces the [sig-api-machinery] API chunking conformance failure:
        // after a continue token's resourceVersion is compacted away, the server
        // returns 410 with a fresh "inconsistent" continue token; re-listing with
        // it must resume from the SAME offset (not restart at 0).
        let items: Vec<i32> = (0..400).collect();

        // Page 1: limit 40.
        let p1 = paginate(
            items.clone(),
            PaginationParams {
                limit: Some(40),
                continue_token: None,
            },
            "100",
        )
        .unwrap();
        assert_eq!(p1.items.len(), 40);
        assert_eq!(p1.remaining_item_count, Some(360));
        let first = p1.continue_token.unwrap();
        assert_eq!(ContinuationToken::decode(&first).unwrap().start, 40);

        // Forge a stale page-1 token (ancient created_at) to drive the 410 path.
        let mut cont = ContinuationToken::decode(&first).unwrap();
        cont.created_at = 1;
        let stale = cont.encode().unwrap();

        let err = paginate(
            items.clone(),
            PaginationParams {
                limit: Some(40),
                continue_token: Some(stale),
            },
            "200",
        )
        .unwrap_err();
        let fresh = err
            .fresh_continue_token
            .expect("stale token must yield a fresh continue token");
        assert_eq!(
            ContinuationToken::decode(&fresh).unwrap().start,
            40,
            "fresh (inconsistent) token must resume at offset 40, not 0"
        );

        // Re-list with the fresh token: exactly `limit` items from offset 40,
        // remaining = 400 - 80 = 320.
        let p2 = paginate(
            items,
            PaginationParams {
                limit: Some(40),
                continue_token: Some(fresh),
            },
            "200",
        )
        .unwrap();
        assert_eq!(p2.items.len(), 40);
        assert_eq!(p2.items[0], 40, "second page must start at item 40");
        assert_eq!(p2.remaining_item_count, Some(320));
    }

    #[test]
    fn inconsistent_continue_token_pins_original_snapshot_total() {
        // The real API-chunking failure mode: the collection appears larger at
        // re-list time than when the chain started. The inconsistent continue
        // token MUST keep computing remaining against the ORIGINAL snapshot total
        // (carried in the token), so remaining + returned + alreadySeen stays
        // equal to the original total — not the inflated current count.
        let initial: Vec<i32> = (0..400).collect();

        // Page 1 over the original 400-item snapshot.
        let p1 = paginate(
            initial,
            PaginationParams {
                limit: Some(40),
                continue_token: None,
            },
            "100",
        )
        .unwrap();
        let mut cont = ContinuationToken::decode(&p1.continue_token.unwrap()).unwrap();
        assert_eq!(cont.start, 40);
        assert_eq!(cont.total_at_creation, 400);
        cont.created_at = 1; // force stale
        let stale = cont.encode().unwrap();

        // The collection has since grown to 440 items.
        let grown: Vec<i32> = (0..440).collect();
        let err = paginate(
            grown.clone(),
            PaginationParams {
                limit: Some(40),
                continue_token: Some(stale),
            },
            "200",
        )
        .unwrap_err();
        let fresh = err.fresh_continue_token.unwrap();
        // The fresh token must carry the ORIGINAL total (400), not the grown 440.
        assert_eq!(
            ContinuationToken::decode(&fresh).unwrap().total_at_creation,
            400,
            "inconsistent token must pin the original snapshot total"
        );

        let p2 = paginate(
            grown,
            PaginationParams {
                limit: Some(40),
                continue_token: Some(fresh),
            },
            "200",
        )
        .unwrap();
        assert_eq!(p2.items.len(), 40);
        // remaining(320) + returned(40) + alreadySeen(40) == 400, not 440.
        assert_eq!(p2.remaining_item_count, Some(320));
    }

    #[test]
    fn test_last_page() {
        let items = vec![1, 2, 3, 4, 5];

        // Simulate getting to the last page
        let token = ContinuationToken {
            start: 4,
            resource_version: "v1".to_string(),
            filters: HashMap::new(),
            nonce: 0,
            total_at_creation: 0,
            created_at: 0,
        }
        .encode()
        .unwrap();

        let params = PaginationParams {
            limit: Some(2),
            continue_token: Some(token),
        };
        let result = paginate(items, params, "v1").unwrap();

        assert_eq!(result.items, vec![5]);
        assert_eq!(result.continue_token, None);
        // Kubernetes convention: remainingItemCount is nil when continue is empty
        assert_eq!(result.remaining_item_count, None);
    }

    #[test]
    fn test_limit_zero_returns_all_items_upstream_contract() {
        // Upstream: `limit=0` is "no chunking", not "empty page".
        let items = vec![1, 2, 3, 4, 5];
        let params = PaginationParams {
            limit: Some(0),
            continue_token: None,
        };

        let result = paginate(items, params, "v1").unwrap();

        assert_eq!(result.items, vec![1, 2, 3, 4, 5]);
        assert_eq!(result.continue_token, None);
        assert_eq!(result.remaining_item_count, None);
    }

    #[test]
    fn test_snapshot_pinning_excludes_mid_chain_inserts() {
        // Page 1 over the original 5-item list.
        let items_v1 = vec![1, 2, 3, 4, 5];
        let result1 = paginate(
            items_v1,
            PaginationParams {
                limit: Some(2),
                continue_token: None,
            },
            "v1",
        )
        .unwrap();
        assert_eq!(result1.items, vec![1, 2]);
        let token = result1.continue_token.expect("first page has a token");

        // Between pages, two new items appear. Snapshot pinning should
        // truncate items_v2 back to the original 5 before paginating.
        let items_v2 = vec![1, 2, 3, 4, 5, 6, 7];
        let result2 = paginate(
            items_v2,
            PaginationParams {
                limit: Some(2),
                continue_token: Some(token),
            },
            "v1",
        )
        .unwrap();
        assert_eq!(result2.items, vec![3, 4]);
        assert!(result2.continue_token.is_some());

        // Drain remaining pages — must not surface 6 or 7.
        let mut all = vec![1, 2, 3, 4];
        let mut next = result2.continue_token;
        while let Some(t) = next {
            let r = paginate(
                vec![1, 2, 3, 4, 5, 6, 7],
                PaginationParams {
                    limit: Some(2),
                    continue_token: Some(t),
                },
                "v1",
            )
            .unwrap();
            all.extend(r.items);
            next = r.continue_token;
        }
        assert_eq!(all, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_resource_version_mismatch_is_tolerated() {
        let items = vec![1, 2, 3, 4, 5];

        let token = ContinuationToken {
            start: 2,
            resource_version: "v1".to_string(),
            filters: HashMap::new(),
            nonce: 0,
            total_at_creation: 0,
            created_at: 0,
        }
        .encode()
        .unwrap();

        let params = PaginationParams {
            limit: Some(2),
            continue_token: Some(token),
        };

        // Resource version change should be tolerated (Kubernetes compacts versions)
        let result = paginate(items, params, "v2");

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.items, vec![3, 4]);
    }
}
