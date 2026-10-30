//! Unit tests moved from `src/handlers/mod.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_discovery as discovery;
}

mod collection_rv_tests {
    use rusternetes_api_server::handlers::{collection_resource_version, list_resource_version};
    use serde_json::json;

    /// A LIST's `metadata.resourceVersion` must never be below an item the
    /// same LIST returned.
    ///
    /// The store revision and the items are read separately here, so a
    /// `current_revision()` that answers even one revision stale used to win
    /// outright. A client watching from that RV
    /// (`List` -> `Watch(ResourceVersion: list.ResourceVersion)`) then gets the
    /// newest object's CREATE replayed and sees `ADDED` where it expects
    /// `MODIFIED` — verified against a rhino-backed api-server, where a watch
    /// one revision below an object's creation replays it as `ADDED` (#1824).
    #[test]
    fn collection_rv_is_never_below_a_returned_item() {
        // Stale store revision, fresher items: the items win.
        assert_eq!(collection_resource_version(Some(17), "18"), "18");
        assert_eq!(collection_resource_version(Some(1), "4143"), "4143");
        // Store ahead of the items (writes to other collections): store wins.
        assert_eq!(collection_resource_version(Some(4200), "4143"), "4200");
        // Agreement.
        assert_eq!(collection_resource_version(Some(18), "18"), "18");
        // No store revision available: fall back to the items untouched.
        assert_eq!(collection_resource_version(None, "18"), "18");
        // Empty collection keeps `list_resource_version`'s non-empty guarantee.
        assert_eq!(collection_resource_version(None, "1"), "1");
        assert_eq!(collection_resource_version(Some(42), "1"), "42");
    }

    /// The result stays a non-empty decimal string in every branch — a
    /// reflector cannot start a watch from `""` or `"0"`.
    #[test]
    fn collection_rv_is_always_a_positive_decimal() {
        for (cur, items) in [
            (Some(5_i64), "3"),
            (Some(3), "5"),
            (None, "7"),
            (None, "1"),
            (Some(1), "1"),
        ] {
            let rv = collection_resource_version(cur, items);
            assert!(
                rv.chars().all(|c| c.is_ascii_digit()) && rv != "0" && !rv.is_empty(),
                "bad collection rv {rv:?} for ({cur:?}, {items})"
            );
        }
    }

    /// The item-side input this composes with: highest RV wins, and an empty
    /// collection still yields a usable value.
    #[test]
    fn items_max_rv_feeds_the_collection_rv() {
        let items = vec![
            json!({"metadata": {"resourceVersion": "16"}}),
            json!({"metadata": {"resourceVersion": "18"}}),
            json!({"metadata": {"resourceVersion": "17"}}),
        ];
        assert_eq!(list_resource_version(&items), "18");
        assert_eq!(
            collection_resource_version(Some(17), &list_resource_version(&items)),
            "18",
            "the third FlowSchema's revision must not be left outside the list RV"
        );
        let empty: Vec<serde_json::Value> = vec![];
        assert_eq!(list_resource_version(&empty), "1");
    }
}
