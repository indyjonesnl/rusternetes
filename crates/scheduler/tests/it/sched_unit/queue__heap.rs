//! Unit tests moved from `src/queue/heap.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::queue::heap::*;

    fn int_heap() -> Heap<(String, i64)> {
        Heap::new(
            Box::new(|(k, _): &(String, i64)| k.clone()),
            Box::new(|a: &(String, i64), b: &(String, i64)| a.1 < b.1),
        )
    }

    /// `TestHeapBasic` / `TestHeap_AddOrUpdate` / `TestHeap_Delete`
    /// (`heap_test.go`), condensed.
    #[test]
    fn basic_ordering_update_and_delete() {
        let mut h = int_heap();
        for (k, v) in [("a", 10), ("b", 1), ("c", 11), ("d", 30), ("e", 5)] {
            h.add_or_update((k.to_string(), v));
        }
        assert_eq!(h.len(), 5);
        assert_eq!(h.peek().unwrap().1, 1);
        // update in place re-sorts
        h.add_or_update(("d".to_string(), 0));
        assert_eq!(h.peek().unwrap().0, "d");
        // delete by key
        h.delete(&("d".to_string(), 0)).unwrap();
        assert!(h.delete(&("zz".to_string(), 0)).is_err());
        let mut out = Vec::new();
        while let Ok(x) = h.pop() {
            out.push(x.1);
        }
        assert_eq!(out, vec![1, 5, 10, 11]);
        assert!(h.pop().is_err());
        assert!(h.peek().is_none());
        assert!(h.is_empty());
    }

    #[test]
    fn get_and_has() {
        let mut h = int_heap();
        h.add_or_update(("x".to_string(), 3));
        assert!(h.has(&("x".to_string(), 0)));
        assert_eq!(h.get_by_key("x").unwrap().1, 3);
        assert!(h.get_by_key("y").is_none());
        assert_eq!(h.list().len(), 1);
    }
}
