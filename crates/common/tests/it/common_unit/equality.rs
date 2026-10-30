//! Unit tests moved from `src/equality.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use serde::Serialize;
}

mod tests {
    use rusternetes_common::equality::semantic_equal;
    use std::collections::HashMap;

    #[test]
    fn nil_and_empty_are_equal() {
        let none: Option<HashMap<String, String>> = None;
        assert!(semantic_equal(&none, &Some(HashMap::new())));
        assert!(!semantic_equal(
            &none,
            &Some(HashMap::from([("a".to_string(), "1".to_string())]))
        ));
        assert!(semantic_equal(&Vec::<i32>::new(), &Vec::new()));
    }
}
