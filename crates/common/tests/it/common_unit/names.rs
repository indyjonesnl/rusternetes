//! Unit tests moved from `src/names.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {}

mod tests {
    use super::__parent::*;
    use rusternetes_common::names::*;

    /// Upstream `TestSimpleNameGenerator` (generate_test.go): the base is kept
    /// and five characters are appended.
    #[test]
    fn appends_five_random_characters_to_the_base() {
        let name = simple_name_generator("foo");
        assert!(name.starts_with("foo"), "{name}");
        assert_eq!(name.len(), 3 + RANDOM_LENGTH);
        assert!(name[3..].bytes().all(|b| ALPHANUMS.contains(&b)), "{name}");
    }

    /// Upstream `TestSimpleNameGeneratorLength`: however long the base, the
    /// result never exceeds 63 characters.
    #[test]
    fn a_long_base_is_truncated_to_fit_a_name() {
        let base = "a".repeat(100);
        let name = simple_name_generator(&base);
        assert_eq!(name.len(), MAX_NAME_LENGTH);
        assert!(name.starts_with(&"a".repeat(MAX_GENERATED_NAME_LENGTH)));
    }
}
