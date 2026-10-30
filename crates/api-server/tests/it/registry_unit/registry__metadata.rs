//! Unit tests moved from `src/registry/metadata.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {}

mod tests {
    use rusternetes_api_server::registry::metadata::HasMetadata;

    fn assert_has_metadata<T: HasMetadata>() {}

    /// Every type the watch paths and the Store serve goes through this one
    /// trait; the watch module used to carry its own copy with its own list.
    #[test]
    fn watch_and_store_types_share_one_trait() {
        use rusternetes_common::resources::*;
        assert_has_metadata::<Pod>();
        assert_has_metadata::<Event>();
        assert_has_metadata::<EventV1>();
        assert_has_metadata::<Endpoints>();
        assert_has_metadata::<EndpointSlice>();
        assert_has_metadata::<Namespace>();
        assert_has_metadata::<CustomResource>();
    }

    /// The accessors read and write the object's own metadata (the accessor
    /// test dropped with `finalizers_test.rs`, #2156).
    #[test]
    fn accessors_read_and_write_the_objects_metadata() {
        let mut pod = rusternetes_common::resources::Pod::new("a", Default::default());
        pod.metadata.name = "a".to_string();
        assert_eq!(HasMetadata::metadata(&pod).name, "a");
        HasMetadata::metadata_mut(&mut pod).finalizers = Some(vec!["f".to_string()]);
        assert_eq!(pod.metadata.finalizers, Some(vec!["f".to_string()]));
    }
}
