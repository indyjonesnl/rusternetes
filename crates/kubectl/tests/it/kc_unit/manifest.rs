//! Unit tests moved from `src/manifest.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use serde::Deserialize;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::manifest::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_temp(contents: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn test_read_documents_single() {
        let f = write_temp("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a\n");
        let docs = read_documents(f.path().to_str().unwrap()).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(
            docs[0].get("kind").and_then(|k| k.as_str()),
            Some("ConfigMap")
        );
    }

    #[test]
    fn test_read_documents_two_docs() {
        let f = write_temp(
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a\n---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: b\n",
        );
        let docs = read_documents(f.path().to_str().unwrap()).unwrap();
        assert_eq!(docs.len(), 2);
        assert_eq!(
            docs[0]
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str()),
            Some("a")
        );
        assert_eq!(
            docs[1]
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str()),
            Some("b")
        );
    }

    #[test]
    fn test_read_documents_trailing_separator_skipped() {
        // A trailing `---` produces an extra empty (null) document that must be
        // skipped, leaving only the real document.
        let f = write_temp("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a\n---\n");
        let docs = read_documents(f.path().to_str().unwrap()).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(
            docs[0]
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str()),
            Some("a")
        );
    }

    #[test]
    fn test_read_documents_empty_middle_doc_skipped() {
        // An empty document in the middle (between two `---`) is also skipped.
        let f = write_temp(
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a\n---\n---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: b\n",
        );
        let docs = read_documents(f.path().to_str().unwrap()).unwrap();
        assert_eq!(docs.len(), 2);
    }

    #[test]
    fn test_read_documents_missing_file_errors() {
        let result = read_documents("/nonexistent/path/xyz.yaml");
        assert!(result.is_err());
    }
}
