//! Shared helpers for reading Kubernetes manifests from a file (or stdin) and
//! splitting them into individual YAML documents.
//!
//! `apply`, `create -f`, `diff -f`, and `delete -f` all need the same
//! "read the source, then parse the multi-document YAML into a list of
//! documents" step. This module centralises that so the four commands cannot
//! drift apart.

use anyhow::{Context, Result};
use serde::Deserialize;

/// Read a manifest source (a file path, or `-` for stdin) and parse it into its
/// YAML documents.
///
/// Multi-document YAML (documents separated by `---`) is supported. Empty
/// documents — including a trailing `---` with nothing after it — are skipped,
/// matching the behaviour each command previously implemented inline.
pub fn read_documents(source: &str) -> Result<Vec<serde_yaml::Value>> {
    let contents = if source == "-" {
        let mut buf = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut buf)
            .context("failed to read manifest from stdin")?;
        buf
    } else {
        std::fs::read_to_string(source)
            .with_context(|| format!("failed to read manifest file {source}"))?
    };

    let mut docs = Vec::new();
    for de in serde_yaml::Deserializer::from_str(&contents) {
        let value = serde_yaml::Value::deserialize(de)?;
        // Skip empty documents / trailing `---`.
        if !value.is_null() {
            docs.push(value);
        }
    }
    Ok(docs)
}
