//! `structuralpruning.PruneWithOptions` with `UnknownFieldPathOptions`.
//!
//! Port of
//! `staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning/algorithm.go`
//! (`PruneWithOptions` :28-41, `prune` :52-111, `skipPrune` :113-148) and the
//! path bookkeeping of `schema/options.go:25-66` (`RecordUnknownField`,
//! `AppendKey`, `AppendIndex`), release-1.35.
//!
//! Go mutates `opts.ParentPath` (a `[]string` joined with `""`); the same
//! string is kept here and truncated back to its saved length.

use serde_json::Value;

use super::Structural;

fn is_meta_field(k: &str) -> bool {
    // algorithm.go:50-54 `metaFields`.
    matches!(k, "apiVersion" | "kind" | "metadata")
}

/// `schema.UnknownFieldPathOptions` (options.go:25-34).
struct Opts {
    track: bool,
    parent_path: String,
    unknown: Vec<String>,
}

impl Opts {
    /// options.go:36-46.
    fn record_unknown_field(&mut self, field: &str) {
        if !self.track {
            return;
        }
        let l = self.parent_path.len();
        self.append_key(field);
        self.unknown.push(self.parent_path.clone());
        self.parent_path.truncate(l);
    }

    /// options.go:48-58.
    fn append_key(&mut self, key: &str) {
        if !self.track {
            return;
        }
        if !self.parent_path.is_empty() {
            self.parent_path.push('.');
        }
        self.parent_path.push_str(key);
    }

    /// options.go:60-66.
    fn append_index(&mut self, index: usize) {
        if !self.track {
            return;
        }
        self.parent_path.push_str(&format!("[{index}]"));
    }
}

/// `PruneWithOptions` (algorithm.go:28-41): removes the fields of `obj` that
/// `s` does not specify, skipping TypeMeta and ObjectMeta when
/// `XEmbeddedResource` is set or for the root when `is_resource_root`.
/// Returns the sorted paths it pruned when `track_unknown_field_paths`.
pub fn prune_with_options(
    obj: &mut Value,
    s: Option<&Structural>,
    is_resource_root: bool,
    track_unknown_field_paths: bool,
) -> Vec<String> {
    let mut opts = Opts {
        track: track_unknown_field_paths,
        parent_path: String::new(),
        unknown: Vec::new(),
    };
    if is_resource_root {
        let mut clone = s.cloned().unwrap_or_default();
        clone.extensions.x_embedded_resource = true;
        prune(obj, Some(&clone), &mut opts);
    } else {
        prune(obj, s, &mut opts);
    }
    opts.unknown.sort();
    opts.unknown
}

fn prune(x: &mut Value, s: Option<&Structural>, opts: &mut Opts) {
    if let Some(st) = s {
        if st.extensions.x_preserve_unknown_fields {
            skip_prune(x, s, opts);
            return;
        }
    }

    let orig = opts.parent_path.len();
    match x {
        Value::Object(map) => {
            let Some(s) = s else {
                let keys: Vec<String> = map.keys().cloned().collect();
                for k in keys {
                    opts.record_unknown_field(&k);
                    map.remove(&k);
                }
                return;
            };
            let keys: Vec<String> = map.keys().cloned().collect();
            for k in keys {
                if s.extensions.x_embedded_resource && is_meta_field(&k) {
                    continue;
                }
                if let Some(prop) = s.properties.get(&k) {
                    opts.append_key(&k);
                    prune(map.get_mut(&k).unwrap(), Some(prop), opts);
                    opts.parent_path.truncate(orig);
                } else if let Some(ap) = s.additional_properties.as_deref() {
                    opts.append_key(&k);
                    prune(map.get_mut(&k).unwrap(), ap.structural.as_ref(), opts);
                    opts.parent_path.truncate(orig);
                } else {
                    if !is_meta_field(&k) || !opts.parent_path.is_empty() {
                        opts.record_unknown_field(&k);
                    }
                    map.remove(&k);
                }
            }
        }
        Value::Array(items) => {
            let item_schema = s.and_then(|s| s.items.as_deref());
            for (i, v) in items.iter_mut().enumerate() {
                opts.append_index(i);
                prune(v, item_schema, opts);
                opts.parent_path.truncate(orig);
            }
        }
        _ => {}
    }
    opts.parent_path.truncate(orig);
}

fn skip_prune(x: &mut Value, s: Option<&Structural>, opts: &mut Opts) {
    let Some(s) = s else {
        return;
    };
    let orig = opts.parent_path.len();
    match x {
        Value::Object(map) => {
            let keys: Vec<String> = map.keys().cloned().collect();
            for k in keys {
                if s.extensions.x_embedded_resource && is_meta_field(&k) {
                    continue;
                }
                if let Some(prop) = s.properties.get(&k) {
                    opts.append_key(&k);
                    prune(map.get_mut(&k).unwrap(), Some(prop), opts);
                    opts.parent_path.truncate(orig);
                } else if let Some(ap) = s.additional_properties.as_deref() {
                    opts.append_key(&k);
                    prune(map.get_mut(&k).unwrap(), ap.structural.as_ref(), opts);
                    opts.parent_path.truncate(orig);
                }
            }
        }
        Value::Array(items) => {
            let item_schema = s.items.as_deref();
            for (i, v) in items.iter_mut().enumerate() {
                opts.append_index(i);
                skip_prune(v, item_schema, opts);
                opts.parent_path.truncate(orig);
            }
        }
        _ => {}
    }
    opts.parent_path.truncate(orig);
}
