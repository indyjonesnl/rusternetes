//! Port of `k8s.io/apimachinery/pkg/api/equality`.

use serde::Serialize;

/// `apiequality.Semantic.DeepEqual` over serialized values: unlike
/// `reflect.DeepEqual` it treats nil and empty slices and maps as equal
/// (third_party/forked/golang/reflect/deep_equal.go). Serialized, "nil" is an
/// absent key or `null`, so both sides drop those and empty containers first.
pub fn semantic_equal<A: Serialize>(a: &A, b: &A) -> bool {
    fn normalize(v: &mut serde_json::Value) -> bool {
        use serde_json::Value;
        match v {
            Value::Null => true,
            Value::Object(map) => {
                map.retain(|_, child| !normalize(child));
                map.is_empty()
            }
            Value::Array(items) => {
                for item in items.iter_mut() {
                    normalize(item);
                }
                items.is_empty()
            }
            _ => false,
        }
    }
    let normalized = |v: &A| {
        let mut v = serde_json::to_value(v).unwrap_or_default();
        if normalize(&mut v) {
            v = serde_json::Value::Null;
        }
        v
    };
    normalized(a) == normalized(b)
}

#[cfg(test)]
mod tests {
    use super::semantic_equal;
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
