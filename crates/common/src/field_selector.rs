// Field Selector implementation for server-side filtering
//
// Enables filtering resources by field values like:
// - status.phase=Running
// - spec.nodeName=node-1
// - metadata.namespace=default

use serde_json::Value;
use std::fmt;

/// Field selector for filtering resources by field values
#[derive(Debug, Clone, PartialEq)]
pub struct FieldSelector {
    /// Individual field requirements
    pub requirements: Vec<FieldRequirement>,
}

impl FieldSelector {
    /// Parse a field selector string
    ///
    /// Format: field1=value1,field2!=value2
    /// Operators: =, ==, !=
    ///
    /// Examples:
    /// - "status.phase=Running"
    /// - "spec.nodeName=node-1,status.phase!=Failed"
    /// - "metadata.namespace=default"
    pub fn parse(selector: &str) -> Result<Self, FieldSelectorError> {
        if selector.is_empty() {
            return Ok(Self {
                requirements: Vec::new(),
            });
        }

        let mut requirements = Vec::new();

        // Split by comma
        for requirement_str in selector.split(',') {
            let requirement = FieldRequirement::parse(requirement_str.trim())?;
            requirements.push(requirement);
        }

        Ok(Self { requirements })
    }

    /// Check if a resource matches this field selector
    pub fn matches(&self, resource: &Value) -> bool {
        // Empty selector matches everything
        if self.requirements.is_empty() {
            return true;
        }

        // All requirements must match
        self.requirements.iter().all(|req| req.matches(resource))
    }

    /// Get the individual requirements
    pub fn requirements(&self) -> &[FieldRequirement] {
        &self.requirements
    }

    /// Check if this is an empty selector
    pub fn is_empty(&self) -> bool {
        self.requirements.is_empty()
    }
}

/// A single field requirement
#[derive(Debug, Clone, PartialEq)]
pub struct FieldRequirement {
    /// Field path (e.g., "status.phase", "spec.nodeName")
    pub field: String,
    /// Operator (=, ==, !=)
    pub operator: FieldOperator,
    /// Expected value
    pub value: String,
}

impl FieldRequirement {
    /// Parse a single field requirement
    fn parse(requirement: &str) -> Result<Self, FieldSelectorError> {
        // Try to find operator
        if let Some(pos) = requirement.find("!=") {
            let field = requirement[..pos].trim().to_string();
            let value = requirement[pos + 2..].trim().to_string();
            return Ok(Self {
                field,
                operator: FieldOperator::NotEquals,
                value,
            });
        }

        if let Some(pos) = requirement.find("==") {
            let field = requirement[..pos].trim().to_string();
            let value = requirement[pos + 2..].trim().to_string();
            return Ok(Self {
                field,
                operator: FieldOperator::Equals,
                value,
            });
        }

        if let Some(pos) = requirement.find('=') {
            let field = requirement[..pos].trim().to_string();
            let value = requirement[pos + 1..].trim().to_string();
            return Ok(Self {
                field,
                operator: FieldOperator::Equals,
                value,
            });
        }

        Err(FieldSelectorError::InvalidRequirement(
            requirement.to_string(),
        ))
    }

    /// Check if a resource matches this requirement
    fn matches(&self, resource: &Value) -> bool {
        let field_value = extract_field_value(resource, &self.field);

        match &self.operator {
            FieldOperator::Equals => {
                if let Some(ref fv) = field_value {
                    fv == &self.value
                } else {
                    // Field is missing/null. In Kubernetes, missing boolean fields
                    // default to false, missing string fields default to "".
                    self.value == "false" || self.value.is_empty()
                }
            }
            FieldOperator::NotEquals => {
                if let Some(ref fv) = field_value {
                    fv != &self.value
                } else {
                    // Missing field: treat as "false" or "" for comparison
                    self.value != "false" && !self.value.is_empty()
                }
            }
        }
    }

    /// Get the field path
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Get the operator
    pub fn operator(&self) -> FieldOperator {
        self.operator
    }

    /// Get the expected value
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// Field selector operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldOperator {
    /// Equals (= or ==)
    Equals,
    /// Not equals (!=)
    NotEquals,
}

impl fmt::Display for FieldOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldOperator::Equals => write!(f, "="),
            FieldOperator::NotEquals => write!(f, "!="),
        }
    }
}

/// Extract a field value from a JSON object using a path
///
/// Supports nested fields like "status.phase" or "metadata.namespace"
pub fn extract_field_value(resource: &Value, field_path: &str) -> Option<String> {
    // K8s field selector aliases — some fields are shorthands for nested paths
    // Event `source` falls back to the reporting controller at selection time
    // and never mutates the object: `ToSelectableFields`,
    // pkg/registry/core/event/strategy.go:117-121 (`source := Source.Component;
    // if source == "" { source = ReportingController }`). The core/v1 JSON name
    // of ReportingController is `reportingComponent`.
    if field_path == "source" {
        let component = resource
            .get("source")
            .and_then(|s| s.get("component"))
            .and_then(Value::as_str)
            .unwrap_or("");
        if component.is_empty() {
            // The stored core Event serializes it as `reportingController`
            // (resources/event.rs `reporting_component`).
            return Some(
                resource
                    .get("reportingController")
                    .or_else(|| resource.get("reportingComponent"))
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            );
        }
        return Some(component.to_string());
    }
    // strategy.go:117-121: `"reportingComponent": event.ReportingController`
    // ("use the core/v1 field name").
    if field_path == "reportingComponent" {
        return Some(
            resource
                .get("reportingController")
                .or_else(|| resource.get("reportingComponent"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        );
    }
    // Every other key (`type`, `reason`, `involvedObject.*`, the events.k8s.io/v1
    // `reportingController`, ...) is a plain dotted path into the object.
    let resolved_path = field_path;

    let parts: Vec<&str> = resolved_path.split('.').collect();
    let mut current = resource;

    for part in parts {
        current = current.get(part)?;
    }

    // Convert the final value to string
    match current {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null => Some("null".to_string()),
        _ => None, // Objects and arrays don't convert to simple strings
    }
}

/// Errors that can occur during field selector operations
#[derive(Debug, thiserror::Error)]
pub enum FieldSelectorError {
    #[error("Invalid field selector requirement: {0}")]
    InvalidRequirement(String),

    #[error("Invalid operator in field selector: {0}")]
    InvalidOperator(String),

    #[error("Field not found: {0}")]
    FieldNotFound(String),
}

/// Common field selector patterns for Kubernetes resources
impl FieldSelector {
    /// Match pods by status phase
    pub fn pod_phase(phase: &str) -> Self {
        Self {
            requirements: vec![FieldRequirement {
                field: "status.phase".to_string(),
                operator: FieldOperator::Equals,
                value: phase.to_string(),
            }],
        }
    }

    /// Match pods by node name
    pub fn pod_node(node_name: &str) -> Self {
        Self {
            requirements: vec![FieldRequirement {
                field: "spec.nodeName".to_string(),
                operator: FieldOperator::Equals,
                value: node_name.to_string(),
            }],
        }
    }

    /// Match resources by namespace
    pub fn namespace(namespace: &str) -> Self {
        Self {
            requirements: vec![FieldRequirement {
                field: "metadata.namespace".to_string(),
                operator: FieldOperator::Equals,
                value: namespace.to_string(),
            }],
        }
    }

    /// Match resources by name
    pub fn name(name: &str) -> Self {
        Self {
            requirements: vec![FieldRequirement {
                field: "metadata.name".to_string(),
                operator: FieldOperator::Equals,
                value: name.to_string(),
            }],
        }
    }
}

impl fmt::Display for FieldSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = self
            .requirements
            .iter()
            .map(|req| format!("{}{}{}", req.field, req.operator, req.value))
            .collect();
        write!(f, "{}", parts.join(","))
    }
}
