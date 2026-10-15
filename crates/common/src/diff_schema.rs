//! Schema-driven [`GoJson`] rendering of Go's INTERNAL (`pkg/apis/core`)
//! types, for the `diff.Diff(old, new)` texts upstream validation appends to
//! its "may not be changed" errors (validation.go:6206).
//!
//! The internal types (`pkg/apis/core/types.go`, release-1.35) carry no json
//! tags, so `json.MarshalIndent` emits Go field names in declaration order
//! with no omitempty; an embedded struct without a tag is inlined; a nil
//! pointer/slice/map is `null` and a struct value is always an object. Each
//! [`Field`] table below is the field list of the named Go struct, paired
//! with the key our serde model serialises it under.

use serde_json::Value;

use crate::diff::GoJson;
use crate::quantity::Quantity;

#[derive(Clone, Copy)]
pub enum Kind {
    /// `string` and named string types: `""` when absent.
    Str,
    /// `bool`: `false` when absent.
    Bool,
    /// Integer value type: `0` when absent.
    Int,
    /// `*string` and pointers to named strings: `null` when absent.
    PtrStr,
    /// `*bool`.
    PtrBool,
    /// `*int64` / `*int32`.
    PtrInt,
    /// `[]string` (nil slice is `null`).
    StrSlice,
    /// `[]int32`.
    IntSlice,
    /// Struct value: an object of its zero value when absent.
    Struct(&'static [Field]),
    /// Pointer to struct: `null` when absent.
    PtrStruct(&'static [Field]),
    /// Slice of structs: `null` when absent.
    Slice(&'static [Field]),
    /// `resource.Quantity` value: canonical string, `"0"` when absent.
    Quantity,
    /// `ResourceList`: sorted keys, canonical quantity strings, nil is `null`.
    QuantityMap,
    /// Not modelled: rendered from the serde value as is. Documented gap.
    Raw,
}

#[derive(Clone, Copy)]
pub struct Field {
    pub go: &'static str,
    pub key: &'static str,
    pub kind: Kind,
}

const fn f(go: &'static str, key: &'static str, kind: Kind) -> Field {
    Field { go, key, kind }
}

/// `core.LocalObjectReference` (inlined where embedded).
const NAME: Field = f("Name", "name", Kind::Str);

const CONTAINER_PORT: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("HostPort", "hostPort", Kind::Int),
    f("ContainerPort", "containerPort", Kind::Int),
    f("Protocol", "protocol", Kind::Str),
    f("HostIP", "hostIP", Kind::Str),
];
const ENV_SOURCE: &[Field] = &[NAME, f("Optional", "optional", Kind::PtrBool)];
const ENV_FROM_SOURCE: &[Field] = &[
    f("Prefix", "prefix", Kind::Str),
    f("ConfigMapRef", "configMapRef", Kind::PtrStruct(ENV_SOURCE)),
    f("SecretRef", "secretRef", Kind::PtrStruct(ENV_SOURCE)),
];
const OBJECT_FIELD_SELECTOR: &[Field] = &[
    f("APIVersion", "apiVersion", Kind::Str),
    f("FieldPath", "fieldPath", Kind::Str),
];
const RESOURCE_FIELD_SELECTOR: &[Field] = &[
    f("ContainerName", "containerName", Kind::Str),
    f("Resource", "resource", Kind::Str),
    f("Divisor", "divisor", Kind::Quantity),
];
const KEY_SELECTOR: &[Field] = &[
    NAME,
    f("Key", "key", Kind::Str),
    f("Optional", "optional", Kind::PtrBool),
];
const FILE_KEY_SELECTOR: &[Field] = &[
    f("VolumeName", "volumeName", Kind::Str),
    f("Path", "path", Kind::Str),
    f("Key", "key", Kind::Str),
    f("Optional", "optional", Kind::PtrBool),
];
const ENV_VAR_SOURCE: &[Field] = &[
    f(
        "FieldRef",
        "fieldRef",
        Kind::PtrStruct(OBJECT_FIELD_SELECTOR),
    ),
    f(
        "ResourceFieldRef",
        "resourceFieldRef",
        Kind::PtrStruct(RESOURCE_FIELD_SELECTOR),
    ),
    f(
        "ConfigMapKeyRef",
        "configMapKeyRef",
        Kind::PtrStruct(KEY_SELECTOR),
    ),
    f(
        "SecretKeyRef",
        "secretKeyRef",
        Kind::PtrStruct(KEY_SELECTOR),
    ),
    f(
        "FileKeyRef",
        "fileKeyRef",
        Kind::PtrStruct(FILE_KEY_SELECTOR),
    ),
];
const ENV_VAR: &[Field] = &[
    NAME,
    f("Value", "value", Kind::Str),
    f("ValueFrom", "valueFrom", Kind::PtrStruct(ENV_VAR_SOURCE)),
];
const RESOURCE_CLAIM: &[Field] = &[NAME, f("Request", "request", Kind::Str)];
const RESOURCE_REQUIREMENTS: &[Field] = &[
    f("Limits", "limits", Kind::QuantityMap),
    f("Requests", "requests", Kind::QuantityMap),
    f("Claims", "claims", Kind::Slice(RESOURCE_CLAIM)),
];
const CONTAINER_RESIZE_POLICY: &[Field] = &[
    f("ResourceName", "resourceName", Kind::Str),
    f("RestartPolicy", "restartPolicy", Kind::Str),
];
const EXIT_CODES: &[Field] = &[
    f("Operator", "operator", Kind::Str),
    f("Values", "values", Kind::IntSlice),
];
const CONTAINER_RESTART_RULE: &[Field] = &[
    f("Action", "action", Kind::Str),
    f("ExitCodes", "exitCodes", Kind::PtrStruct(EXIT_CODES)),
];
const VOLUME_MOUNT: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("ReadOnly", "readOnly", Kind::Bool),
    f("RecursiveReadOnly", "recursiveReadOnly", Kind::PtrStr),
    f("MountPath", "mountPath", Kind::Str),
    f("SubPath", "subPath", Kind::Str),
    f("MountPropagation", "mountPropagation", Kind::PtrStr),
    f("SubPathExpr", "subPathExpr", Kind::Str),
];
const VOLUME_DEVICE: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("DevicePath", "devicePath", Kind::Str),
];
const CAPABILITIES: &[Field] = &[
    f("Add", "add", Kind::StrSlice),
    f("Drop", "drop", Kind::StrSlice),
];
const SELINUX_OPTIONS: &[Field] = &[
    f("User", "user", Kind::Str),
    f("Role", "role", Kind::Str),
    f("Type", "type", Kind::Str),
    f("Level", "level", Kind::Str),
];
const WINDOWS_OPTIONS: &[Field] = &[
    f(
        "GMSACredentialSpecName",
        "gmsaCredentialSpecName",
        Kind::PtrStr,
    ),
    f("GMSACredentialSpec", "gmsaCredentialSpec", Kind::PtrStr),
    f("RunAsUserName", "runAsUserName", Kind::PtrStr),
    f("HostProcess", "hostProcess", Kind::PtrBool),
];
const PROFILE: &[Field] = &[
    f("Type", "type", Kind::Str),
    f("LocalhostProfile", "localhostProfile", Kind::PtrStr),
];
const SECURITY_CONTEXT: &[Field] = &[
    f(
        "Capabilities",
        "capabilities",
        Kind::PtrStruct(CAPABILITIES),
    ),
    f("Privileged", "privileged", Kind::PtrBool),
    f(
        "SELinuxOptions",
        "seLinuxOptions",
        Kind::PtrStruct(SELINUX_OPTIONS),
    ),
    f(
        "WindowsOptions",
        "windowsOptions",
        Kind::PtrStruct(WINDOWS_OPTIONS),
    ),
    f("RunAsUser", "runAsUser", Kind::PtrInt),
    f("RunAsGroup", "runAsGroup", Kind::PtrInt),
    f("RunAsNonRoot", "runAsNonRoot", Kind::PtrBool),
    f(
        "ReadOnlyRootFilesystem",
        "readOnlyRootFilesystem",
        Kind::PtrBool,
    ),
    f(
        "AllowPrivilegeEscalation",
        "allowPrivilegeEscalation",
        Kind::PtrBool,
    ),
    f("ProcMount", "procMount", Kind::PtrStr),
    f("SeccompProfile", "seccompProfile", Kind::PtrStruct(PROFILE)),
    f(
        "AppArmorProfile",
        "appArmorProfile",
        Kind::PtrStruct(PROFILE),
    ),
];

/// `core.EphemeralContainer` (pkg/apis/core/types.go): the untagged embedded
/// `EphemeralContainerCommon` is inlined, then `TargetContainerName`. Probes
/// and Lifecycle are [`Kind::Raw`] (not modelled); validation forbids them on
/// an ephemeral container.
pub const EPHEMERAL_CONTAINER: &[Field] = &[
    f("Name", "name", Kind::Str),
    f("Image", "image", Kind::Str),
    f("Command", "command", Kind::StrSlice),
    f("Args", "args", Kind::StrSlice),
    f("WorkingDir", "workingDir", Kind::Str),
    f("Ports", "ports", Kind::Slice(CONTAINER_PORT)),
    f("EnvFrom", "envFrom", Kind::Slice(ENV_FROM_SOURCE)),
    f("Env", "env", Kind::Slice(ENV_VAR)),
    f(
        "Resources",
        "resources",
        Kind::Struct(RESOURCE_REQUIREMENTS),
    ),
    f(
        "ResizePolicy",
        "resizePolicy",
        Kind::Slice(CONTAINER_RESIZE_POLICY),
    ),
    f("RestartPolicy", "restartPolicy", Kind::PtrStr),
    f(
        "RestartPolicyRules",
        "restartPolicyRules",
        Kind::Slice(CONTAINER_RESTART_RULE),
    ),
    f("VolumeMounts", "volumeMounts", Kind::Slice(VOLUME_MOUNT)),
    f("VolumeDevices", "volumeDevices", Kind::Slice(VOLUME_DEVICE)),
    f("LivenessProbe", "livenessProbe", Kind::Raw),
    f("ReadinessProbe", "readinessProbe", Kind::Raw),
    f("StartupProbe", "startupProbe", Kind::Raw),
    f("Lifecycle", "lifecycle", Kind::Raw),
    f(
        "TerminationMessagePath",
        "terminationMessagePath",
        Kind::Str,
    ),
    f(
        "TerminationMessagePolicy",
        "terminationMessagePolicy",
        Kind::Str,
    ),
    f("ImagePullPolicy", "imagePullPolicy", Kind::Str),
    f(
        "SecurityContext",
        "securityContext",
        Kind::PtrStruct(SECURITY_CONTEXT),
    ),
    f("Stdin", "stdin", Kind::Bool),
    f("StdinOnce", "stdinOnce", Kind::Bool),
    f("TTY", "tty", Kind::Bool),
    f("TargetContainerName", "targetContainerName", Kind::Str),
];

fn render_struct(fields: &[Field], v: Option<&Value>) -> GoJson {
    GoJson::Object(
        fields
            .iter()
            .map(|fl| {
                (
                    fl.go.to_string(),
                    render(fl.kind, v.and_then(|o| o.get(fl.key))),
                )
            })
            .collect(),
    )
}

/// Render `v` as Go's `json.MarshalIndent` would emit a value of `kind`.
pub fn render(kind: Kind, v: Option<&Value>) -> GoJson {
    let v = v.filter(|v| !v.is_null());
    match kind {
        Kind::Str => GoJson::str(v.and_then(Value::as_str).unwrap_or_default()),
        Kind::Bool => GoJson::Bool(v.and_then(Value::as_bool).unwrap_or(false)),
        Kind::Int => GoJson::Number(v.map_or("0".to_string(), Value::to_string)),
        Kind::PtrStr => v.and_then(Value::as_str).map_or(GoJson::Null, GoJson::str),
        Kind::PtrBool => v
            .and_then(Value::as_bool)
            .map_or(GoJson::Null, GoJson::Bool),
        Kind::PtrInt => v.map_or(GoJson::Null, |n| GoJson::Number(n.to_string())),
        Kind::StrSlice => v.and_then(Value::as_array).map_or(GoJson::Null, |a| {
            GoJson::Array(
                a.iter()
                    .map(|s| GoJson::str(s.as_str().unwrap_or_default()))
                    .collect(),
            )
        }),
        Kind::IntSlice => v.and_then(Value::as_array).map_or(GoJson::Null, |a| {
            GoJson::Array(a.iter().map(|n| GoJson::Number(n.to_string())).collect())
        }),
        Kind::Struct(fields) => render_struct(fields, v),
        Kind::PtrStruct(fields) => v.map_or(GoJson::Null, |v| render_struct(fields, Some(v))),
        Kind::Slice(fields) => v.and_then(Value::as_array).map_or(GoJson::Null, |a| {
            GoJson::Array(a.iter().map(|e| render_struct(fields, Some(e))).collect())
        }),
        Kind::Quantity => GoJson::str(v.and_then(Value::as_str).map_or_else(
            || "0".to_string(),
            |raw| {
                Quantity::parse(raw)
                    .map(|q| q.canonical_string())
                    .unwrap_or_else(|_| raw.to_string())
            },
        )),
        Kind::QuantityMap => v.and_then(Value::as_object).map_or(GoJson::Null, |m| {
            let mut e: Vec<(String, GoJson)> = m
                .iter()
                .map(|(k, raw)| (k.clone(), render(Kind::Quantity, Some(raw))))
                .collect();
            e.sort_by(|a, b| a.0.cmp(&b.0));
            GoJson::Object(e)
        }),
        Kind::Raw => v.map_or(GoJson::Null, raw_json),
    }
}

fn raw_json(v: &Value) -> GoJson {
    match v {
        Value::Null => GoJson::Null,
        Value::Bool(b) => GoJson::Bool(*b),
        Value::Number(n) => GoJson::Number(n.to_string()),
        Value::String(s) => GoJson::str(s.clone()),
        Value::Array(a) => GoJson::Array(a.iter().map(raw_json).collect()),
        Value::Object(o) => {
            GoJson::Object(o.iter().map(|(k, v)| (k.clone(), raw_json(v))).collect())
        }
    }
}

/// `diff.Diff(old, new)` of two values of the struct `fields` describes.
pub fn struct_diff(fields: &[Field], old: &Value, new: &Value) -> String {
    crate::diff::diff(
        &render_struct(fields, Some(old)),
        &render_struct(fields, Some(new)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Nested internal types render with Go field names, `null` for nil
    /// pointers/slices, canonical quantities and sorted resource keys.
    #[test]
    fn renders_internal_ephemeral_container_shape() {
        let c = json!({
            "name": "d", "image": "busybox", "tty": true,
            "env": [{"name": "A", "value": "1"}],
            "resources": {"requests": {"memory": "1024Mi", "cpu": "1"}},
            "securityContext": {"runAsUser": 0}
        });
        let out = render_struct(EPHEMERAL_CONTAINER, Some(&c)).marshal_indent();
        for want in [
            " \"Env\": [\n  {\n   \"Name\": \"A\",\n   \"Value\": \"1\",\n   \"ValueFrom\": null\n  }\n ],",
            " \"Resources\": {\n  \"Limits\": null,\n  \"Requests\": {\n   \"cpu\": \"1\",\n   \"memory\": \"1Gi\"\n  },\n  \"Claims\": null\n },",
            "  \"RunAsUser\": 0,",
            " \"TTY\": true,",
        ] {
            assert!(out.contains(want), "missing {want:?} in\n{out}");
        }
    }
}
