//! Every container field of a Pod decodes when the body omits it — and the
//! answer comes from a validator, with a field path, not from serde.
//!
//! Go has no required JSON fields: `decoder.Decode(body, &defaultGVK, obj)`
//! fills the zero value and validation answers. Rusternetes modelled 33
//! container-side fields of `resources/pod.rs` as bare non-`Option`, so an
//! omitted `image`, `containerPort`, `mountPath`, `devicePath`, resize policy,
//! restart-rule action, probe port or profile `type` was a serde 400
//! BadRequest — no `reason`, no `details.causes`, no field path (#1939).
//!
//! Making them reachable exposed six validators upstream has and Rusternetes
//! did not have at all — `ValidateEnvFrom`, `ValidateVolumeDevices`,
//! `validateResizePolicy`, the rules half of `validateContainerRestartPolicy`,
//! `validateSeccompProfileField` and `ValidateAppArmorProfileField` — each
//! ported in `crates/common/src/validation/pod.rs` with its upstream citation.
//! Without them these bodies would have been silently *accepted*, which is why
//! the ports and the defaults land together.
//!
//! The table below is the whole audited surface: one row per field, asserted
//! against the error a client actually reads.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::json;

/// `(label, pod spec, substring the answer must contain)`.
fn cases() -> Vec<(&'static str, serde_json::Value, &'static str)> {
    vec![
        (
            "container with no name",
            json!({"containers": [{"image": "nginx"}]}),
            "spec.containers[0].name: Required value",
        ),
        (
            "container with no image",
            json!({"containers": [{"name": "c"}]}),
            "spec.containers[0].image: Required value",
        ),
        (
            "port with no containerPort",
            json!({"containers": [{"name": "c", "image": "n", "ports": [{"protocol": "TCP"}]}]}),
            "spec.containers[0].ports[0].containerPort: Required value",
        ),
        (
            "env with no name",
            json!({"containers": [{"name": "c", "image": "n", "env": [{"value": "v"}]}]}),
            "spec.containers[0].env[0].name: Required value",
        ),
        (
            "envFrom configMapRef with no name",
            json!({"containers": [{"name": "c", "image": "n", "envFrom": [{"configMapRef": {}}]}]}),
            "spec.containers[0].envFrom[0].configMapRef.name: Required value",
        ),
        (
            "envFrom secretRef with no name",
            json!({"containers": [{"name": "c", "image": "n", "envFrom": [{"secretRef": {}}]}]}),
            "spec.containers[0].envFrom[0].secretRef.name: Required value",
        ),
        (
            "configMapKeyRef with no key",
            json!({"containers": [{"name": "c", "image": "n", "env": [{"name": "e", "valueFrom": {"configMapKeyRef": {"name": "cm"}}}]}]}),
            "spec.containers[0].env[0].valueFrom.configMapKeyRef.key: Required value",
        ),
        (
            "configMapKeyRef with no name",
            json!({"containers": [{"name": "c", "image": "n", "env": [{"name": "e", "valueFrom": {"configMapKeyRef": {"key": "k"}}}]}]}),
            "spec.containers[0].env[0].valueFrom.configMapKeyRef.name: Required value",
        ),
        (
            "secretKeyRef with no key",
            json!({"containers": [{"name": "c", "image": "n", "env": [{"name": "e", "valueFrom": {"secretKeyRef": {"name": "s"}}}]}]}),
            "spec.containers[0].env[0].valueFrom.secretKeyRef.key: Required value",
        ),
        (
            "volumeMount with no name",
            json!({"containers": [{"name": "c", "image": "n", "volumeMounts": [{"mountPath": "/x"}]}]}),
            "spec.containers[0].volumeMounts[0].name: Required value",
        ),
        (
            "volumeMount with no mountPath",
            json!({"containers": [{"name": "c", "image": "n", "volumeMounts": [{"name": "v"}]}], "volumes": [{"name": "v", "emptyDir": {}}]}),
            "spec.containers[0].volumeMounts[0].mountPath: Required value",
        ),
        (
            "volumeDevice with no name",
            json!({"containers": [{"name": "c", "image": "n", "volumeDevices": [{"devicePath": "/dev/x"}]}]}),
            "spec.containers[0].volumeDevices[0].name: Required value",
        ),
        (
            "volumeDevice with no devicePath",
            json!({"containers": [{"name": "c", "image": "n", "volumeDevices": [{"name": "v"}]}]}),
            "spec.containers[0].volumeDevices[0].devicePath: Required value",
        ),
        (
            "resizePolicy with no resourceName",
            json!({"containers": [{"name": "c", "image": "n", "resizePolicy": [{"restartPolicy": "NotRequired"}]}]}),
            "spec.containers[0].resizePolicy: Required value",
        ),
        (
            "resizePolicy with no restartPolicy",
            json!({"containers": [{"name": "c", "image": "n", "resizePolicy": [{"resourceName": "cpu"}]}]}),
            "spec.containers[0].resizePolicy: Required value",
        ),
        (
            "restartPolicyRules with no action",
            json!({"restartPolicy": "Never", "containers": [{"name": "c", "image": "n", "restartPolicyRules": [{"exitCodes": {"operator": "In", "values": [1]}}]}]}),
            "spec.containers[0].restartPolicyRules[0].action: Unsupported value: \"\"",
        ),
        (
            "restartPolicyRules exitCodes with no operator",
            json!({"restartPolicy": "Never", "containers": [{"name": "c", "image": "n", "restartPolicyRules": [{"action": "Restart", "exitCodes": {"values": [1]}}]}]}),
            "spec.containers[0].restartPolicyRules[0].exitCodes.operator: Unsupported value: \"\"",
        ),
        (
            "sleep lifecycle handler with no seconds",
            json!({"containers": [{"name": "c", "image": "n", "lifecycle": {"preStop": {"sleep": {}}}}]}),
            "spec.containers[0].lifecycle.preStop.sleep: Invalid value: 0",
        ),
        (
            "exec probe with no command",
            json!({"containers": [{"name": "c", "image": "n", "livenessProbe": {"exec": {}}}]}),
            "spec.containers[0].livenessProbe.exec.command: Required value",
        ),
        (
            "httpGet probe with no port",
            json!({"containers": [{"name": "c", "image": "n", "livenessProbe": {"httpGet": {"path": "/"}}}]}),
            "spec.containers[0].livenessProbe.httpGet.port: Invalid value: 0",
        ),
        (
            "tcpSocket probe with no port",
            json!({"containers": [{"name": "c", "image": "n", "livenessProbe": {"tcpSocket": {}}}]}),
            "spec.containers[0].livenessProbe.tcpSocket.port: Invalid value: 0",
        ),
        (
            "grpc probe with no port",
            json!({"containers": [{"name": "c", "image": "n", "livenessProbe": {"grpc": {}}}]}),
            "spec.containers[0].livenessProbe.grpc.port: Invalid value: 0",
        ),
        (
            "httpHeader with no name",
            json!({"containers": [{"name": "c", "image": "n", "livenessProbe": {"httpGet": {"port": 8080, "httpHeaders": [{"value": "v"}]}}}]}),
            "spec.containers[0].livenessProbe.httpGet.httpHeaders: Invalid value: \"\"",
        ),
        (
            "seccompProfile with no type",
            json!({"containers": [{"name": "c", "image": "n"}], "securityContext": {"seccompProfile": {}}}),
            "spec.securityContext.seccompProfile.type: Required value",
        ),
        (
            "appArmorProfile with no type",
            json!({"containers": [{"name": "c", "image": "n"}], "securityContext": {"appArmorProfile": {}}}),
            "spec.securityContext.appArmorProfile.type: Required value",
        ),
        (
            "container seccompProfile with no type",
            json!({"containers": [{"name": "c", "image": "n", "securityContext": {"seccompProfile": {}}}]}),
            "spec.containers[0].securityContext.seccompProfile.type: Required value",
        ),
        (
            "container appArmorProfile with no type",
            json!({"containers": [{"name": "c", "image": "n", "securityContext": {"appArmorProfile": {}}}]}),
            "spec.containers[0].securityContext.appArmorProfile.type: Required value",
        ),
    ]
}

#[tokio::test]
async fn every_absent_container_field_answers_422_with_a_field_path() {
    let api = TestApiServer::new();

    for (i, (label, spec, expected)) in cases().into_iter().enumerate() {
        let (status, body) = api
            .send(
                "POST",
                "/api/v1/namespaces/default/pods",
                Some("application/json"),
                Some(&json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": { "name": format!("absent-{i}"), "namespace": "default" },
                    "spec": spec,
                })),
            )
            .await;

        assert_ne!(
            status.as_u16(),
            400,
            "{label} was rejected by the decoder, before any validation: {body}"
        );
        assert_eq!(
            status.as_u16(),
            422,
            "{label} must be Invalid, not {status}: {body}"
        );
        let message = body["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(expected),
            "{label} must report `{expected}`, got: {message}"
        );
    }
}
