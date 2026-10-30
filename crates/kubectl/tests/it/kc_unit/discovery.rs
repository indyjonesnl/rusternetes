//! Unit tests moved from `src/discovery.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_client::http::ApiClient;
    pub use serde_json::Value;
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::discovery::*;

    fn fixture() -> Value {
        let raw = include_str!("../../fixtures/aggregated-discovery.json");
        serde_json::from_str(raw).expect("fixture parses")
    }

    #[test]
    fn parses_core_and_grouped_kinds() {
        let mappings = parse_aggregated_discovery(&fixture()).unwrap();
        let dep = mappings
            .iter()
            .find(|m| m.kind == "Deployment")
            .expect("Deployment present");
        assert_eq!(dep.group, "apps");
        assert_eq!(dep.version, "v1");
        assert_eq!(dep.plural, "deployments");
        assert!(dep.namespaced);

        let ns = mappings
            .iter()
            .find(|m| m.kind == "Namespace")
            .expect("Namespace present");
        assert_eq!(ns.group, ""); // core
        assert!(!ns.namespaced);
        assert_eq!(ns.short_names, vec!["ns"]); // short names round-trip
    }

    #[test]
    fn skips_subresources() {
        let mappings = parse_aggregated_discovery(&fixture()).unwrap();
        assert!(mappings.iter().all(|m| !m.plural.contains('/')));
    }

    #[test]
    fn skips_slash_resource_entries() {
        let doc = serde_json::json!({
            "items": [{
                "metadata": {"name": ""},
                "versions": [{"version": "v1", "resources": [
                    {"resource": "pods", "responseKind": {"kind": "Pod"}, "scope": "Namespaced", "singularResource": "pod"},
                    {"resource": "pods/status", "responseKind": {"kind": "Pod"}, "scope": "Namespaced", "singularResource": "pod"}
                ]}]
            }]
        });
        let mappings = parse_aggregated_discovery(&doc).unwrap();
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0].plural, "pods");
    }

    fn mapper() -> RestMapper {
        RestMapper::new(parse_aggregated_discovery(&fixture()).unwrap())
    }

    #[test]
    fn resolves_by_every_key_form() {
        let m = mapper();
        assert_eq!(m.resolve("deployments").unwrap().kind, "Deployment");
        assert_eq!(m.resolve("deployment").unwrap().kind, "Deployment");
        assert_eq!(m.resolve("Deployment").unwrap().kind, "Deployment");
        assert_eq!(m.resolve("DEPLOYMENT").unwrap().kind, "Deployment"); // case-insensitive
    }

    #[test]
    fn resolves_by_short_name() {
        // Use a short name that ACTUALLY EXISTS in the fixture. `namespaces`
        // has shortName "ns".
        let m = mapper();
        assert_eq!(m.resolve("ns").unwrap().kind, "Namespace");
    }

    #[test]
    fn unknown_resolves_to_none() {
        assert!(mapper().resolve("nonexistentthing").is_none());
        assert!(mapper().resolve("").is_none());
    }

    #[test]
    fn event_prefers_core_group() {
        // "Event" exists in both core and events.k8s.io; core wins.
        let m = mapper();
        assert_eq!(m.resolve("Event").unwrap().group, "");
    }

    fn is_writable(m: &ResourceMapping) -> bool {
        m.verbs.iter().any(|v| v == "create" || v == "update")
    }

    /// Anti-drift parity guard: every WRITABLE kind the server serves (per the
    /// discovery fixture) must be resolvable by its plural. This structurally
    /// catches any parse/index regression that silently drops a served kind —
    /// the exact class of bug that motivated the discovery-driven RestMapper
    /// (the client used to lag the server by 31 writable kinds).
    #[test]
    fn mapper_resolves_every_served_writable_kind() {
        let mappings = parse_aggregated_discovery(&fixture()).unwrap();
        let mapper = RestMapper::new(mappings.clone());

        let writable: Vec<&ResourceMapping> = mappings.iter().filter(|m| is_writable(m)).collect();

        let mut unresolved: Vec<String> = Vec::new();
        for m in &writable {
            // Stronger than "resolves to SOMETHING": the plural must resolve to
            // ITS OWN kind. This catches a future shadowing regression where a
            // writable kind is hidden behind a same-plural sibling.
            match mapper.resolve(&m.plural) {
                Some(r) if r.kind == m.kind => { /* ok */ }
                Some(r) => unresolved.push(format!(
                    "{} (plural {} shadowed by kind {})",
                    m.kind, m.plural, r.kind
                )),
                None => unresolved.push(format!("{} (plural {} unresolved)", m.kind, m.plural)),
            }
        }

        eprintln!(
            "parity: verified {} writable kinds resolve to their own kind by plural",
            writable.len()
        );

        assert!(
            unresolved.is_empty(),
            "RestMapper failed to resolve {} served writable kind(s) by plural: {:?}",
            unresolved.len(),
            unresolved
        );
        // A healthy server surface has dozens of writable kinds. If this drops
        // to a handful the fixture is truncated / parsing broke wholesale.
        assert!(
            writable.len() >= 30,
            "expected >=30 writable kinds in fixture, found {} (stale/truncated fixture?)",
            writable.len()
        );
    }

    /// Explicit regression guard for the specific kinds that the legacy
    /// hardcoded table could not touch. Each present one must resolve by BOTH
    /// its plural and its Kind. At least 6 of the 8 must exist in the fixture
    /// (fewer => stale fixture).
    #[test]
    fn previously_unsupported_kinds_resolve() {
        let m = mapper();
        // plural -> Kind for the kinds that motivated the migration.
        let explicit: &[(&str, &str)] = &[
            ("networkpolicies", "NetworkPolicy"),
            ("poddisruptionbudgets", "PodDisruptionBudget"),
            ("endpointslices", "EndpointSlice"),
            ("horizontalpodautoscalers", "HorizontalPodAutoscaler"),
            ("replicasets", "ReplicaSet"),
            ("ingresses", "Ingress"),
            ("runtimeclasses", "RuntimeClass"),
            (
                "validatingwebhookconfigurations",
                "ValidatingWebhookConfiguration",
            ),
        ];

        let mut asserted = 0usize;
        for (plural, kind) in explicit {
            // Only kinds actually present in the fixture are asserted; missing
            // ones are skipped (the >=6 floor below catches a stale fixture).
            if m.resolve(plural).is_none() && m.resolve(kind).is_none() {
                continue;
            }
            assert!(
                m.resolve(plural).is_some(),
                "expected to resolve previously-unsupported plural {plural:?}",
            );
            assert!(
                m.resolve(kind).is_some(),
                "expected to resolve previously-unsupported Kind {kind:?}",
            );
            asserted += 1;
        }

        eprintln!("parity: asserted {asserted} previously-unsupported kinds");
        assert!(
            asserted >= 6,
            "only {asserted} of the previously-unsupported kinds present in the \
             fixture — expected >=6 (stale fixture?)",
        );
    }

    /// Optional live-surface parity: if an api-server is reachable, run the same
    /// writable-kind parity assertion against the real discovery surface. Skips
    /// cleanly (green) when no server is up, so CI without a cluster passes.
    #[tokio::test]
    async fn live_server_writable_parity() {
        let client = match ApiClient::new("https://localhost:6443", true, None) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("parity(live): skipping — could not build client ({e:#})");
                return;
            }
        };
        let mapper = match RestMapper::from_server(&client).await {
            Ok(m) => m,
            Err(e) => {
                eprintln!("parity(live): skipping — no reachable server ({e:#})");
                return;
            }
        };

        let writable: Vec<&ResourceMapping> =
            mapper.all().iter().filter(|m| is_writable(m)).collect();
        let mut unresolved: Vec<String> = Vec::new();
        for m in &writable {
            if mapper.resolve(&m.plural).is_none() {
                unresolved.push(format!("{} ({}/{})", m.kind, m.group, m.plural));
            }
        }
        eprintln!(
            "parity(live): verified {} writable kinds resolve by plural",
            writable.len()
        );
        assert!(
            unresolved.is_empty(),
            "live RestMapper failed to resolve writable kind(s): {unresolved:?}",
        );
    }
}
