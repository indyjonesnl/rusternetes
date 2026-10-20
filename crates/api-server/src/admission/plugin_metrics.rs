//! Which in-tree admission plugin is called for which operation and step, for
//! `WithControllerMetrics`.
//!
//! `NewFromPlugins` decorates every plugin
//! (`staging/src/k8s.io/apiserver/pkg/admission/plugins.go:142-144`,
//! `server/options/admission.go:90`) and the chain skips a plugin whose
//! `Handles(op)` is false (`admission/chain.go:33-35`, `:49-51`) or that does
//! not implement the phase's interface (`chain.go:36`, `:52`, and
//! `metrics/metrics.go:89-113`, which return before observing). So a plugin is
//! observed exactly for the operations its `admission.NewHandler(...)` names
//! and under the steps for which it implements `MutationInterface` (`admit`)
//! and/or `ValidationInterface` (`validate`), whatever the resource.

use rusternetes_common::admission::Operation;

const C: u8 = 1;
const U: u8 = 2;
const D: u8 = 4;

struct Spec {
    name: &'static str,
    /// `NewHandler(...)` operations, as a bit set of [`C`], [`U`], [`D`].
    ops: u8,
    /// `var _ admission.MutationInterface` / an `Admit` method.
    admit: bool,
    /// `var _ admission.ValidationInterface` / a `Validate` method.
    validate: bool,
}

/// The in-tree plugins of `AllOrderedPlugins` (pkg/kubeapiserver/options/
/// plugins.go:70-101) that this server implements, with the citation of each
/// handler and interface assertion (release-1.35, `plugin/pkg/admission/`).
const PLUGINS: &[Spec] = &[
    // limitranger/admission.go:76-77, :206
    Spec {
        name: "LimitRanger",
        ops: C | U,
        admit: true,
        validate: true,
    },
    // serviceaccount/admission.go:89-90, :102
    Spec {
        name: "ServiceAccount",
        ops: C | U,
        admit: true,
        validate: true,
    },
    // noderestriction/admission.go:72, :183 (Admit only)
    Spec {
        name: "NodeRestriction",
        ops: C | U | D,
        admit: true,
        validate: false,
    },
    // security/podsecurity/admission.go:82, :108
    Spec {
        name: "PodSecurity",
        ops: C | U,
        admit: false,
        validate: true,
    },
    // priority/admission.go:56-57, :64
    Spec {
        name: "Priority",
        ops: C | U | D,
        admit: true,
        validate: true,
    },
    // defaulttolerationseconds/admission.go:80, :86
    Spec {
        name: "DefaultTolerationSeconds",
        ops: C | U,
        admit: true,
        validate: false,
    },
    // storage/storageclass/setdefault/admission.go:56, :62
    Spec {
        name: "DefaultStorageClass",
        ops: C,
        admit: true,
        validate: false,
    },
    // storage/storageobjectinuseprotection/admission.go:55, :70
    Spec {
        name: "StorageObjectInUseProtection",
        ops: C,
        admit: true,
        validate: false,
    },
    // runtimeclass/admission.go:65-66, :137
    Spec {
        name: "RuntimeClass",
        ops: C,
        admit: true,
        validate: true,
    },
    // certificates/approval/admission.go:61, :67
    Spec {
        name: "CertificateApproval",
        ops: U,
        admit: false,
        validate: true,
    },
    // certificates/signing/admission.go:64, :70
    Spec {
        name: "CertificateSigning",
        ops: U,
        admit: false,
        validate: true,
    },
    // certificates/ctbattest/admission.go:57, :64
    Spec {
        name: "ClusterTrustBundleAttest",
        ops: C | U,
        admit: false,
        validate: true,
    },
    // certificates/subjectrestriction/admission.go:50, :55
    Spec {
        name: "CertificateSubjectRestriction",
        ops: C,
        admit: false,
        validate: true,
    },
    // podtopologylabels/admission.go:83, :67
    Spec {
        name: "PodTopologyLabels",
        ops: C,
        admit: true,
        validate: false,
    },
    // nodedeclaredfeatures/admission.go:62, :78
    Spec {
        name: "NodeDeclaredFeatureValidator",
        ops: U,
        admit: false,
        validate: true,
    },
    // alwayspullimages/admission.go:57-58, :176
    Spec {
        name: "AlwaysPullImages",
        ops: C | U,
        admit: true,
        validate: true,
    },
    // network/defaultingressclass/admission.go:57, :63 (Admit only)
    Spec {
        name: "DefaultIngressClass",
        ops: C,
        admit: true,
        validate: false,
    },
    // namespace/autoprovision/admission.go (Admit only, NewHandler(Create))
    Spec {
        name: "NamespaceAutoProvision",
        ops: C,
        admit: true,
        validate: false,
    },
    // namespace/exists/admission.go (Validate only, NewHandler(Create, Update, Delete))
    Spec {
        name: "NamespaceExists",
        ops: C | U | D,
        admit: false,
        validate: true,
    },
    // podnodeselector/admission.go (Admit and Validate, NewHandler(Create))
    Spec {
        name: "PodNodeSelector",
        ops: C,
        admit: true,
        validate: true,
    },
    // podtolerationrestriction/admission.go (Admit and Validate,
    // NewHandler(Create, Update))
    Spec {
        name: "PodTolerationRestriction",
        ops: C | U,
        admit: true,
        validate: true,
    },
    // imagepolicy/admission.go:91 (ValidationInterface only), :284
    // (NewHandler(Create, Update))
    Spec {
        name: "ImagePolicyWebhook",
        ops: C | U,
        admit: false,
        validate: true,
    },
    // eventratelimit/admission.go:64 (ValidationInterface only), :78
    // (NewHandler(Create, Update))
    Spec {
        name: "EventRateLimit",
        ops: C | U,
        admit: false,
        validate: true,
    },
    // antiaffinity/admission.go:45, :50
    Spec {
        name: "LimitPodHardAntiAffinityTopology",
        ops: C | U,
        admit: false,
        validate: true,
    },
    // storage/persistentvolume/resize/admission.go:46, :57
    Spec {
        name: "PersistentVolumeClaimResize",
        ops: U,
        admit: false,
        validate: true,
    },
    // network/denyserviceexternalips/admission.go:49, :54
    Spec {
        name: "DenyServiceExternalIPs",
        ops: C | U,
        admit: false,
        validate: true,
    },
];

fn op_bit(op: &Operation) -> u8 {
    match op {
        Operation::Create => C,
        Operation::Update => U,
        Operation::Delete => D,
        Operation::Connect => 0,
    }
}

/// Whether the chain calls `name` for `op` in the `admit` / `validate` step
/// (`step_type`), i.e. whether upstream's decorator would observe it.
pub fn observed(name: &str, op: &Operation, step_type: &str) -> bool {
    let Some(spec) = PLUGINS.iter().find(|s| s.name == name) else {
        return false;
    };
    let implements = match step_type {
        "admit" => spec.admit,
        "validate" => spec.validate,
        _ => false,
    };
    implements && spec.ops & op_bit(op) != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handlers_match_upstream_new_handler() {
        // NodeRestriction: Create, Update, Delete; Admit only.
        for op in [Operation::Create, Operation::Update, Operation::Delete] {
            assert!(observed("NodeRestriction", &op, "admit"));
            assert!(!observed("NodeRestriction", &op, "validate"));
        }
        // LimitRanger: Create, Update, both steps; never Delete.
        assert!(observed("LimitRanger", &Operation::Update, "validate"));
        assert!(!observed("LimitRanger", &Operation::Delete, "validate"));
        // Priority: Create, Update, Delete, both steps.
        assert!(observed("Priority", &Operation::Delete, "validate"));
        // RuntimeClass and DefaultStorageClass: Create only.
        assert!(observed("RuntimeClass", &Operation::Create, "admit"));
        assert!(!observed("RuntimeClass", &Operation::Update, "admit"));
        assert!(!observed(
            "DefaultStorageClass",
            &Operation::Update,
            "admit"
        ));
        // The certificate approval/signing plugins and the node-declared
        // feature validator: Update, Validate only.
        for p in [
            "CertificateApproval",
            "CertificateSigning",
            "NodeDeclaredFeatureValidator",
        ] {
            assert!(observed(p, &Operation::Update, "validate"));
            assert!(!observed(p, &Operation::Update, "admit"));
            assert!(!observed(p, &Operation::Create, "validate"));
        }
        // Connect is handled by no in-tree plugin.
        assert!(!observed("LimitRanger", &Operation::Connect, "admit"));
        // Unknown plugins are not observed.
        assert!(!observed("NoSuchPlugin", &Operation::Create, "admit"));
    }
}
