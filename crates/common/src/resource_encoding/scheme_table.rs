//! Generated from release-1.35 `pkg/apis/*/install/install.go` (version priority),
//! `staging/src/k8s.io/api/*/*/register.go` (registered kinds) and
//! `zz_generated.prerelease-lifecycle.go` (`APILifecycleIntroduced`).

use super::{GroupTable, VersionTable};

pub(super) const SCHEME_TABLE: &[GroupTable] = &[
    GroupTable {
        group: "admission.k8s.io",
        versions: &[
            VersionTable {
                version: "v1",
                kinds: &[("AdmissionReview", Some((1, 19)))],
            },
            VersionTable {
                version: "v1beta1",
                kinds: &[("AdmissionReview", Some((1, 9)))],
            },
        ],
    },
    GroupTable {
        group: "admissionregistration.k8s.io",
        versions: &[
            VersionTable {
                version: "v1",
                kinds: &[
                    ("ValidatingWebhookConfiguration", Some((1, 16))),
                    ("ValidatingWebhookConfigurationList", Some((1, 16))),
                    ("MutatingWebhookConfiguration", Some((1, 16))),
                    ("MutatingWebhookConfigurationList", Some((1, 16))),
                    ("ValidatingAdmissionPolicy", Some((1, 30))),
                    ("ValidatingAdmissionPolicyList", Some((1, 30))),
                    ("ValidatingAdmissionPolicyBinding", Some((1, 30))),
                    ("ValidatingAdmissionPolicyBindingList", Some((1, 30))),
                ],
            },
            VersionTable {
                version: "v1beta1",
                kinds: &[
                    ("ValidatingWebhookConfiguration", Some((1, 9))),
                    ("ValidatingWebhookConfigurationList", Some((1, 9))),
                    ("MutatingWebhookConfiguration", Some((1, 9))),
                    ("MutatingWebhookConfigurationList", Some((1, 9))),
                    ("ValidatingAdmissionPolicy", Some((1, 28))),
                    ("ValidatingAdmissionPolicyList", Some((1, 28))),
                    ("ValidatingAdmissionPolicyBinding", Some((1, 28))),
                    ("ValidatingAdmissionPolicyBindingList", Some((1, 28))),
                    ("MutatingAdmissionPolicy", Some((1, 34))),
                    ("MutatingAdmissionPolicyList", Some((1, 34))),
                    ("MutatingAdmissionPolicyBinding", Some((1, 34))),
                    ("MutatingAdmissionPolicyBindingList", Some((1, 34))),
                ],
            },
            VersionTable {
                version: "v1alpha1",
                kinds: &[
                    ("ValidatingAdmissionPolicy", Some((1, 26))),
                    ("ValidatingAdmissionPolicyList", Some((1, 26))),
                    ("ValidatingAdmissionPolicyBinding", Some((1, 26))),
                    ("ValidatingAdmissionPolicyBindingList", Some((1, 26))),
                    ("MutatingAdmissionPolicy", Some((1, 32))),
                    ("MutatingAdmissionPolicyList", Some((1, 32))),
                    ("MutatingAdmissionPolicyBinding", Some((1, 32))),
                    ("MutatingAdmissionPolicyBindingList", Some((1, 32))),
                ],
            },
        ],
    },
    GroupTable {
        group: "internal.apiserver.k8s.io",
        versions: &[VersionTable {
            version: "v1alpha1",
            kinds: &[("StorageVersion", None), ("StorageVersionList", None)],
        }],
    },
    GroupTable {
        group: "apps",
        versions: &[
            VersionTable {
                version: "v1",
                kinds: &[
                    ("Deployment", Some((1, 9))),
                    ("DeploymentList", Some((1, 9))),
                    ("StatefulSet", Some((1, 9))),
                    ("StatefulSetList", Some((1, 9))),
                    ("DaemonSet", Some((1, 9))),
                    ("DaemonSetList", Some((1, 9))),
                    ("ReplicaSet", Some((1, 9))),
                    ("ReplicaSetList", Some((1, 9))),
                    ("ControllerRevision", Some((1, 9))),
                    ("ControllerRevisionList", Some((1, 9))),
                ],
            },
            VersionTable {
                version: "v1beta2",
                kinds: &[
                    ("Deployment", Some((1, 8))),
                    ("DeploymentList", Some((1, 8))),
                    ("Scale", Some((1, 8))),
                    ("StatefulSet", Some((1, 8))),
                    ("StatefulSetList", Some((1, 8))),
                    ("DaemonSet", Some((1, 8))),
                    ("DaemonSetList", Some((1, 8))),
                    ("ReplicaSet", Some((1, 8))),
                    ("ReplicaSetList", Some((1, 8))),
                    ("ControllerRevision", Some((1, 8))),
                    ("ControllerRevisionList", Some((1, 8))),
                ],
            },
            VersionTable {
                version: "v1beta1",
                kinds: &[
                    ("Deployment", Some((1, 6))),
                    ("DeploymentList", Some((1, 6))),
                    ("DeploymentRollback", Some((1, 6))),
                    ("Scale", Some((1, 6))),
                    ("StatefulSet", Some((1, 5))),
                    ("StatefulSetList", Some((1, 5))),
                    ("ControllerRevision", Some((1, 7))),
                    ("ControllerRevisionList", Some((1, 7))),
                ],
            },
        ],
    },
    GroupTable {
        group: "authentication.k8s.io",
        versions: &[
            VersionTable {
                version: "v1",
                kinds: &[
                    ("TokenReview", Some((1, 6))),
                    ("TokenRequest", Some((1, 10))),
                    ("SelfSubjectReview", Some((1, 28))),
                ],
            },
            VersionTable {
                version: "v1beta1",
                kinds: &[
                    ("SelfSubjectReview", Some((1, 27))),
                    ("TokenReview", Some((1, 4))),
                ],
            },
            VersionTable {
                version: "v1alpha1",
                kinds: &[("SelfSubjectReview", Some((1, 26)))],
            },
        ],
    },
    GroupTable {
        group: "authorization.k8s.io",
        versions: &[
            VersionTable {
                version: "v1",
                kinds: &[
                    ("SelfSubjectRulesReview", Some((1, 19))),
                    ("SelfSubjectAccessReview", Some((1, 19))),
                    ("SubjectAccessReview", Some((1, 6))),
                    ("LocalSubjectAccessReview", Some((1, 19))),
                ],
            },
            VersionTable {
                version: "v1beta1",
                kinds: &[
                    ("SelfSubjectRulesReview", Some((1, 8))),
                    ("SelfSubjectAccessReview", Some((1, 2))),
                    ("SubjectAccessReview", Some((1, 2))),
                    ("LocalSubjectAccessReview", Some((1, 2))),
                ],
            },
        ],
    },
    GroupTable {
        group: "autoscaling",
        versions: &[
            VersionTable {
                version: "v2",
                kinds: &[
                    ("HorizontalPodAutoscaler", Some((1, 23))),
                    ("HorizontalPodAutoscalerList", Some((1, 23))),
                ],
            },
            VersionTable {
                version: "v1",
                kinds: &[
                    ("HorizontalPodAutoscaler", Some((1, 2))),
                    ("HorizontalPodAutoscalerList", Some((1, 2))),
                    ("Scale", Some((1, 2))),
                ],
            },
            VersionTable {
                version: "v2beta1",
                kinds: &[
                    ("HorizontalPodAutoscaler", Some((1, 8))),
                    ("HorizontalPodAutoscalerList", Some((1, 8))),
                ],
            },
            VersionTable {
                version: "v2beta2",
                kinds: &[
                    ("HorizontalPodAutoscaler", Some((1, 12))),
                    ("HorizontalPodAutoscalerList", Some((1, 12))),
                ],
            },
        ],
    },
    GroupTable {
        group: "batch",
        versions: &[
            VersionTable {
                version: "v1",
                kinds: &[
                    ("Job", Some((1, 2))),
                    ("JobList", Some((1, 2))),
                    ("CronJob", Some((1, 21))),
                    ("CronJobList", Some((1, 21))),
                ],
            },
            VersionTable {
                version: "v1beta1",
                kinds: &[("CronJob", Some((1, 8))), ("CronJobList", Some((1, 8)))],
            },
        ],
    },
    GroupTable {
        group: "certificates.k8s.io",
        versions: &[
            VersionTable {
                version: "v1",
                kinds: &[
                    ("CertificateSigningRequest", Some((1, 19))),
                    ("CertificateSigningRequestList", Some((1, 19))),
                ],
            },
            VersionTable {
                version: "v1beta1",
                kinds: &[
                    ("CertificateSigningRequest", Some((1, 12))),
                    ("CertificateSigningRequestList", Some((1, 12))),
                    ("ClusterTrustBundle", Some((1, 33))),
                    ("ClusterTrustBundleList", Some((1, 33))),
                    ("PodCertificateRequest", Some((1, 35))),
                    ("PodCertificateRequestList", Some((1, 35))),
                ],
            },
            VersionTable {
                version: "v1alpha1",
                kinds: &[
                    ("ClusterTrustBundle", Some((1, 26))),
                    ("ClusterTrustBundleList", Some((1, 26))),
                ],
            },
        ],
    },
    GroupTable {
        group: "coordination.k8s.io",
        versions: &[
            VersionTable {
                version: "v1",
                kinds: &[("Lease", Some((1, 14))), ("LeaseList", Some((1, 19)))],
            },
            VersionTable {
                version: "v1beta1",
                kinds: &[
                    ("Lease", Some((1, 12))),
                    ("LeaseList", Some((1, 12))),
                    ("LeaseCandidate", Some((1, 33))),
                    ("LeaseCandidateList", Some((1, 33))),
                ],
            },
            VersionTable {
                version: "v1alpha2",
                kinds: &[
                    ("LeaseCandidate", Some((1, 32))),
                    ("LeaseCandidateList", Some((1, 32))),
                ],
            },
        ],
    },
    GroupTable {
        group: "",
        versions: &[VersionTable {
            version: "v1",
            kinds: &[
                ("Pod", Some((1, 0))),
                ("PodList", Some((1, 0))),
                ("PodStatusResult", Some((1, 0))),
                ("PodTemplate", Some((1, 0))),
                ("PodTemplateList", Some((1, 0))),
                ("ReplicationController", Some((1, 0))),
                ("ReplicationControllerList", Some((1, 0))),
                ("Service", Some((1, 0))),
                ("ServiceProxyOptions", Some((1, 2))),
                ("ServiceList", Some((1, 0))),
                ("Endpoints", Some((1, 0))),
                ("EndpointsList", Some((1, 0))),
                ("Node", Some((1, 0))),
                ("NodeList", Some((1, 0))),
                ("NodeProxyOptions", Some((1, 2))),
                ("Binding", Some((1, 0))),
                ("Event", Some((1, 0))),
                ("EventList", Some((1, 0))),
                ("List", Some((1, 0))),
                ("LimitRange", Some((1, 0))),
                ("LimitRangeList", Some((1, 0))),
                ("ResourceQuota", Some((1, 0))),
                ("ResourceQuotaList", Some((1, 0))),
                ("Namespace", Some((1, 0))),
                ("NamespaceList", Some((1, 0))),
                ("Secret", Some((1, 0))),
                ("SecretList", Some((1, 0))),
                ("ServiceAccount", Some((1, 0))),
                ("ServiceAccountList", Some((1, 0))),
                ("PersistentVolume", Some((1, 0))),
                ("PersistentVolumeList", Some((1, 0))),
                ("PersistentVolumeClaim", Some((1, 0))),
                ("PersistentVolumeClaimList", Some((1, 0))),
                ("PodAttachOptions", Some((1, 1))),
                ("PodLogOptions", Some((1, 0))),
                ("PodExecOptions", Some((1, 0))),
                ("PodPortForwardOptions", Some((1, 6))),
                ("PodProxyOptions", Some((1, 0))),
                ("ComponentStatus", None),
                ("ComponentStatusList", None),
                ("SerializedReference", Some((1, 0))),
                ("RangeAllocation", Some((1, 0))),
                ("ConfigMap", Some((1, 2))),
                ("ConfigMapList", Some((1, 2))),
            ],
        }],
    },
    GroupTable {
        group: "discovery.k8s.io",
        versions: &[
            VersionTable {
                version: "v1",
                kinds: &[
                    ("EndpointSlice", Some((1, 21))),
                    ("EndpointSliceList", Some((1, 21))),
                ],
            },
            VersionTable {
                version: "v1beta1",
                kinds: &[
                    ("EndpointSlice", Some((1, 16))),
                    ("EndpointSliceList", Some((1, 16))),
                ],
            },
        ],
    },
    GroupTable {
        group: "events.k8s.io",
        versions: &[
            VersionTable {
                version: "v1",
                kinds: &[("Event", Some((1, 19))), ("EventList", Some((1, 19)))],
            },
            VersionTable {
                version: "v1beta1",
                kinds: &[("Event", Some((1, 8))), ("EventList", Some((1, 8)))],
            },
        ],
    },
    GroupTable {
        group: "extensions",
        versions: &[VersionTable {
            version: "v1beta1",
            kinds: &[
                ("Deployment", Some((1, 1))),
                ("DeploymentList", Some((1, 1))),
                ("DeploymentRollback", Some((1, 2))),
                ("Scale", Some((1, 1))),
                ("DaemonSetList", Some((1, 1))),
                ("DaemonSet", Some((1, 1))),
                ("Ingress", Some((1, 1))),
                ("IngressList", Some((1, 1))),
                ("ReplicaSet", Some((1, 2))),
                ("ReplicaSetList", Some((1, 2))),
                ("NetworkPolicy", Some((1, 3))),
                ("NetworkPolicyList", Some((1, 3))),
            ],
        }],
    },
    GroupTable {
        group: "flowcontrol.apiserver.k8s.io",
        versions: &[
            VersionTable {
                version: "v1",
                kinds: &[
                    ("FlowSchema", Some((1, 29))),
                    ("FlowSchemaList", Some((1, 29))),
                    ("PriorityLevelConfiguration", Some((1, 29))),
                    ("PriorityLevelConfigurationList", Some((1, 29))),
                ],
            },
            VersionTable {
                version: "v1beta3",
                kinds: &[
                    ("FlowSchema", Some((1, 26))),
                    ("FlowSchemaList", Some((1, 26))),
                    ("PriorityLevelConfiguration", Some((1, 26))),
                    ("PriorityLevelConfigurationList", Some((1, 26))),
                ],
            },
            VersionTable {
                version: "v1beta2",
                kinds: &[
                    ("FlowSchema", Some((1, 23))),
                    ("FlowSchemaList", Some((1, 23))),
                    ("PriorityLevelConfiguration", Some((1, 23))),
                    ("PriorityLevelConfigurationList", Some((1, 23))),
                ],
            },
            VersionTable {
                version: "v1beta1",
                kinds: &[
                    ("FlowSchema", Some((1, 20))),
                    ("FlowSchemaList", Some((1, 20))),
                    ("PriorityLevelConfiguration", Some((1, 20))),
                    ("PriorityLevelConfigurationList", Some((1, 20))),
                ],
            },
        ],
    },
    GroupTable {
        group: "imagepolicy.k8s.io",
        versions: &[VersionTable {
            version: "v1alpha1",
            kinds: &[("ImageReview", None)],
        }],
    },
    GroupTable {
        group: "networking.k8s.io",
        versions: &[
            VersionTable {
                version: "v1",
                kinds: &[
                    ("Ingress", Some((1, 19))),
                    ("IngressList", Some((1, 19))),
                    ("IngressClass", Some((1, 19))),
                    ("IngressClassList", Some((1, 19))),
                    ("NetworkPolicy", Some((1, 7))),
                    ("NetworkPolicyList", Some((1, 19))),
                    ("IPAddress", Some((1, 33))),
                    ("IPAddressList", Some((1, 33))),
                    ("ServiceCIDR", Some((1, 33))),
                    ("ServiceCIDRList", Some((1, 33))),
                ],
            },
            VersionTable {
                version: "v1beta1",
                kinds: &[
                    ("Ingress", Some((1, 14))),
                    ("IngressList", Some((1, 14))),
                    ("IngressClass", Some((1, 18))),
                    ("IngressClassList", Some((1, 18))),
                    ("IPAddress", Some((1, 31))),
                    ("IPAddressList", Some((1, 31))),
                    ("ServiceCIDR", Some((1, 31))),
                    ("ServiceCIDRList", Some((1, 31))),
                ],
            },
        ],
    },
    GroupTable {
        group: "node.k8s.io",
        versions: &[
            VersionTable {
                version: "v1",
                kinds: &[
                    ("RuntimeClass", Some((1, 20))),
                    ("RuntimeClassList", Some((1, 20))),
                ],
            },
            VersionTable {
                version: "v1beta1",
                kinds: &[
                    ("RuntimeClass", Some((1, 13))),
                    ("RuntimeClassList", Some((1, 13))),
                ],
            },
        ],
    },
    GroupTable {
        group: "policy",
        versions: &[
            VersionTable {
                version: "v1",
                kinds: &[
                    ("PodDisruptionBudget", Some((1, 21))),
                    ("PodDisruptionBudgetList", Some((1, 21))),
                    ("Eviction", Some((1, 22))),
                ],
            },
            VersionTable {
                version: "v1beta1",
                kinds: &[
                    ("PodDisruptionBudget", Some((1, 5))),
                    ("PodDisruptionBudgetList", Some((1, 5))),
                    ("Eviction", Some((1, 5))),
                ],
            },
        ],
    },
    GroupTable {
        group: "rbac.authorization.k8s.io",
        versions: &[
            VersionTable {
                version: "v1",
                kinds: &[
                    ("Role", Some((1, 8))),
                    ("RoleBinding", Some((1, 8))),
                    ("RoleBindingList", Some((1, 8))),
                    ("RoleList", Some((1, 8))),
                    ("ClusterRole", Some((1, 8))),
                    ("ClusterRoleBinding", Some((1, 8))),
                    ("ClusterRoleBindingList", Some((1, 8))),
                    ("ClusterRoleList", Some((1, 8))),
                ],
            },
            VersionTable {
                version: "v1beta1",
                kinds: &[
                    ("Role", Some((1, 6))),
                    ("RoleBinding", Some((1, 6))),
                    ("RoleBindingList", Some((1, 6))),
                    ("RoleList", Some((1, 6))),
                    ("ClusterRole", Some((1, 6))),
                    ("ClusterRoleBinding", Some((1, 6))),
                    ("ClusterRoleBindingList", Some((1, 6))),
                    ("ClusterRoleList", Some((1, 6))),
                ],
            },
            VersionTable {
                version: "v1alpha1",
                kinds: &[
                    ("Role", None),
                    ("RoleBinding", None),
                    ("RoleBindingList", None),
                    ("RoleList", None),
                    ("ClusterRole", None),
                    ("ClusterRoleBinding", None),
                    ("ClusterRoleBindingList", None),
                    ("ClusterRoleList", None),
                ],
            },
        ],
    },
    GroupTable {
        group: "resource.k8s.io",
        versions: &[
            VersionTable {
                version: "v1",
                kinds: &[
                    ("DeviceClass", Some((1, 34))),
                    ("DeviceClassList", Some((1, 34))),
                    ("ResourceClaim", Some((1, 34))),
                    ("ResourceClaimList", Some((1, 34))),
                    ("ResourceClaimTemplate", Some((1, 34))),
                    ("ResourceClaimTemplateList", Some((1, 34))),
                    ("ResourceSlice", Some((1, 34))),
                    ("ResourceSliceList", Some((1, 34))),
                ],
            },
            VersionTable {
                version: "v1beta2",
                kinds: &[
                    ("DeviceClass", Some((1, 33))),
                    ("DeviceClassList", Some((1, 33))),
                    ("ResourceClaim", Some((1, 33))),
                    ("ResourceClaimList", Some((1, 33))),
                    ("ResourceClaimTemplate", Some((1, 33))),
                    ("ResourceClaimTemplateList", Some((1, 33))),
                    ("ResourceSlice", Some((1, 33))),
                    ("ResourceSliceList", Some((1, 33))),
                ],
            },
            VersionTable {
                version: "v1beta1",
                kinds: &[
                    ("DeviceClass", Some((1, 32))),
                    ("DeviceClassList", Some((1, 32))),
                    ("ResourceClaim", Some((1, 32))),
                    ("ResourceClaimList", Some((1, 32))),
                    ("ResourceClaimTemplate", Some((1, 32))),
                    ("ResourceClaimTemplateList", Some((1, 32))),
                    ("ResourceSlice", Some((1, 32))),
                    ("ResourceSliceList", Some((1, 32))),
                ],
            },
            VersionTable {
                version: "v1alpha3",
                kinds: &[
                    ("DeviceTaintRule", Some((1, 33))),
                    ("DeviceTaintRuleList", Some((1, 33))),
                ],
            },
        ],
    },
    GroupTable {
        group: "scheduling.k8s.io",
        versions: &[
            VersionTable {
                version: "v1",
                kinds: &[
                    ("PriorityClass", Some((1, 14))),
                    ("PriorityClassList", Some((1, 14))),
                ],
            },
            VersionTable {
                version: "v1beta1",
                kinds: &[
                    ("PriorityClass", Some((1, 11))),
                    ("PriorityClassList", Some((1, 11))),
                ],
            },
            VersionTable {
                version: "v1alpha1",
                kinds: &[
                    ("PriorityClass", None),
                    ("PriorityClassList", None),
                    ("Workload", None),
                    ("WorkloadList", None),
                ],
            },
        ],
    },
    GroupTable {
        group: "storage.k8s.io",
        versions: &[
            VersionTable {
                version: "v1",
                kinds: &[
                    ("StorageClass", Some((1, 6))),
                    ("StorageClassList", Some((1, 6))),
                    ("VolumeAttachment", Some((1, 13))),
                    ("VolumeAttachmentList", Some((1, 13))),
                    ("CSINode", Some((1, 17))),
                    ("CSINodeList", Some((1, 17))),
                    ("CSIDriver", Some((1, 18))),
                    ("CSIDriverList", Some((1, 18))),
                    ("CSIStorageCapacity", Some((1, 24))),
                    ("CSIStorageCapacityList", Some((1, 24))),
                    ("VolumeAttributesClass", Some((1, 34))),
                    ("VolumeAttributesClassList", Some((1, 34))),
                ],
            },
            VersionTable {
                version: "v1beta1",
                kinds: &[
                    ("StorageClass", Some((1, 4))),
                    ("StorageClassList", Some((1, 4))),
                    ("VolumeAttachment", Some((1, 10))),
                    ("VolumeAttachmentList", Some((1, 10))),
                    ("CSIDriver", Some((1, 14))),
                    ("CSIDriverList", Some((1, 14))),
                    ("CSINode", Some((1, 14))),
                    ("CSINodeList", Some((1, 14))),
                    ("CSIStorageCapacity", Some((1, 21))),
                    ("CSIStorageCapacityList", Some((1, 21))),
                    ("VolumeAttributesClass", Some((1, 31))),
                    ("VolumeAttributesClassList", Some((1, 31))),
                ],
            },
            VersionTable {
                version: "v1alpha1",
                kinds: &[
                    ("VolumeAttachment", Some((1, 9))),
                    ("VolumeAttachmentList", Some((1, 9))),
                    ("CSIStorageCapacity", Some((1, 19))),
                    ("CSIStorageCapacityList", Some((1, 19))),
                    ("VolumeAttributesClass", Some((1, 29))),
                    ("VolumeAttributesClassList", Some((1, 29))),
                ],
            },
        ],
    },
    GroupTable {
        group: "storagemigration.k8s.io",
        versions: &[VersionTable {
            version: "v1beta1",
            kinds: &[
                ("StorageVersionMigration", Some((1, 35))),
                ("StorageVersionMigrationList", Some((1, 35))),
            ],
        }],
    },
];
