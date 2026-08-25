//! Single integration-test binary for this crate.
//!
//! Cargo compiles and links every `tests/*.rs` as its own binary, each
//! one statically linking this crate plus the whole dependency graph. At
//! this crate's test count that made a one-line edit to `src/` pay for
//! that many link steps before a single test could run. Keeping the tests
//! as modules of one target collapses those into one link.
//!
//! Add a new test file as `tests/it/<name>.rs` plus a `mod <name>;` line
//! below. A stray `tests/<name>.rs` still compiles -- it just silently
//! reintroduces a separate binary, which is what this layout avoids.
mod authz_rbac_node;
mod crd_jsonschemaprops_roundtrip_test;
mod decode_parity;
mod fuzz_roundtrip_jsonproto_test;
mod ipaddress_test;
mod k8s_openapi_parity;
mod list_empty_items_invariant_test;
mod managed_fields_roundtrip_test;
mod null_field_deserialization_test;
mod qos_pod_class;
mod quantity_decode_test;
mod quantity_normalization_test;
mod roundtrip_apps_v1;
mod roundtrip_batch;
mod roundtrip_core_v1;
mod roundtrip_networking;
mod roundtrip_rbac_storage;
mod secret_data_roundtrip_test;
mod servicecidr_test;
mod validation_configmap;
mod validation_controllerrevision;
mod validation_cronjob;
mod validation_csidriver;
mod validation_csinode;
mod validation_csistoragecapacity;
mod validation_csistoragecapacity_update;
mod validation_csr;
mod validation_csr_conditions;
mod validation_daemonset;
mod validation_deployment;
mod validation_endpoints;
mod validation_endpointslice;
mod validation_endpointslice_update;
mod validation_flowschema;
mod validation_hpa;
mod validation_ingress;
mod validation_ingressclass;
mod validation_ipaddress;
mod validation_job;
mod validation_lease;
mod validation_limitrange;
mod validation_metav1;
mod validation_namespace;
mod validation_networkpolicy;
mod validation_node;
mod validation_node_update;
mod validation_objectmeta;
mod validation_pdb;
mod validation_persistentvolume;
mod validation_pod_create;
mod validation_podtemplate;
mod validation_priorityclass;
mod validation_prioritylevelconfiguration;
mod validation_pvc;
mod validation_pvc_update;
mod validation_rbac;
mod validation_replicaset;
mod validation_replicationcontroller;
mod validation_resourcequota;
mod validation_resourcequota_update;
mod validation_runtimeclass;
mod validation_secret;
mod validation_service;
mod validation_servicecidr;
mod validation_statefulset;
mod validation_statefulset_update;
mod validation_storageclass;
mod validation_storageclass_update;
mod validation_vac_update;
mod validation_volumeattachment;
mod validation_volumeattributesclass;
mod validation_webhookconfiguration;
