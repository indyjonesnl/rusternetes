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
mod cas_retry_create_container_error_test;
mod cas_retry_heartbeat_test;
mod cas_retry_init_container_test;
mod cas_retry_pod_status_test;
mod cni_integration_test;
mod cni_only_networking;
mod conformance_node_container_restart_policy;
mod conformance_node_ephemeral_containers;
mod conformance_node_exec_logs_downward;
mod conformance_node_image_volume;
mod conformance_node_pod_admission;
mod conformance_node_pod_level_resources;
mod conformance_node_pod_lifecycle;
mod conformance_node_pod_resize;
mod conformance_node_privileged;
mod conformance_node_probes_init_containers;
mod conformance_node_runtime_lifecycle;
mod conformance_node_runtimeclass;
mod conformance_node_runtimeclass_extended;
mod conformance_node_secrets;
mod conformance_node_security_context;
mod conformance_node_termination_message_non_root;
mod conformance_node_variable_expansion;
mod conformance_storage_configmap_secret_projected;
mod conformance_storage_downwardapi_secrets_namespace;
mod conformance_storage_emptydir_hostpath;
mod coverage_qos;
mod cri_translate_e2e;
mod ephemeral_containers_test;
mod etc_hosts_hostaliases_test;
mod eviction_test;
mod hosts_file_test;
mod init_container_restart_test;
mod init_containers_test;
mod kubelet_lifecycle_test;
mod node_registration_preserves_spec;
mod pods_endpoint_test;
mod runtime_prestop_exit_test;
mod sandbox_lookup_isolation;
mod server_extended_test;
mod sidecar_containers_test;
mod static_pod_defaults_test;
mod static_pods_test;
mod status_idempotency_test;
