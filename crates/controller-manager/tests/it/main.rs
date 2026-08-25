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
mod apiservice_available_test;
mod availability_status_test;
mod conformance_apps_deployment_replicaset;
mod conformance_apps_job_cronjob;
mod conformance_apps_rc_daemonset_pdb;
mod conformance_apps_statefulset_daemonset;
mod conformance_network_services;
mod conformance_storage_pv_csi;
mod cronjob_controller_test;
mod csr_controller_test;
mod daemonset_controller_revision_test;
mod daemonset_controller_test;
mod daemonset_extended_test;
mod daemonset_revision_match_test;
mod deployment_controller_test;
mod deployment_extended_test;
mod deployment_idempotency_test;
mod deployment_proportional_test;
mod deployment_scaling_test;
mod dynamic_provisioner_test;
mod endpoints_controller_test;
mod endpointslice_controller_test;
mod endpointslice_idempotency_test;
mod garbage_collector_idempotency_test;
mod garbage_collector_test;
mod hpa_controller_test;
mod ingress_controller_extended_test;
mod integration_endpoints_terminal_pods;
mod integration_gc_cascading;
mod integration_node_taint_evictions;
mod integration_pdb_disruption;
mod integration_quota_enforcement;
mod job_completion_modes_test;
mod job_controller_test;
mod job_extended_test;
mod job_success_policy_test;
mod loadbalancer_status_lifecycle_test;
mod namespace_controller_test;
mod networkpolicy_controller_test;
mod node_controller_test;
mod pdb_controller_test;
mod priorityclass_controller_test;
mod pv_binder_test;
mod pv_controller_extended_test;
mod pvc_controller_test;
mod replicaset_controller_test;
mod resource_quota_idempotency_test;
mod resource_quota_spec_clobber_test;
mod resource_quota_test;
mod resource_quota_usage_recompute_test;
mod service_controller_test;
mod service_lb_endpoints_test;
mod service_lb_extended_test;
mod serviceaccount_controller_test;
mod servicecidr_controller_test;
mod statefulset_controller_test;
mod statefulset_extended_test;
mod statefulset_status_correctness_test;
mod storageclass_controller_test;
mod ttl_controller_test;
mod volume_attachment_test;
mod volume_expansion_test;
mod volume_snapshot_controller_test;
mod vpa_controller_test;
