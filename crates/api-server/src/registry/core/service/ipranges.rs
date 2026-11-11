//! `--service-cluster-ip-range`. The implementation moved to
//! `k3rs_common::service_ip_ranges` so the controller-manager's Service
//! controller parses the same flag the same way (#2446 item 3); re-exported here
//! so existing paths keep working.

pub use k3rs_common::service_ip_ranges::*;
