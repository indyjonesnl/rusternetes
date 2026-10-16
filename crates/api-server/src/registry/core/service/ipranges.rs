//! `--service-cluster-ip-range`. The implementation moved to
//! `rusternetes_common::service_ip_ranges` so the controller-manager's Service
//! controller parses the same flag the same way (#2446 item 3); re-exported here
//! so existing paths keep working.

pub use rusternetes_common::service_ip_ranges::*;
