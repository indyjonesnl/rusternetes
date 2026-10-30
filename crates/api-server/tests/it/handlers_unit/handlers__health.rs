//! Unit tests moved from `src/handlers/health.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{extract::State, http::StatusCode, Json};
    pub use rusternetes_api_server::state::ApiServerState;
    pub use serde::{Deserialize, Serialize};
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::health::*;
    use rusternetes_api_server::post_start_hooks::PostStartHooks;
    use std::sync::atomic::Ordering;

    #[test]
    fn poststarthook_check_fails_until_finished() {
        let reg = PostStartHooks::new();
        let n = "scheduling/bootstrap-system-priority-classes";
        let done = reg.register(n);
        let (code, body) = post_start_hook_response(&reg, n);
        assert_eq!(code, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.contains("not finished"));
        done.store(true, Ordering::SeqCst);
        assert_eq!(
            post_start_hook_response(&reg, n),
            (StatusCode::OK, "ok".into())
        );
        assert_eq!(
            post_start_hook_response(&reg, "missing").0,
            StatusCode::NOT_FOUND
        );
    }
}
