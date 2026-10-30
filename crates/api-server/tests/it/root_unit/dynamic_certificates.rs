//! Unit tests moved from `src/dynamic_certificates.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Context, Result};
    pub use axum_server::tls_rustls::RustlsConfig;
    pub use futures::StreamExt;
    pub use inotify::{EventMask, Inotify, WatchMask};
    pub use rusternetes_common::tls::{client_cert_verifier_for_ca_bundle, TlsConfig};
    pub use rustls::server::danger::ClientCertVerifier;
    pub use std::sync::atomic::{AtomicU32, Ordering};
    pub use std::sync::{Arc, Mutex, RwLock};
    pub use std::time::Duration;
    pub use tokio::sync::Notify;
    pub use tokio::task::JoinHandle;
    pub use tracing::{error, info};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::dynamic_certificates::*;
    use std::sync::atomic::AtomicUsize;

    fn ca_pem(cn: &str) -> String {
        let mut params = rcgen::CertificateParams::default();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        let key = rcgen::KeyPair::generate().unwrap();
        params.self_signed(&key).unwrap().pem()
    }

    fn tmp(name: &str, content: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("rn-dynca-{name}-{}", uuid::Uuid::new_v4()));
        std::fs::write(&p, content).unwrap();
        p
    }

    #[derive(Default)]
    struct Counter(AtomicUsize);
    impl Listener for Counter {
        fn enqueue(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn missing_filename_is_an_error() {
        // NewDynamicCAContentFromFile: len(filename) == 0.
        assert!(DynamicFileCaContent::new("x", "").is_err());
    }

    #[test]
    fn name_is_purpose_and_filename() {
        let p = tmp("n", &ca_pem("a"));
        let c = DynamicFileCaContent::new("client-ca-bundle", p.to_str().unwrap()).unwrap();
        assert_eq!(
            c.name(),
            format!("client-ca-bundle::{}", p.to_str().unwrap())
        );
    }

    #[test]
    fn empty_or_certless_file_fails_to_load() {
        // loadCABundle: "missing content for CA bundle"; NewPoolFromBytes: no certs.
        let empty = tmp("e", "");
        let err = DynamicFileCaContent::new("x", empty.to_str().unwrap())
            .err()
            .unwrap();
        assert!(
            format!("{err:#}").contains("missing content for CA bundle"),
            "{err:#}"
        );
        let junk = tmp("j", "not a certificate");
        assert!(DynamicFileCaContent::new("x", junk.to_str().unwrap()).is_err());
        assert!(DynamicFileCaContent::new("x", "/nonexistent/ca.crt").is_err());
    }

    #[test]
    fn reload_notifies_listeners_only_when_content_changes() {
        // loadCABundle: hasCAChanged gates both the swap and the listeners.
        let a = ca_pem("a");
        let p = tmp("l", &a);
        let c = DynamicFileCaContent::new("x", p.to_str().unwrap()).unwrap();
        let l = Arc::new(Counter::default());
        c.add_listener(l.clone());

        c.run_once().unwrap();
        assert_eq!(l.0.load(Ordering::SeqCst), 0, "unchanged content");

        let b = ca_pem("b");
        std::fs::write(&p, &b).unwrap();
        c.run_once().unwrap();
        assert_eq!(l.0.load(Ordering::SeqCst), 1);
        assert_eq!(c.current_ca_bundle_content(), b.as_bytes());
    }

    #[test]
    fn failed_reload_keeps_the_last_good_bundle() {
        let a = ca_pem("a");
        let p = tmp("k", &a);
        let c = DynamicFileCaContent::new("x", p.to_str().unwrap()).unwrap();
        std::fs::write(&p, "").unwrap();
        assert!(c.run_once().is_err());
        std::fs::write(&p, "garbage").unwrap();
        assert!(c.run_once().is_err());
        assert_eq!(c.current_ca_bundle_content(), a.as_bytes());
        assert!(c.verifier().is_some());
    }

    #[test]
    fn union_joins_trimmed_bundles_with_newline_and_names_with_comma() {
        // unionCAContent.CurrentCABundleContent / Name.
        let (a, b) = (ca_pem("a"), ca_pem("b"));
        let pa = tmp("ua", &format!("{a}\n\n"));
        let pb = tmp("ub", &b);
        let ca = DynamicFileCaContent::new("one", pa.to_str().unwrap()).unwrap();
        let cb = DynamicFileCaContent::new("two", pb.to_str().unwrap()).unwrap();
        let u = UnionCaContent::new(vec![ca.clone(), cb.clone()]);
        assert_eq!(
            u.current_ca_bundle_content(),
            format!("{}\n{}", a.trim(), b.trim()).into_bytes()
        );
        assert_eq!(u.name(), format!("{},{}", ca.name(), cb.name()));
        assert!(u.verifier().is_some());
        assert!(UnionCaContent::new(vec![]).verifier().is_none());
    }

    #[test]
    fn union_listener_fires_when_any_member_changes_and_run_once_aggregates() {
        let pa = tmp("fa", &ca_pem("a"));
        let pb = tmp("fb", &ca_pem("b"));
        let u = UnionCaContent::new(vec![
            DynamicFileCaContent::new("one", pa.to_str().unwrap()).unwrap(),
            DynamicFileCaContent::new("two", pb.to_str().unwrap()).unwrap(),
        ]);
        let l = Arc::new(Counter::default());
        u.add_listener(l.clone());
        std::fs::write(&pb, ca_pem("b2")).unwrap();
        u.run_once().unwrap();
        assert_eq!(l.0.load(Ordering::SeqCst), 1);
        std::fs::write(&pa, "").unwrap();
        std::fs::write(&pb, "").unwrap();
        let err = u.run_once().unwrap_err().to_string();
        assert_eq!(err.matches("missing content").count(), 2, "{err}");
    }

    #[test]
    fn rate_limiter_is_exponential_from_5ms_and_forget_resets() {
        // ItemExponentialFailureRateLimiter(5ms, 1000s).
        let q = SingleItemQueue::new();
        assert_eq!(q.when(), Duration::from_millis(5));
        assert_eq!(q.when(), Duration::from_millis(10));
        assert_eq!(q.when(), Duration::from_millis(20));
        q.forget();
        assert_eq!(q.when(), Duration::from_millis(5));
        for _ in 0..60 {
            q.when();
        }
        assert_eq!(q.when(), Duration::from_secs(1000));
    }
}
