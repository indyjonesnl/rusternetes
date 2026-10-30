//! Unit tests moved from `src/admission/caching_authorizer.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_common::auth::UserInfo;
    pub use rusternetes_common::authz::{
        AuthorizationOutcome, Authorizer, Decision, Opinion, RequestAttributes,
    };
    pub use rusternetes_common::resources::{NonResourceRule, ResourceRule};
    pub use rusternetes_common::{Error, Result};
    pub use std::collections::{BTreeMap, HashMap};
    pub use std::sync::{Arc, Mutex};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::caching_authorizer::*;
    use rusternetes_common::field_selector::FieldSelector;
    use rusternetes_common::label_selector::LabelSelector;
    use rusternetes_common::Error;
    use std::collections::VecDeque;

    struct Scripted {
        answers: Mutex<VecDeque<&'static str>>,
        calls: Mutex<usize>,
    }

    #[async_trait]
    impl Authorizer for Scripted {
        async fn authorize(&self, _: &RequestAttributes) -> Result<Decision> {
            *self.calls.lock().unwrap() += 1;
            let err = self
                .answers
                .lock()
                .unwrap()
                .pop_front()
                .expect("got more backend invocations than expected");
            Err(Error::Internal(err.to_string()))
        }
        async fn get_user_rules(
            &self,
            _: &UserInfo,
            _: &str,
        ) -> Result<(Vec<ResourceRule>, Vec<NonResourceRule>)> {
            Ok((vec![], vec![]))
        }
    }

    fn blank_user() -> UserInfo {
        UserInfo {
            username: String::new(),
            uid: String::new(),
            groups: vec![],
            extra: HashMap::new(),
        }
    }

    fn attrs(name: &str) -> RequestAttributes {
        RequestAttributes::new(blank_user(), "get", "pods").with_name(name)
    }

    fn user(groups: &[&str], extra: &[(&str, &[&str])]) -> RequestAttributes {
        let mut u = blank_user();
        u.groups = groups.iter().map(|s| s.to_string()).collect();
        u.extra = extra
            .iter()
            .map(|(k, v)| (k.to_string(), v.iter().map(|s| s.to_string()).collect()))
            .collect();
        RequestAttributes::new(u, "get", "pods")
    }

    /// Runs `calls` against a scripted backend and returns the error message
    /// seen per call and the backend invocation count.
    async fn run(calls: Vec<RequestAttributes>, backend: &[&'static str]) -> (Vec<String>, usize) {
        let inner = Arc::new(Scripted {
            answers: Mutex::new(backend.iter().copied().collect()),
            calls: Mutex::new(0),
        });
        let front = new_caching_authorizer(inner.clone());
        let mut out = vec![];
        for c in &calls {
            out.push(front.authorize(c).await.unwrap_err().to_string());
        }
        let n = *inner.calls.lock().unwrap();
        (out, n)
    }

    #[tokio::test]
    async fn hit() {
        let (out, n) = run(
            vec![attrs("test name"), attrs("test name")],
            &["test error"],
        )
        .await;
        assert_eq!(n, 1);
        assert!(out.iter().all(|e| e.contains("test error")), "{out:?}");
    }

    #[tokio::test]
    async fn hit_with_differently_ordered_groups() {
        let (_, n) = run(
            vec![user(&["a", "b", "c"], &[]), user(&["c", "b", "a"], &[])],
            &["test error"],
        )
        .await;
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn hit_with_differently_ordered_extra() {
        let (_, n) = run(
            vec![
                user(&[], &[("k", &["a", "b", "c"])]),
                user(&[], &[("k", &["c", "b", "a"])]),
            ],
            &["test error"],
        )
        .await;
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn miss_due_to_different_name() {
        let (out, n) = run(vec![attrs("alpha"), attrs("beta")], &["alpha", "beta"]).await;
        assert_eq!(n, 2);
        assert!(
            out[0].contains("alpha") && out[1].contains("beta"),
            "{out:?}"
        );
    }

    #[tokio::test]
    async fn miss_due_to_different_user() {
        let a = |n: &str| {
            let mut u = blank_user();
            u.username = n.into();
            RequestAttributes::new(u, "get", "pods")
        };
        let (_, n) = run(vec![a("alpha"), a("beta")], &["alpha", "beta"]).await;
        assert_eq!(n, 2);
    }

    fn with_field(mut a: RequestAttributes, sel: &str) -> RequestAttributes {
        a.field_selector_requirements = FieldSelector::parse(sel).unwrap().requirements().to_vec();
        a
    }
    fn with_label(mut a: RequestAttributes, sel: &str) -> RequestAttributes {
        a.label_selector_requirements = LabelSelector::parse(sel).unwrap().requirements().to_vec();
        a
    }

    #[tokio::test]
    async fn honor_good_field_selector() {
        let (out, n) = run(
            vec![
                with_field(attrs("test name"), "foo=bar"),
                attrs("test name"),
                with_field(attrs("test name"), "foo=bar"),
            ],
            &["test error", "test error 2"],
        )
        .await;
        assert_eq!(n, 2);
        assert!(out[2].contains("test error") && !out[2].contains("error 2"));
    }

    #[tokio::test]
    async fn ignore_malformed_field_selector_first() {
        let mut bad = attrs("test name");
        bad.field_selector_parsing_err = Some("malformed".into());
        let (_, n) = run(vec![bad, attrs("test name")], &["test error"]).await;
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn ignore_malformed_field_selector_second() {
        let mut bad = attrs("test name");
        bad.field_selector_parsing_err = Some("malformed".into());
        let (_, n) = run(vec![attrs("test name"), bad], &["test error"]).await;
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn honor_good_label_selector() {
        let (_, n) = run(
            vec![
                with_label(attrs("test name"), "foo=bar"),
                attrs("test name"),
                with_label(attrs("test name"), "foo=bar"),
                with_label(attrs("test name"), "diff=zero"),
            ],
            &["test error", "test error 2", "test error 3"],
        )
        .await;
        assert_eq!(n, 3);
    }

    #[tokio::test]
    async fn ignore_malformed_label_selector_both_orders() {
        let mut bad = attrs("test name");
        bad.label_selector_parsing_err = Some("malformed mess".into());
        let (_, n) = run(vec![bad.clone(), attrs("test name")], &["test error"]).await;
        assert_eq!(n, 1);
        let (_, n) = run(vec![attrs("test name"), bad], &["test error"]).await;
        assert_eq!(n, 1);
    }

    /// The opinion form is cached under the same key rules.
    #[tokio::test]
    async fn authorize_opinion_is_cached() {
        struct Op(Mutex<usize>);
        #[async_trait]
        impl Authorizer for Op {
            async fn authorize(&self, _: &RequestAttributes) -> Result<Decision> {
                Ok(Decision::Allow)
            }
            async fn authorize_opinion(&self, _: &RequestAttributes) -> Result<Opinion> {
                *self.0.lock().unwrap() += 1;
                Ok(Opinion::Deny("no".into()))
            }
            async fn get_user_rules(
                &self,
                _: &UserInfo,
                _: &str,
            ) -> Result<(Vec<ResourceRule>, Vec<NonResourceRule>)> {
                Ok((vec![], vec![]))
            }
        }
        let inner = Arc::new(Op(Mutex::new(0)));
        let front = new_caching_authorizer(inner.clone());
        for _ in 0..2 {
            assert_eq!(
                front.authorize_opinion(&attrs("x")).await.unwrap(),
                Opinion::Deny("no".into())
            );
        }
        assert_eq!(*inner.0.lock().unwrap(), 1);
    }
}
