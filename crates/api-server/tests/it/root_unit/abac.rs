//! Unit tests moved from `src/abac.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::auth::UserInfo;
    pub use rusternetes_common::authz::{Authorizer, Decision, Opinion, RequestAttributes};
    pub use rusternetes_common::resources::{NonResourceRule, ResourceRule};
    pub use serde::Deserialize;
    pub use std::path::Path;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::abac::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const ALL_AUTHENTICATED: &str = "system:authenticated";

    /// `newWithContents` (abac_test.go, end of file).
    fn new_with_contents(contents: &str) -> Result<PolicyList, PolicyLoadError> {
        static N: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "abac_test_{}_{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::write(&path, contents).unwrap();
        let r = PolicyList::new_from_file(&path);
        let _ = std::fs::remove_file(&path);
        r
    }

    fn user(name: &str, groups: &[&str]) -> UserInfo {
        UserInfo {
            username: name.to_string(),
            uid: String::new(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            extra: Default::default(),
        }
    }

    /// The Go tests set `ResourceRequest: len(ns) > 0 || len(resource) > 0`.
    fn attrs(
        u: &UserInfo,
        verb: &str,
        resource: &str,
        ns: &str,
        group: &str,
        path: &str,
    ) -> RequestAttributes {
        let mut a = RequestAttributes::new(u.clone(), verb, resource);
        a.api_group = group.to_string();
        if !ns.is_empty() {
            a.namespace = Some(ns.to_string());
        }
        if ns.is_empty() && resource.is_empty() {
            a.is_non_resource_request = true;
            a.path = Some(path.to_string());
        }
        a
    }

    async fn decide(pl: &PolicyList, a: &RequestAttributes) -> bool {
        matches!(pl.authorize(a).await.unwrap(), Decision::Allow)
    }

    type Case<'a> = (
        &'a UserInfo,
        &'a str,
        &'a str,
        &'a str,
        &'a str,
        &'a str,
        bool,
    );

    async fn run_cases(pl: &PolicyList, cases: Vec<Case<'_>>) {
        for (i, (u, verb, res, ns, grp, path, want)) in cases.into_iter().enumerate() {
            let a = attrs(u, verb, res, ns, grp, path);
            assert_eq!(
                decide(pl, &a).await,
                want,
                "case {i}: {} {verb} {res} ns={ns} path={path}",
                u.username
            );
        }
    }

    // TestEmptyFile
    #[test]
    fn empty_file() {
        assert!(new_with_contents("").unwrap().0.is_empty());
    }

    // TestOneLineFileNoNewLine
    #[test]
    fn one_line_file_no_new_line() {
        let pl = new_with_contents(
            r#"{"user":"scheduler",  "readonly": true, "resource": "pods", "namespace":"ns1"}"#,
        )
        .unwrap();
        assert_eq!(pl.0.len(), 1);
    }

    // TestTwoLineFile
    #[test]
    fn two_line_file() {
        let pl = new_with_contents(
            "{\"user\":\"scheduler\",  \"readonly\": true, \"resource\": \"pods\"}\n{\"user\":\"scheduler\",  \"readonly\": true, \"resource\": \"services\"}\n",
        )
        .unwrap();
        assert_eq!(pl.0.len(), 2);
    }

    // TestExampleFile (the file upstream points users at, copied verbatim).
    #[test]
    fn example_file() {
        let pl =
            new_with_contents(include_str!("../../../src/abac_example_policy_file.jsonl")).unwrap();
        assert_eq!(pl.0.len(), 11);
    }

    // abac.go:80-84 skips blank and `#` lines; :86-109 errors carry the line.
    #[test]
    fn bad_line_reports_path_and_line_number() {
        let err = new_with_contents("# c\n\n{\"user\":\"a\"}\nnot json\n").unwrap_err();
        let msg = err.to_string();
        assert!(msg.starts_with("error reading policy file "), "{msg}");
        assert!(msg.contains(", line 4: not json: "), "{msg}");
    }

    #[test]
    fn missing_file_is_an_error() {
        assert!(
            PolicyList::new_from_file(std::path::Path::new("/nonexistent/abac.jsonl")).is_err()
        );
    }

    // TestAuthorizeV0 (abac_test.go:66-169)
    #[tokio::test]
    async fn authorize_v0() {
        let pl = new_with_contents(
            r#"{                    "readonly": true, "resource": "events"   }
{"user":"scheduler", "readonly": true, "resource": "pods"     }
{"user":"scheduler",                   "resource": "bindings" }
{"user":"kubelet",   "readonly": true, "resource": "bindings" }
{"user":"kubelet",                     "resource": "events"   }
{"user":"alice",                                              "namespace": "projectCaribou"}
{"user":"bob",       "readonly": true,                        "namespace": "projectCaribou"}
"#,
        )
        .unwrap();
        let auth = [ALL_AUTHENTICATED];
        let sched = user("scheduler", &auth);
        let alice = user("alice", &auth);
        let chuck = user("chuck", &auth);
        run_cases(
            &pl,
            vec![
                (&sched, "list", "pods", "ns1", "", "", true),
                (&sched, "list", "pods", "", "", "", true),
                (&sched, "create", "pods", "ns1", "", "", false),
                (&sched, "create", "pods", "", "", "", false),
                (&sched, "get", "bindings", "ns1", "", "", true),
                (&sched, "get", "bindings", "", "", "", true),
                (&alice, "get", "pods", "projectCaribou", "", "", true),
                (&alice, "get", "widgets", "projectCaribou", "", "", true),
                (&alice, "get", "", "projectCaribou", "", "", true),
                (&alice, "update", "pods", "projectCaribou", "", "", true),
                (&alice, "update", "widgets", "projectCaribou", "", "", true),
                (&alice, "update", "", "projectCaribou", "", "", true),
                (&alice, "update", "foo", "projectCaribou", "bar", "", true),
                (&alice, "get", "pods", "ns1", "", "", false),
                (&alice, "get", "widgets", "ns1", "", "", false),
                (&alice, "get", "", "ns1", "", "", false),
                (&chuck, "get", "events", "ns1", "", "", true),
                (&chuck, "get", "events", "", "", "", true),
                (&chuck, "update", "events", "ns1", "", "", false),
                (&chuck, "get", "pods", "ns1", "", "", false),
                (&chuck, "get", "floop", "ns1", "", "", false),
                (&chuck, "get", "", "", "", "/", false),
            ],
        )
        .await;
    }

    // TestAuthorizeV1beta1 (abac_test.go:344-462)
    #[tokio::test]
    async fn authorize_v1beta1() {
        let l = |spec: &str| {
            format!(
                r#"{{"apiVersion":"abac.authorization.kubernetes.io/v1beta1","kind":"Policy","spec":{spec}}}"#
            )
        };
        let contents = [
            "\t\t # Comment line, after a blank line".to_string(),
            l(r#"{"user":"*","readonly":true,"nonResourcePath":"/api"}"#),
            l(r#"{"user":"*","nonResourcePath":"/custom"}"#),
            l(r#"{"user":"*","nonResourcePath":"/root/*"}"#),
            l(r#"{"user":"noresource","nonResourcePath":"*"}"#),
            l(r#"{"user":"*","readonly":true,"resource":"events","namespace":"*"}"#),
            l(r#"{"user":"scheduler","readonly":true,"resource":"pods","namespace":"*"}"#),
            l(r#"{"user":"scheduler","resource":"bindings","namespace":"*"}"#),
            l(r#"{"user":"kubelet","readonly":true,"resource":"bindings","namespace":"*"}"#),
            l(r#"{"user":"kubelet","resource":"events","namespace":"*"}"#),
            l(r#"{"user":"alice","resource":"*","namespace":"projectCaribou"}"#),
            l(r#"{"user":"bob","readonly":true,"resource":"*","namespace":"projectCaribou"}"#),
            l(r#"{"user":"debbie","resource":"pods","namespace":"projectCaribou"}"#),
            l(
                r#"{"user":"apigroupuser","resource":"*","namespace":"projectAnyGroup","apiGroup":"*"}"#,
            ),
            l(
                r#"{"user":"apigroupuser","resource":"*","namespace":"projectEmptyGroup","apiGroup":""}"#,
            ),
            format!(
                "\t\t {}",
                l(
                    r#"{"user":"apigroupuser","resource":"*","namespace":"projectXGroup","apiGroup":"x"}"#
                )
            ),
        ]
        .join("\n");
        let pl = new_with_contents(&contents).unwrap();
        let auth = [ALL_AUTHENTICATED];
        let sched = user("scheduler", &auth);
        let alice = user("alice", &auth);
        let chuck = user("chuck", &auth);
        let debbie = user("debbie", &auth);
        let nores = user("noresource", &auth);
        let apig = user("apigroupuser", &auth);
        run_cases(
            &pl,
            vec![
                (&sched, "list", "pods", "ns1", "", "", true),
                (&sched, "list", "pods", "", "", "", true),
                (&sched, "create", "pods", "ns1", "", "", false),
                (&sched, "create", "pods", "", "", "", false),
                (&sched, "get", "bindings", "ns1", "", "", true),
                (&sched, "get", "bindings", "", "", "", true),
                (&alice, "get", "pods", "projectCaribou", "", "", true),
                (&alice, "get", "widgets", "projectCaribou", "", "", true),
                (&alice, "get", "", "projectCaribou", "", "", true),
                (&alice, "update", "pods", "projectCaribou", "", "", true),
                (&alice, "update", "widgets", "projectCaribou", "", "", true),
                (&alice, "update", "", "projectCaribou", "", "", true),
                (&alice, "get", "pods", "ns1", "", "", false),
                (&alice, "get", "widgets", "ns1", "", "", false),
                (&alice, "get", "", "ns1", "", "", false),
                (&debbie, "update", "pods", "projectCaribou", "", "", true),
                (&chuck, "get", "events", "ns1", "", "", true),
                (&chuck, "get", "events", "", "", "", true),
                (&chuck, "update", "events", "ns1", "", "", false),
                (&chuck, "get", "pods", "ns1", "", "", false),
                (&chuck, "get", "floop", "ns1", "", "", false),
                (&chuck, "get", "", "", "", "/", false),
                (&chuck, "get", "", "", "", "/api", true),
                (&chuck, "create", "", "", "", "/api", false),
                (&chuck, "update", "", "", "", "/custom", true),
                (&chuck, "get", "", "", "", "/root", false),
                (&chuck, "get", "", "", "", "/root/", true),
                (&chuck, "get", "", "", "", "/root/test/1/2/3", true),
                (&nores, "get", "", "", "", "", true),
                (&nores, "get", "", "", "", "/", true),
                (&nores, "get", "", "", "", "/foo/bar/baz", true),
                (&nores, "get", "", "bar", "", "/", false),
                (&nores, "get", "foo", "bar", "", "/foo/bar/baz", false),
                (&apig, "get", "foo", "projectAnyGroup", "x", "", true),
                (&apig, "get", "foo", "projectEmptyGroup", "x", "", false),
                (&apig, "get", "foo", "projectXGroup", "x", "", true),
            ],
        )
        .await;
    }

    /// An explicit `v0` apiVersion is a registered version (v0/register.go:30)
    /// and converts like an unversioned line; an unregistered version or a
    /// missing kind falls back to v0 (abac.go:88).
    #[tokio::test]
    async fn explicit_v0_and_unregistered_decode_as_v0() {
        let pl = new_with_contents(
            "{\"apiVersion\":\"abac.authorization.kubernetes.io/v0\",\"kind\":\"Policy\",\"user\":\"alice\"}\n{\"apiVersion\":\"example.com/v9\",\"kind\":\"Policy\",\"user\":\"bob\"}\n{\"kind\":\"Policy\",\"user\":\"carol\"}\n",
        )
        .unwrap();
        let auth = [ALL_AUTHENTICATED];
        for name in ["alice", "bob", "carol"] {
            let a = attrs(&user(name, &auth), "get", "pods", "ns", "g", "");
            assert!(decide(&pl, &a).await, "{name}");
        }
    }

    // TestRulesFor (abac_test.go:171-342)
    #[tokio::test]
    async fn rules_for() {
        let pl = new_with_contents(
            r#"
{                    "readonly": true, "resource": "events"   }
{"user":"scheduler", "readonly": true, "resource": "pods"     }
{"user":"scheduler",                   "resource": "bindings" }
{"user":"kubelet",   "readonly": true, "resource": "pods"     }
{"user":"kubelet",                     "resource": "events"   }
{"user":"alice",                                              "namespace": "projectCaribou"}
{"user":"bob",       "readonly": true,                        "namespace": "projectCaribou"}
{"user":"bob",       "readonly": true,                                                     "nonResourcePath": "*"}
{"group":"a",                          "resource": "bindings" }
{"group":"b",        "readonly": true,                                                     "nonResourcePath": "*"}
"#,
        )
        .unwrap();
        let ro: Vec<String> = ["get", "list", "watch"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let all: Vec<String> = vec!["*".to_string()];
        type Res = Vec<(Vec<String>, Vec<String>, Vec<String>)>;
        type NonRes = Vec<(Vec<String>, Vec<String>)>;
        let r =
            |v: &Vec<String>, res: &str| (v.clone(), vec!["*".to_string()], vec![res.to_string()]);
        let nr = |v: &Vec<String>| (v.clone(), vec!["*".to_string()]);
        let auth = [ALL_AUTHENTICATED];
        let cases: Vec<(UserInfo, &str, Res, NonRes)> = vec![
            (
                user("scheduler", &auth),
                "ns1",
                vec![r(&ro, "events"), r(&ro, "pods"), r(&all, "bindings")],
                vec![],
            ),
            (
                user("kubelet", &["a", "b"]),
                "ns1",
                vec![
                    r(&ro, "pods"),
                    r(&all, "events"),
                    r(&all, "bindings"),
                    r(&ro, "*"),
                ],
                vec![nr(&ro)],
            ),
            (
                user("alice", &auth),
                "projectCaribou",
                vec![r(&ro, "events"), r(&all, "*")],
                vec![],
            ),
            (
                user("bob", &auth),
                "projectCaribou",
                vec![r(&ro, "events"), r(&ro, "*"), r(&ro, "*")],
                vec![nr(&ro)],
            ),
            (
                user("chuck", &["a", "b"]),
                "ns1",
                vec![r(&all, "bindings"), r(&ro, "*")],
                vec![nr(&ro)],
            ),
        ];
        for (i, (u, ns, want_r, want_nr)) in cases.into_iter().enumerate() {
            let (rr, nrr) = pl.get_user_rules(&u, ns).await.unwrap();
            let got_r: Res = rr
                .into_iter()
                .map(|x| {
                    (
                        x.verbs,
                        x.api_groups.unwrap_or_default(),
                        x.resources.unwrap_or_default(),
                    )
                })
                .collect();
            let got_nr: NonRes = nrr
                .into_iter()
                .map(|x| (x.verbs, x.non_resource_urls.unwrap_or_default()))
                .collect();
            assert_eq!(got_r, want_r, "case {i} resource rules");
            assert_eq!(got_nr, want_nr, "case {i} non-resource rules");
        }
    }

    fn v0(user: &str, group: &str) -> Policy {
        v0_to_policy(V0Policy {
            user: user.into(),
            group: group.into(),
            ..Default::default()
        })
    }

    fn v1(user: &str, group: &str) -> Policy {
        v1beta1_to_policy(V1beta1Policy {
            spec: V1beta1PolicySpec {
                user: user.into(),
                group: group.into(),
                ..Default::default()
            },
        })
    }

    // TestSubjectMatches (abac_test.go:464-834), representative rows of each
    // v0/v1 group.
    #[test]
    fn subject_matches_table() {
        let anon = user("system:anonymous", &["system:unauthenticated"]);
        let foo = user("Foo", &[ALL_AUTHENTICATED]);
        let foo_groups = user("Foo", &["a", "b", ALL_AUTHENTICATED]);
        let cases: Vec<(&str, &UserInfo, Policy, bool)> = vec![
            ("v0 empty, unauthed", &anon, v0("", ""), false),
            ("v0 * user, unauthed", &anon, v0("*", ""), false),
            ("v0 * group, unauthed", &anon, v0("", "*"), false),
            ("v0 empty, authed", &foo, v0("", ""), true),
            ("v0 empty, authed w/ groups", &foo_groups, v0("", ""), true),
            ("v0 user, unauthed", &anon, v0("Foo", ""), false),
            ("v0 user case-sensitive", &foo, v0("foo", ""), false),
            ("v0 user match", &foo, v0("Foo", ""), true),
            ("v0 group match", &foo_groups, v0("", "a"), true),
            ("v0 group case-sensitive", &foo_groups, v0("", "A"), false),
            (
                "v0 user+group wrong user",
                &foo_groups,
                v0("Bar", "a"),
                false,
            ),
            (
                "v0 user+group wrong group",
                &foo_groups,
                v0("Foo", "c"),
                false,
            ),
            ("v0 user+group match", &foo_groups, v0("Foo", "a"), true),
            ("v1 empty, authed", &foo, v1("", ""), false),
            ("v1 empty, authed w/ groups", &foo_groups, v1("", ""), false),
            ("v1 * user, unauthed", &anon, v1("*", ""), false),
            ("v1 * group, unauthed", &anon, v1("", "*"), false),
            ("v1 * user, authed", &foo, v1("*", ""), true),
            ("v1 user match", &foo, v1("Foo", ""), true),
            ("v1 user substring", &foo, v1("Fo", ""), false),
            ("v1 group match", &foo_groups, v1("", "b"), true),
            (
                "v1 user+group wrong user",
                &foo_groups,
                v1("Bar", "a"),
                false,
            ),
            ("v1 user+group match", &foo_groups, v1("Foo", "a"), true),
        ];
        for (name, u, p, want) in cases {
            assert_eq!(subject_matches(&p, u), want, "{name}");
        }
    }

    // TestPolicy (abac_test.go:836-1253), representative rows.
    #[test]
    fn policy_matches_table() {
        let foo = user("foo", &[ALL_AUTHENTICATED]);
        let mk = |verb: &str, res: &str, ns: &str, path: Option<&str>| {
            let mut a = RequestAttributes::new(foo.clone(), verb, res);
            if !ns.is_empty() {
                a.namespace = Some(ns.to_string());
            }
            if let Some(p) = path {
                a.is_non_resource_request = true;
                a.path = Some(p.to_string());
            }
            a
        };
        let p = v0_to_policy(V0Policy {
            readonly: true,
            ..Default::default()
        });
        assert!(
            !matches(&p, &mk("create", "", "", None)),
            "v0 read-only mismatch"
        );
        assert!(matches(&p, &mk("get", "", "", None)), "v0 read-only match");
        let p = v0_to_policy(V0Policy {
            user: "foo".into(),
            ..Default::default()
        });
        assert!(matches(&p, &mk("get", "x", "y", None)), "v0 user match");
        let bar = user("bar", &[ALL_AUTHENTICATED]);
        assert!(
            !matches(
                &p,
                &RequestAttributes::new(bar, "get", "x").with_namespace("y")
            ),
            "v0 user mismatch"
        );
        let p = v0_to_policy(V0Policy {
            resource: "foo".into(),
            ..Default::default()
        });
        assert!(
            !matches(&p, &mk("get", "bar", "y", None)),
            "v0 resource mismatch"
        );
        assert!(
            matches(&p, &mk("get", "foo", "y", None)),
            "v0 resource match"
        );
        let p = v1beta1_to_policy(V1beta1Policy {
            spec: V1beta1PolicySpec {
                user: "foo".into(),
                non_resource_path: "/foo/*".into(),
                ..Default::default()
            },
        });
        assert!(
            matches(&p, &mk("get", "", "", Some("/foo/bar"))),
            "v1 subpath match"
        );
        assert!(
            !matches(&p, &mk("get", "", "", Some("/bar/foo"))),
            "v1 subpath mismatch"
        );
        let p = v1beta1_to_policy(V1beta1Policy {
            spec: V1beta1PolicySpec {
                user: "foo".into(),
                namespace: "ns".into(),
                resource: "*".into(),
                api_group: "*".into(),
                ..Default::default()
            },
        });
        assert!(
            !matches(&p, &mk("get", "pods", "other", None)),
            "v1 ns mismatch"
        );
        assert!(matches(&p, &mk("get", "pods", "ns", None)), "v1 ns match");
        let p = v1beta1_to_policy(V1beta1Policy {
            spec: V1beta1PolicySpec {
                user: "foo".into(),
                non_resource_path: "*".into(),
                ..Default::default()
            },
        });
        assert!(
            !matches(&p, &mk("get", "pods", "ns", None)),
            "non-resource policy never matches a resource request"
        );
    }
}
