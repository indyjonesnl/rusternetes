//! Ports of `pkg/credentialprovider/keyring_test.go` and
//! `secrets/secrets_test.go`, plus the pull-credential path of
//! `kuberuntime_image.go` `PullImage`.
use base64::Engine;
use rusternetes_common::resources::{Pod, PodSpec, Secret};
use rusternetes_cri::fake::FakeCri;
use rusternetes_kubelet::credentialprovider::*;
use std::collections::HashMap;
use std::sync::Arc;

fn auth(u: &str, p: &str, e: &str) -> AuthConfig {
    AuthConfig {
        username: u.into(),
        password: p.into(),
        email: e.into(),
    }
}

#[test]
fn test_urls_match() {
    // (glob, target, expected) from TestURLsMatch.
    let cases = [
        ("*.kubernetes.io", "prefix.kubernetes.io", true),
        ("prefix.*.io", "prefix.kubernetes.io", true),
        ("prefix.kubernetes.*", "prefix.kubernetes.io", true),
        ("*-good.kubernetes.io", "prefix-good.kubernetes.io", true),
        ("*.kubernetes.io/blah", "prefix.kubernetes.io/blah", true),
        ("prefix.*.io/foo", "prefix.kubernetes.io/foo/bar", true),
        (
            "*.kubernetes.io:1111/blah",
            "prefix.kubernetes.io:1111/blah",
            true,
        ),
        (
            "prefix.*.io:1111/foo",
            "prefix.kubernetes.io:1111/foo/bar",
            true,
        ),
        ("*.kubernetes.io", "kubernetes.io", false),
        ("*.*.kubernetes.io", "prefix.kubernetes.io", false),
        ("*.*.kubernetes.io", "kubernetes.io", false),
        ("*kubernetes.io", "a.kubernetes.io", false),
        ("*kubernetes.io", "kubernetes.io", true),
        ("*.*.*.kubernetes.io", "a.b.c.kubernetes.io", true),
        ("kubernetes.io", "kubernetes.com", false),
        ("k*.io", "quay.io", false),
        (
            "*.kubernetes.io:1234/blah",
            "prefix.kubernetes.io:1111/blah",
            false,
        ),
        (
            "prefix.*.io/foo",
            "prefix.kubernetes.io:1111/foo/bar",
            false,
        ),
    ];
    for (glob, target, want) in cases {
        assert_eq!(urls_match_str(glob, target), want, "{glob} vs {target}");
    }
}

#[test]
fn keyring_lookup_most_specific_first() {
    let ada = auth("ada", "smash", "ada@example.com");
    let grace = auth("grace", "squash", "grace@example.com");
    let mut dk = BasicDockerKeyring::default();
    dk.add(vec![
        ("bar.example.com/pong".into(), grace.clone()),
        ("bar.example.com".into(), ada.clone()),
    ]);
    assert_eq!(dk.lookup("bar.example.com"), vec![ada.clone()]);
    assert_eq!(
        dk.lookup("bar.example.com/pong"),
        vec![grace.clone(), ada.clone()]
    );
    assert_eq!(dk.lookup("bar.example.com/ping"), vec![ada.clone()]);
    assert_eq!(dk.lookup("bar.example.com/pong/pang"), vec![grace, ada]);
    assert!(dk.lookup("example.com").is_empty());
    assert!(dk.lookup("foo.example.com").is_empty());
}

#[test]
fn issue_3797_scheme_and_v1_path_match_hostname() {
    let rex = auth("rex", "tiny arms", "rex@example.com");
    let mut dk = BasicDockerKeyring::default();
    dk.add(vec![("https://quay.io/v1/".into(), rex.clone())]);
    for image in ["quay.io", "quay.io/foo", "quay.io/foo/bar"] {
        assert_eq!(dk.lookup(image), vec![rex.clone()], "{image}");
    }
}

#[test]
fn dockerhub_credentials_serve_unqualified_images() {
    let c = auth("u", "p", "");
    let mut dk = BasicDockerKeyring::default();
    dk.add(vec![("https://index.docker.io/v1/".into(), c.clone())]);
    for image in [
        "ubuntu",
        "library/ubuntu",
        "docker.io/library/ubuntu",
        "index.docker.io/library/ubuntu",
        "someuser/img",
    ] {
        assert_eq!(dk.lookup(image), vec![c.clone()], "{image}");
    }
    // A non-DockerHub registry must not get them (TestKeyringMissWithDockerHubCredentials).
    assert!(dk.lookup("quay.io/foo").is_empty());
}

#[test]
fn parse_image_repo_normalizes_like_parse_normalized_named() {
    for (image, want) in [
        ("nginx", "docker.io/library/nginx"),
        ("nginx:1.2", "docker.io/library/nginx"),
        ("user/img:tag", "docker.io/user/img"),
        ("index.docker.io/user/img", "docker.io/user/img"),
        ("localhost:5000/a/b:v1", "localhost:5000/a/b"),
        (
            "reg.example.com:5000/a@sha256:abcd",
            "reg.example.com:5000/a",
        ),
        ("quay.io/foo/bar:1", "quay.io/foo/bar"),
    ] {
        assert_eq!(parse_image_repo(image).unwrap(), want, "{image}");
    }
    assert!(parse_image_repo("").is_err());
}

fn secret(name: &str, ty: &str, key: &str, body: &str) -> Secret {
    let mut s = Secret::new(name, "ns");
    s.secret_type = Some(ty.into());
    s.data = Some(HashMap::from([(key.to_string(), body.as_bytes().to_vec())]));
    s
}

fn b64(s: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(s)
}

#[test]
fn make_docker_keyring_reads_both_secret_kinds() {
    let json = format!(
        r#"{{"auths":{{"reg.example.com":{{"auth":"{}","email":"a@b"}}}}}}"#,
        b64("ada:smash")
    );
    let legacy = r#"{"quay.io":{"username":"u","password":"p"}}"#;
    let secrets = vec![
        secret(
            "j",
            "kubernetes.io/dockerconfigjson",
            ".dockerconfigjson",
            &json,
        ),
        secret("l", "kubernetes.io/dockercfg", ".dockercfg", legacy),
        secret("o", "Opaque", ".dockercfg", legacy),
    ];
    let kr = make_docker_keyring(&secrets).unwrap().unwrap();
    assert_eq!(
        kr.lookup("reg.example.com/x"),
        vec![auth("ada", "smash", "a@b")]
    );
    assert_eq!(kr.lookup("quay.io/x"), vec![auth("u", "p", "")]);

    // No docker-config secret at all: nothing (MakeDockerKeyring falls back to the default).
    assert!(
        make_docker_keyring(&[secret("o", "Opaque", ".dockercfg", legacy)])
            .unwrap()
            .is_none()
    );
    // Malformed JSON is an error, as json.Unmarshal in secretsToTrackedDockerConfigs.
    assert!(
        make_docker_keyring(&[secret("bad", "kubernetes.io/dockercfg", ".dockercfg", "{")])
            .is_err()
    );
    // An auth field that is not user:pass is rejected.
    let bad_auth = format!(r#"{{"x.io":{{"auth":"{}"}}}}"#, b64("nocolon"));
    assert!(make_docker_keyring(&[secret(
        "b",
        "kubernetes.io/dockercfg",
        ".dockercfg",
        &bad_auth
    )])
    .is_err());
}

#[tokio::test]
async fn pull_sends_matching_pod_secret_credentials() {
    use rusternetes_common::resources::pod::LocalObjectReference;
    use rusternetes_storage::{build_key, Storage, StorageBackend};
    let storage = Arc::new(StorageBackend::new_memory());
    let cfg = r#"{"auths":{"reg.example.com":{"username":"ada","password":"smash"}}}"#;
    storage
        .create(
            &build_key("secrets", Some("ns"), "pull"),
            &secret(
                "pull",
                "kubernetes.io/dockerconfigjson",
                ".dockerconfigjson",
                cfg,
            ),
        )
        .await
        .unwrap();
    let vm = rusternetes_kubelet::volumes::VolumeManager::new_with_recording_empty_dir_mounts(
        "/tmp/rusternetes-test-vols".to_string(),
        Some(storage),
        rusternetes_common::auth::TokenManager::new_auto(b"test-secret"),
    );
    let fake = FakeCri::new();
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("fake-cri.sock");
    fake.serve(&sock);
    let rt = rusternetes_kubelet::cri_runtime::CriContainerRuntime::connect(
        &sock,
        tmp.path().to_str().unwrap(),
    )
    .await
    .unwrap()
    .with_volumes(vm)
    // Hermetic: never read the machine's real $HOME/.docker or ./config.json.
    .with_node_keyring(NodeKeyring::new(
        vec![],
        vec![],
        std::time::Duration::from_secs(300),
    ));

    let mut pod = Pod::new("p", PodSpec::default());
    pod.metadata.namespace = Some("ns".into());
    pod.spec.as_mut().unwrap().image_pull_secrets = Some(vec![LocalObjectReference {
        name: "pull".into(),
    }]);

    rt.ensure_image_ref(
        "reg.example.com/app:1",
        Some("Always"),
        Some((&pod, "")),
        None,
    )
    .await
    .unwrap();
    rt.ensure_image_ref("other.io/app:1", Some("Always"), Some((&pod, "")), None)
        .await
        .unwrap();
    let auths = fake.pull_auths();
    assert_eq!(auths.len(), 2);
    let a = auths[0]
        .as_ref()
        .expect("credentials for the matching registry");
    assert_eq!((a.username.as_str(), a.password.as_str()), ("ada", "smash"));
    assert!(
        auths[1].is_none(),
        "no credentials leak to other registries"
    );
}

#[tokio::test]
async fn pull_passes_pod_sandbox_config() {
    use rusternetes_cri::v1;
    let fake = FakeCri::new();
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("fake-cri.sock");
    fake.serve(&sock);
    let rt = rusternetes_kubelet::cri_runtime::CriContainerRuntime::connect(
        &sock,
        tmp.path().to_str().unwrap(),
    )
    .await
    .unwrap();
    let pod = Pod::new("p", PodSpec::default());
    let cfg = v1::PodSandboxConfig {
        metadata: Some(v1::PodSandboxMetadata {
            name: "p".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    rt.ensure_image_ref("a.io/x:1", Some("Always"), Some((&pod, "")), Some(&cfg))
        .await
        .unwrap();
    rt.ensure_image_ref("a.io/y:1", Some("Always"), None, None)
        .await
        .unwrap();
    let got = fake.pull_sandbox_configs();
    assert_eq!(
        got[0].as_ref().unwrap().metadata.as_ref().unwrap().name,
        "p"
    );
    assert!(got[1].is_none());
}

fn write(dir: &std::path::Path, name: &str, body: &str) {
    std::fs::write(dir.join(name), body).unwrap();
}

#[test]
fn node_keyring_prefers_config_json_over_dockercfg_and_skips_invalid() {
    let (bad, good, legacy) = (
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    );
    // ReadDockerConfigJSONFile skips an invalid file and takes the next path.
    write(bad.path(), "config.json", "{");
    write(
        good.path(),
        "config.json",
        r#"{"auths":{"reg.example.com":{"username":"json","password":"p"}}}"#,
    );
    write(
        legacy.path(),
        ".dockercfg",
        r#"{"reg.example.com":{"username":"legacy","password":"p"}}"#,
    );
    let nk = NodeKeyring::new(
        vec![bad.path().into(), good.path().into()],
        vec![legacy.path().into()],
        std::time::Duration::from_secs(300),
    );
    assert_eq!(nk.lookup("reg.example.com/x"), vec![auth("json", "p", "")]);

    // No config.json anywhere: falls back to .dockercfg (ReadDockerConfigFile).
    let nk = NodeKeyring::new(
        vec![bad.path().into()],
        vec![legacy.path().into()],
        std::time::Duration::from_secs(300),
    );
    assert_eq!(
        nk.lookup("reg.example.com/x"),
        vec![auth("legacy", "p", "")]
    );

    // Neither: empty, not an error.
    let nk = NodeKeyring::new(vec![], vec![], std::time::Duration::from_secs(300));
    assert!(nk.lookup("reg.example.com/x").is_empty());
}

#[test]
fn node_keyring_caches_for_its_lifetime() {
    let d = tempfile::tempdir().unwrap();
    write(
        d.path(),
        "config.json",
        r#"{"auths":{"r.io":{"username":"a","password":"p"}}}"#,
    );
    let long = NodeKeyring::new(
        vec![d.path().into()],
        vec![],
        std::time::Duration::from_secs(300),
    );
    let none = NodeKeyring::new(vec![d.path().into()], vec![], std::time::Duration::ZERO);
    assert_eq!(long.lookup("r.io/x"), vec![auth("a", "p", "")]);
    assert_eq!(none.lookup("r.io/x"), vec![auth("a", "p", "")]);
    write(
        d.path(),
        "config.json",
        r#"{"auths":{"r.io":{"username":"b","password":"p"}}}"#,
    );
    assert_eq!(long.lookup("r.io/x"), vec![auth("a", "p", "")], "cached");
    assert_eq!(none.lookup("r.io/x"), vec![auth("b", "p", "")], "expired");
}

fn ext() -> rusternetes_kubelet::credentialprovider_plugin::ExternalCredentialProviderKeyring {
    rusternetes_kubelet::credentialprovider_plugin::ExternalCredentialProviderKeyring::from_providers(vec![])
}

#[tokio::test]
async fn pull_secret_credentials_come_before_node_credentials() {
    let d = tempfile::tempdir().unwrap();
    write(
        d.path(),
        "config.json",
        r#"{"auths":{"reg.example.com":{"username":"node","password":"p"}}}"#,
    );
    let nk = NodeKeyring::new(
        vec![d.path().into()],
        vec![],
        std::time::Duration::from_secs(300),
    );
    let sec = secret(
        "s",
        "kubernetes.io/dockercfg",
        ".dockercfg",
        r#"{"reg.example.com":{"username":"secret","password":"p"}}"#,
    );
    let got = lookup_pull_credentials(&[sec], &nk, &ext(), "reg.example.com/app:1")
        .await
        .unwrap();
    assert_eq!(got, vec![auth("secret", "p", ""), auth("node", "p", "")]);
    let got = lookup_pull_credentials(&[], &nk, &ext(), "reg.example.com/app:1")
        .await
        .unwrap();
    assert_eq!(got, vec![auth("node", "p", "")]);
}
