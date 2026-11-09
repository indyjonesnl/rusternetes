//! Ports of `pkg/kubelet/logs/container_log_manager_test.go` (`TestGetAllLogs`,
//! `TestRotateLogs`, `TestClean`, `TestCleanupUnusedLog`, `TestRemoveExcessLog`,
//! `TestCompressLog`, `TestRotateLatestLog`), plus tests that the
//! `KubeletConfiguration` fields reach the manager (`kubelet.go:714-721`).
use rusternetes_cri::fake::FakeCri;
use rusternetes_cri::{v1, CriClient};
use rusternetes_kubelet::config::KubeletConfiguration;
use rusternetes_kubelet::container_log_manager::*;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

const FILE_FIXED_NOW: &str = "20240102-030405";

struct Env {
    fake: FakeCri,
    cri: CriClient,
    _tmp: tempfile::TempDir,
}

async fn env() -> Env {
    let fake = FakeCri::new();
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("fake-cri.sock");
    fake.serve(&sock);
    let cri = CriClient::connect(&sock).await.unwrap();
    Env {
        fake,
        cri,
        _tmp: tmp,
    }
}

fn fixed_clock() -> Box<dyn Fn() -> chrono::DateTime<chrono::Local> + Send + Sync> {
    let now = chrono::NaiveDateTime::parse_from_str(FILE_FIXED_NOW, "%Y%m%d-%H%M%S")
        .unwrap()
        .and_local_timezone(chrono::Local)
        .unwrap();
    Box::new(move || now)
}

fn manager(cri: CriClient, max_files: i64, max_size: i64) -> RotatingContainerLogManager {
    RotatingContainerLogManager::with_clock(
        cri,
        LogRotatePolicy {
            max_size,
            max_files,
        },
        10,
        Duration::from_secs(10),
        fixed_clock(),
    )
}

fn touch(dir: &Path, names: &[&str]) {
    for n in names {
        fs::File::create(dir.join(n)).unwrap();
    }
}

fn names(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    v.sort();
    v
}

#[test]
fn test_get_all_logs() {
    let dir = tempfile::tempdir().unwrap();
    touch(
        dir.path(),
        &[
            "test-log.11111111-111111.gz",
            "test-log",
            "test-log.00000000-000000.gz",
            "test-log.19900322-000000.gz",
            "test-log.19900322-111111.gz",
            "test-log.19880620-000000", // unused log
            "test-log.19880620-000000.gz",
            "test-log.19880620-111111.gz",
            "test-log.20180101-000000",
            "test-log.20180101-000000.tmp", // temporary log
        ],
    );
    let expect: Vec<String> = [
        "test-log.00000000-000000.gz",
        "test-log.11111111-111111.gz",
        "test-log.19880620-000000.gz",
        "test-log.19880620-111111.gz",
        "test-log.19900322-000000.gz",
        "test-log.19900322-111111.gz",
        "test-log.20180101-000000",
        "test-log",
    ]
    .iter()
    .map(|n| dir.path().join(n).to_string_lossy().into_owned())
    .collect();
    let got = get_all_logs(&dir.path().join("test-log").to_string_lossy()).unwrap();
    assert_eq!(expect, got);
}

#[tokio::test]
async fn test_rotate_logs() {
    let e = env().await;
    let dir = tempfile::tempdir().unwrap();
    let c = Arc::new(manager(e.cri.clone(), 3, 10));
    let test_logs = [
        "test-log-1",
        "test-log-2",
        "test-log-3",
        "test-log-4",
        "test-log-3.00000000-000001",
        "test-log-3.00000000-000000.gz",
    ];
    let test_content = [
        "short",
        "longer than 10 bytes",
        "longer than 10 bytes",
        "longer than 10 bytes",
        "the length doesn't matter",
        "the length doesn't matter",
    ];
    for (l, content) in test_logs.iter().zip(test_content) {
        fs::write(dir.path().join(l), content).unwrap();
    }
    let p = |i: usize| dir.path().join(test_logs[i]).to_string_lossy().into_owned();
    use v1::ContainerState::*;
    e.fake
        .add_container_with_log_path("container-not-need-rotate", ContainerRunning, &p(0));
    e.fake
        .add_container_with_log_path("container-need-rotate", ContainerRunning, &p(1));
    e.fake
        .add_container_with_log_path("container-has-excess-log", ContainerRunning, &p(2));
    e.fake
        .add_container_with_log_path("container-is-not-running", ContainerExited, &p(3));

    // Push the items into the queue before starting the worker.
    c.rotate_logs().await.unwrap();
    assert_eq!(c.queue.len(), 3, "only running containers are queued");

    // Monitor the queue and shut it down once drained to end the worker.
    let monitor = {
        let c = c.clone();
        tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while !c.queue.is_empty() {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "queue never drained"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            c.queue.shut_down();
        })
    };
    c.process_queue_items(1).await;
    monitor.await.unwrap();

    let logs = names(dir.path());
    assert_eq!(
        logs,
        vec![
            test_logs[0].to_string(),
            format!("{}.{FILE_FIXED_NOW}", test_logs[1]),
            format!("{}{COMPRESS_SUFFIX}", test_logs[4]),
            format!("{}.{FILE_FIXED_NOW}", test_logs[2]),
            test_logs[3].to_string(),
        ]
    );
    // The compressed one is real gzip of the old rotated content.
    use std::io::Read;
    let gz = fs::File::open(
        dir.path()
            .join(format!("{}{COMPRESS_SUFFIX}", test_logs[4])),
    )
    .unwrap();
    let mut s = String::new();
    flate2::read::GzDecoder::new(gz)
        .read_to_string(&mut s)
        .unwrap();
    assert_eq!(s, "the length doesn't matter");
    // One ReopenContainerLog per rotated container.
    let reopens = e
        .fake
        .called()
        .iter()
        .filter(|c| *c == "ReopenContainerLog")
        .count();
    assert_eq!(reopens, 2);
}

#[tokio::test]
async fn test_clean() {
    let e = env().await;
    let dir = tempfile::tempdir().unwrap();
    let c = manager(e.cri.clone(), 3, 10);
    let test_logs = [
        "test-log-1",
        "test-log-2",
        "test-log-3",
        "test-log-2.00000000-000000.gz",
        "test-log-2.00000000-000001",
        "test-log-3.00000000-000000.gz",
        "test-log-3.00000000-000001",
    ];
    touch(dir.path(), &test_logs);
    let p = |i: usize| dir.path().join(test_logs[i]).to_string_lossy().into_owned();
    use v1::ContainerState::*;
    e.fake
        .add_container_with_log_path("container-1", ContainerRunning, &p(0));
    e.fake
        .add_container_with_log_path("container-2", ContainerRunning, &p(1));
    e.fake
        .add_container_with_log_path("container-3", ContainerExited, &p(2));

    c.clean("container-3").await.unwrap();

    assert_eq!(
        names(dir.path()),
        vec![
            test_logs[0].to_string(),
            test_logs[1].to_string(),
            test_logs[3].to_string(),
            test_logs[4].to_string(),
        ]
    );
}

#[tokio::test]
async fn test_cleanup_unused_log() {
    let e = env().await;
    let dir = tempfile::tempdir().unwrap();
    let paths: Vec<String> = [
        "test-log-1",     // regular log
        "test-log-1.tmp", // temporary log
        "test-log-2",     // unused log
        "test-log-2.gz",  // compressed log
    ]
    .iter()
    .map(|n| {
        fs::File::create(dir.path().join(n)).unwrap();
        dir.path().join(n).to_string_lossy().into_owned()
    })
    .collect();
    let c = manager(e.cri.clone(), 5, 10);
    let got = c.cleanup_unused_logs(paths.clone()).unwrap();
    assert_eq!(got, vec![paths[0].clone(), paths[3].clone()]);
    assert_eq!(names(dir.path()), vec!["test-log-1", "test-log-2.gz"]);
}

#[tokio::test]
async fn test_remove_excess_log() {
    let e = env().await;
    for (desc, max, expect) in [
        ("MaxFiles equal to 2", 2, vec![]),
        ("MaxFiles more than 2", 3, vec!["test-log-4"]),
        (
            "MaxFiles more than log file number",
            6,
            vec!["test-log-1", "test-log-2", "test-log-3", "test-log-4"],
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let paths: Vec<String> = ["test-log-3", "test-log-1", "test-log-2", "test-log-4"]
            .iter()
            .map(|n| {
                fs::File::create(dir.path().join(n)).unwrap();
                dir.path().join(n).to_string_lossy().into_owned()
            })
            .collect();
        let c = manager(e.cri.clone(), max, 10);
        let got = c.remove_excess_logs(paths).unwrap();
        let got: Vec<String> = got
            .iter()
            .map(|p| {
                Path::new(p)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(got, expect, "{desc}");
        assert_eq!(names(dir.path()), expect, "{desc}");
    }
}

#[test]
fn test_compress_log() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("test-rotate-latest-log");
    fs::write(&log, "test log content").unwrap();
    fs::set_permissions(&log, fs::Permissions::from_mode(0o640)).unwrap();
    let log = log.to_string_lossy().into_owned();
    let orig_mode = fs::metadata(&log).unwrap().permissions().mode();

    compress_log(&log).unwrap();
    let compressed =
        fs::metadata(format!("{log}{COMPRESS_SUFFIX}")).expect("log should be compressed");
    assert_eq!(
        orig_mode,
        compressed.permissions().mode(),
        "modes must match"
    );
    assert!(
        compress_log("test-unknown-log").is_err(),
        "compressing unknown log errors"
    );
    assert!(
        fs::metadata(format!("{log}{TMP_SUFFIX}")).is_err(),
        "temporary log renamed"
    );
    assert!(fs::metadata(&log).is_err(), "original log removed");
}

#[tokio::test]
async fn test_rotate_latest_log() {
    for (desc, fail, expect_error, expect_original, expect_rotated) in [
        (
            "should successfully rotate log when MaxFiles is 2",
            false,
            false,
            false,
            true,
        ),
        (
            "should restore original log when ReopenContainerLog fails",
            true,
            true,
            true,
            false,
        ),
    ] {
        let e = env().await;
        if fail {
            e.fake.fail_reopen_container_log("random error");
        }
        let dir = tempfile::tempdir().unwrap();
        let c = manager(e.cri.clone(), 2, 0);
        let log = dir.path().join("test-rotate-latest-log");
        fs::File::create(&log).unwrap();
        let log = log.to_string_lossy().into_owned();
        let rotated = format!("{log}.{FILE_FIXED_NOW}");
        let res = c.rotate_latest_log("test-id", &log).await;
        assert_eq!(res.is_err(), expect_error, "{desc}");
        assert_eq!(fs::metadata(&log).is_ok(), expect_original, "{desc}");
        assert_eq!(fs::metadata(&rotated).is_ok(), expect_rotated, "{desc}");
        assert_eq!(
            e.fake.called(),
            vec!["ReopenContainerLog".to_string()],
            "{desc}"
        );
    }
}

#[test]
fn test_parse_max_size() {
    assert_eq!(parse_max_size("10Mi").unwrap(), 10 * 1024 * 1024);
    assert_eq!(parse_max_size("1").unwrap(), 1);
    assert_eq!(parse_max_size("-1").unwrap(), -1);
    assert!(
        parse_max_size("1.5").is_err(),
        "fractional bytes are not an int64"
    );
    assert!(parse_max_size("garbage").is_err());
}

#[tokio::test]
async fn new_container_log_manager_validates() {
    let e = env().await;
    let ok = ContainerLogConfig::default();
    assert!(new_container_log_manager(e.cri.clone(), &ok).is_ok());
    let bad_files = ContainerLogConfig {
        max_files: 1,
        ..ok.clone()
    };
    assert!(new_container_log_manager(e.cri.clone(), &bad_files).is_err());
    let bad_size = ContainerLogConfig {
        max_size: "x".into(),
        ..ok
    };
    assert!(new_container_log_manager(e.cri.clone(), &bad_size).is_err());
}

fn config_from_yaml(yaml: &str) -> KubeletConfiguration {
    serde_yaml::from_str(yaml).unwrap()
}

/// The config fields reach the manager: a kubelet-config file's
/// `containerLog*` values drive what the started manager does.
#[tokio::test]
async fn kubelet_configuration_reaches_the_manager() {
    let cfg = config_from_yaml(
        "apiVersion: kubelet.config.k8s.io/v1beta1\nkind: KubeletConfiguration\n\
         containerLogMaxSize: \"5\"\ncontainerLogMaxFiles: 4\ncontainerLogMaxWorkers: 2\n\
         containerLogMonitorInterval: 3s\n",
    );
    cfg.validate().unwrap();
    let lc = cfg.effective_container_log_config();
    assert_eq!(
        lc,
        ContainerLogConfig {
            max_size: "5".into(),
            max_files: 4,
            max_workers: 2,
            monitor_interval: Duration::from_secs(3),
        }
    );

    // Defaults when unset (defaults.go:257-267).
    let d =
        config_from_yaml("apiVersion: kubelet.config.k8s.io/v1beta1\nkind: KubeletConfiguration\n")
            .effective_container_log_config();
    assert_eq!(d, ContainerLogConfig::default());

    // Started manager rotates a 6-byte log (> containerLogMaxSize 5) ...
    let e = env().await;
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("big.log");
    fs::write(&log, "123456").unwrap();
    e.fake.add_container_with_log_path(
        "c1",
        v1::ContainerState::ContainerRunning,
        &log.to_string_lossy(),
    );
    let m = new_container_log_manager(e.cri.clone(), &lc).unwrap();
    m.clone().start();
    wait_for(|| names(dir.path()).len() == 1 && !log.exists()).await;
    assert!(names(dir.path())[0].starts_with("big.log."));

    // ... while a log under the limit is left alone.
    let e2 = env().await;
    let dir2 = tempfile::tempdir().unwrap();
    let small = dir2.path().join("small.log");
    fs::write(&small, "1234").unwrap();
    e2.fake.add_container_with_log_path(
        "c2",
        v1::ContainerState::ContainerRunning,
        &small.to_string_lossy(),
    );
    new_container_log_manager(e2.cri.clone(), &lc)
        .unwrap()
        .start();
    wait_for(|| e2.fake.called().iter().any(|c| c == "ContainerStatus")).await;
    assert!(small.exists());
}

/// A negative containerLogMaxSize disables rotation (stub manager).
#[tokio::test]
async fn negative_max_size_disables_rotation() {
    let e = env().await;
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("big.log");
    fs::write(&log, "123456").unwrap();
    e.fake.add_container_with_log_path(
        "c1",
        v1::ContainerState::ContainerRunning,
        &log.to_string_lossy(),
    );
    let cfg = ContainerLogConfig {
        max_size: "-1".into(),
        ..Default::default()
    };
    let m = new_container_log_manager(e.cri.clone(), &cfg).unwrap();
    m.clone().start();
    m.clean("c1").await.unwrap(); // stub: no-op
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(log.exists());
    assert!(e.fake.called().is_empty(), "stub makes no CRI calls");
}

#[test]
fn config_validation_matches_upstream() {
    let base = "apiVersion: kubelet.config.k8s.io/v1beta1\nkind: KubeletConfiguration\n";
    for (extra, msg) in [
        ("containerLogMaxWorkers: 0\n", "containerLogMaxWorkers must be greater than or equal to 1"),
        ("containerLogMonitorInterval: 2s\n", "containerLogMonitorInterval must be a positive time duration greater than or equal to 3s"),
        ("containerLogMaxFiles: 1\n", "containerLogMaxFiles must be greater than 1"),
    ] {
        let err = config_from_yaml(&format!("{base}{extra}")).validate().unwrap_err();
        assert!(err.to_string().contains(msg), "{extra}: {err}");
    }
}

async fn wait_for(mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not reached in 10s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The Kubelet carries the config fields to where `run` builds the manager
/// from them (`kubelet.go:714-721`); unset keeps rotation off.
#[tokio::test]
async fn kubelet_builder_carries_container_log_config() {
    let dir = tempfile::tempdir().unwrap();
    let mk = || async {
        rusternetes_kubelet::kubelet::Kubelet::new(
            "node-log".into(),
            Arc::new(rusternetes_storage::StorageBackend::new_memory()),
            10,
            dir.path().join("vols").display().to_string(),
            "10.96.0.10".into(),
            "cluster.local".into(),
            "bridge".into(),
            String::new(),
        )
        .await
        .unwrap()
    };
    assert_eq!(mk().await.container_log_config, None);
    let cfg = config_from_yaml(
        "apiVersion: kubelet.config.k8s.io/v1beta1\nkind: KubeletConfiguration\n\
         containerLogMaxSize: 1Mi\ncontainerLogMonitorInterval: 7s\n",
    );
    let k = mk()
        .await
        .with_container_log_config(cfg.effective_container_log_config());
    let got = k.container_log_config.unwrap();
    assert_eq!(got.max_size, "1Mi");
    assert_eq!(got.monitor_interval, Duration::from_secs(7));
    assert_eq!((got.max_files, got.max_workers), (5, 1));
}
