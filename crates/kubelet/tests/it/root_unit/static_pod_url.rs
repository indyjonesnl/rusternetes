//! Unit tests moved from `src/static_pod_url.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use rusternetes_common::resources::Pod;
    pub use rusternetes_kubelet::static_pods::{parse_http_manifest, MAX_CONFIG_LENGTH};
    pub use std::sync::Mutex;
    pub use std::time::Duration;
    pub use tracing::{debug, info};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::static_pod_url::*;
    use std::collections::BTreeMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const POD: &str = "apiVersion: v1\nkind: Pod\nmetadata:\n  name: web\nspec:\n  containers:\n  - name: c\n    image: busybox\n";

    /// Serve `(status, body)` for each connection; record request heads.
    async fn serve(responses: Vec<(u16, String)>) -> (String, std::sync::Arc<Mutex<Vec<String>>>) {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        tokio::spawn(async move {
            for (code, body) in responses {
                let (mut s, _) = l.accept().await.unwrap();
                let mut buf = vec![0u8; 8192];
                let n = s.read(&mut buf).await.unwrap();
                seen2
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&buf[..n]).to_string());
                let resp = format!(
                    "HTTP/1.1 {code} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = s.write_all(resp.as_bytes()).await;
            }
        });
        (format!("http://{addr}/pod.yaml"), seen)
    }

    #[tokio::test]
    async fn single_pod_defaults_and_unchanged_short_circuit() {
        let (url, seen) = serve(vec![(200, POD.into()), (200, POD.into())]).await;
        let mut hdr = BTreeMap::new();
        hdr.insert(
            "X-Token".to_string(),
            vec!["a".to_string(), "b".to_string()],
        );
        let src = UrlSource::new(&url, &hdr, "node-1").unwrap();
        let r = src.extract_from_url().await;
        assert!(r.error.is_none(), "{:?}", r.error);
        let pods = r.update.expect("SET update");
        assert_eq!(pods.len(), 1);
        let p = &pods[0];
        assert_eq!(p.metadata.name, "web-node-1");
        assert_eq!(p.metadata.namespace.as_deref(), Some("default"));
        assert_eq!(
            p.spec.as_ref().unwrap().node_name.as_deref(),
            Some("node-1")
        );
        let ann = p.metadata.annotations.as_ref().unwrap();
        assert_eq!(ann["kubernetes.io/config.source"], "http");
        // unchanged body: no update (http.go bytes.Equal short-circuit)
        let r2 = src.extract_from_url().await;
        assert!(r2.error.is_none() && r2.update.is_none());
        let heads = seen.lock().unwrap().clone();
        let head = heads[0].to_lowercase();
        assert!(
            head.contains("x-token: a") && head.contains("x-token: b"),
            "{head}"
        );
    }

    #[tokio::test]
    async fn pod_list_non_200_and_empty_body() {
        let list = "apiVersion: v1\nkind: PodList\nitems:\n- metadata:\n    name: a\n  spec:\n    containers:\n    - name: c\n      image: i\n- metadata:\n    name: b\n  spec:\n    containers:\n    - name: c\n      image: i\n";
        let (url, _) = serve(vec![
            (200, list.into()),
            (500, "boom".into()),
            (200, String::new()),
        ])
        .await;
        let src = UrlSource::new(&url, &BTreeMap::new(), "n").unwrap();
        let r = src.extract_from_url().await;
        let names: Vec<_> = r
            .update
            .unwrap()
            .iter()
            .map(|p| p.metadata.name.clone())
            .collect();
        assert_eq!(names, ["a-n", "b-n"]);
        let r = src.extract_from_url().await;
        assert!(r.update.is_none());
        assert!(format!("{:#}", r.error.unwrap()).contains("500"));
        let r = src.extract_from_url().await;
        assert_eq!(
            r.update.unwrap().len(),
            0,
            "empty PodList marks source seen"
        );
        assert!(format!("{:#}", r.error.unwrap()).contains("zero-length"));
    }

    #[tokio::test]
    async fn unparsable_body_errors_once_then_short_circuits() {
        let (url, _) = serve(vec![(200, "{not a pod".into()), (200, "{not a pod".into())]).await;
        let src = UrlSource::new(&url, &BTreeMap::new(), "n").unwrap();
        let r = src.extract_from_url().await;
        assert!(r.update.is_none() && r.error.is_some());
        // s.data was recorded before parsing (http.go:112), so the same bad
        // body is not re-parsed.
        let r = src.extract_from_url().await;
        assert!(r.update.is_none() && r.error.is_none());
    }
}
