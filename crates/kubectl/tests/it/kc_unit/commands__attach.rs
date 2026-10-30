//! Unit tests moved from `src/commands/attach.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_client::http::ApiClient;
    pub use rusternetes_kubectl::websocket;
}

mod tests {
    #[test]
    fn test_attach_url_construction() {
        // Test that attach URL is built correctly with all options
        let namespace = "default";
        let pod_name = "mypod";
        let container = Some("nginx");
        let stdin = true;
        let tty = true;

        let mut url_path = format!("/api/v1/namespaces/{}/pods/{}/attach", namespace, pod_name);

        let mut query_params = vec![];
        if let Some(cont) = container {
            query_params.push(format!("container={}", cont));
        }
        query_params.push("stdout=true".to_string());
        query_params.push("stderr=true".to_string());
        if stdin {
            query_params.push("stdin=true".to_string());
        }
        if tty {
            query_params.push("tty=true".to_string());
        }
        if !query_params.is_empty() {
            url_path.push('?');
            url_path.push_str(&query_params.join("&"));
        }

        assert_eq!(
            url_path,
            "/api/v1/namespaces/default/pods/mypod/attach?container=nginx&stdout=true&stderr=true&stdin=true&tty=true"
        );
    }

    #[test]
    fn test_attach_url_no_container() {
        let namespace = "kube-system";
        let pod_name = "coredns";

        let mut url_path = format!("/api/v1/namespaces/{}/pods/{}/attach", namespace, pod_name);

        let query_params = ["stdout=true".to_string(), "stderr=true".to_string()];

        if !query_params.is_empty() {
            url_path.push('?');
            url_path.push_str(&query_params.join("&"));
        }

        assert_eq!(
            url_path,
            "/api/v1/namespaces/kube-system/pods/coredns/attach?stdout=true&stderr=true"
        );
    }

    #[test]
    fn test_attach_url_stdin_only_no_tty() {
        let namespace = "default";
        let pod_name = "interactive";

        let mut url_path = format!("/api/v1/namespaces/{}/pods/{}/attach", namespace, pod_name);

        let query_params = [
            "stdout=true".to_string(),
            "stderr=true".to_string(),
            "stdin=true".to_string(),
        ];
        // tty is false, so not added

        url_path.push('?');
        url_path.push_str(&query_params.join("&"));

        assert!(url_path.contains("stdin=true"));
        assert!(!url_path.contains("tty=true"));
    }
}
