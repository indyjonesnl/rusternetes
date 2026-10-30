//! Unit tests moved from `src/commands/exec.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_client::http::ApiClient;
    pub use rusternetes_kubectl::websocket;
}

mod tests {
    use rusternetes_kubectl::commands::exec::urlencoding;

    #[test]
    fn test_exec_url_construction_basic() {
        let namespace = "default";
        let pod_name = "my-pod";
        let mut url_path = format!("/api/v1/namespaces/{}/pods/{}/exec", namespace, pod_name);
        let mut query_params = vec![];
        query_params.push(format!("command={}", urlencoding::encode("ls")));
        query_params.push("stdout=true".to_string());
        query_params.push("stderr=true".to_string());
        url_path.push('?');
        url_path.push_str(&query_params.join("&"));

        assert_eq!(
            url_path,
            "/api/v1/namespaces/default/pods/my-pod/exec?command=ls&stdout=true&stderr=true"
        );
    }

    #[test]
    fn test_exec_url_with_container_and_tty() {
        let namespace = "prod";
        let pod_name = "web";
        let mut url_path = format!("/api/v1/namespaces/{}/pods/{}/exec", namespace, pod_name);
        let mut query_params = vec![];
        query_params.push(format!("command={}", urlencoding::encode("/bin/sh")));
        query_params.push(format!("container={}", "main"));
        query_params.push("stdout=true".to_string());
        query_params.push("stderr=true".to_string());
        query_params.push("stdin=true".to_string());
        query_params.push("tty=true".to_string());
        url_path.push('?');
        url_path.push_str(&query_params.join("&"));

        assert!(url_path.contains("container=main"));
        assert!(url_path.contains("stdin=true"));
        assert!(url_path.contains("tty=true"));
        assert!(url_path.contains("command=%2Fbin%2Fsh"));
    }

    #[test]
    fn test_urlencoding_special_chars() {
        assert_eq!(urlencoding::encode("/bin/sh"), "%2Fbin%2Fsh");
        assert_eq!(urlencoding::encode("hello world"), "hello+world");
        assert_eq!(urlencoding::encode("simple"), "simple");
    }

    #[test]
    fn test_exec_url_multi_command_args() {
        let namespace = "default";
        let pod_name = "my-pod";
        let mut url_path = format!("/api/v1/namespaces/{}/pods/{}/exec", namespace, pod_name);
        let mut query_params = vec![];
        for cmd in &["sh", "-c", "echo hello"] {
            query_params.push(format!("command={}", urlencoding::encode(cmd)));
        }
        query_params.push("stdout=true".to_string());
        query_params.push("stderr=true".to_string());
        url_path.push('?');
        url_path.push_str(&query_params.join("&"));

        assert!(url_path.contains("command=sh"));
        assert!(url_path.contains("command=-c"));
        assert!(url_path.contains("command=echo+hello"));
    }

    #[test]
    fn test_exec_url_no_stdin_no_tty() {
        let namespace = "default";
        let pod_name = "test-pod";
        let mut url_path = format!("/api/v1/namespaces/{}/pods/{}/exec", namespace, pod_name);
        let mut query_params = vec![];
        query_params.push(format!("command={}", urlencoding::encode("date")));
        query_params.push("stdout=true".to_string());
        query_params.push("stderr=true".to_string());
        // stdin=false and tty=false means those params are not added
        url_path.push('?');
        url_path.push_str(&query_params.join("&"));

        assert!(!url_path.contains("stdin=true"));
        assert!(!url_path.contains("tty=true"));
    }

    #[test]
    fn test_urlencoding_encode_preserves_alphanumeric() {
        let result = urlencoding::encode("abcABC123");
        assert_eq!(result, "abcABC123");
    }
}
