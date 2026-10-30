//! Unit tests moved from `src/commands/cluster_info.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_client::http::ApiClient;
}

mod tests {
    #[test]
    fn test_cluster_info_output_format() {
        let base_url = "https://localhost:6443";
        let output = format!("Kubernetes control plane is running at {}", base_url);
        assert!(output.contains("Kubernetes control plane is running at"));
        assert!(output.contains("https://localhost:6443"));
    }

    #[test]
    fn test_dump_help_message() {
        let msg =
            "To further debug and diagnose cluster problems, use 'kubectl cluster-info dump'.";
        assert!(msg.contains("cluster-info dump"));
    }

    #[test]
    fn test_cluster_info_various_urls() {
        for url in &[
            "https://10.0.0.1:6443",
            "https://k8s.example.com:443",
            "http://localhost:8080",
        ] {
            let output = format!("Kubernetes control plane is running at {}", url);
            assert!(output.starts_with("Kubernetes control plane is running at"));
            assert!(output.contains(url));
        }
    }
}
