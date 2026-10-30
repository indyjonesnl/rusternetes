//! Unit tests moved from `src/commands/cp.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_client::http::ApiClient;
    pub use std::fs;
    pub use std::path::Path;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::cp::*;

    #[test]
    fn test_parse_copy_spec_from_pod() {
        let (is_upload, pod, pod_path, local) =
            parse_copy_spec("my-pod:/var/log/app.log", "/tmp/app.log").unwrap();
        assert!(!is_upload);
        assert_eq!(pod, "my-pod");
        assert_eq!(pod_path, "/var/log/app.log");
        assert_eq!(local, "/tmp/app.log");
    }

    #[test]
    fn test_parse_copy_spec_to_pod() {
        let (is_upload, pod, pod_path, local) =
            parse_copy_spec("/tmp/config.yaml", "my-pod:/etc/config").unwrap();
        assert!(is_upload);
        assert_eq!(pod, "my-pod");
        assert_eq!(pod_path, "/etc/config");
        assert_eq!(local, "/tmp/config.yaml");
    }

    #[test]
    fn test_parse_copy_spec_both_local_fails() {
        let result = parse_copy_spec("/tmp/a", "/tmp/b");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_copy_spec_both_pod_fails() {
        let result = parse_copy_spec("pod1:/a", "pod2:/b");
        assert!(result.is_err());
    }

    // ===== Additional tests for untested functions =====

    #[test]
    fn test_parse_copy_spec_pod_with_namespace_colon() {
        // Pod name with path containing deeper directories
        let (is_upload, pod, pod_path, local) =
            parse_copy_spec("my-pod:/var/log/nested/deep/file.txt", "/tmp/out").unwrap();
        assert!(!is_upload);
        assert_eq!(pod, "my-pod");
        assert_eq!(pod_path, "/var/log/nested/deep/file.txt");
        assert_eq!(local, "/tmp/out");
    }

    #[test]
    fn test_parse_copy_spec_upload_relative_local() {
        let (is_upload, pod, pod_path, local) =
            parse_copy_spec("./local-file.txt", "my-pod:/tmp/dest").unwrap();
        assert!(is_upload);
        assert_eq!(pod, "my-pod");
        assert_eq!(pod_path, "/tmp/dest");
        assert_eq!(local, "./local-file.txt");
    }

    #[test]
    fn test_parse_copy_spec_root_path() {
        let (is_upload, pod, pod_path, local) = parse_copy_spec("my-pod:/", "/tmp/backup").unwrap();
        assert!(!is_upload);
        assert_eq!(pod, "my-pod");
        assert_eq!(pod_path, "/");
        assert_eq!(local, "/tmp/backup");
    }

    #[test]
    fn test_parse_copy_spec_empty_pod_path() {
        // Colon with empty path after it
        let (is_upload, pod, pod_path, _local) = parse_copy_spec("my-pod:", "/tmp/out").unwrap();
        assert!(!is_upload);
        assert_eq!(pod, "my-pod");
        assert_eq!(pod_path, "");
    }

    #[test]
    fn test_create_tar_from_file_roundtrip() {
        use std::io::Write;
        // Create a temp file
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        {
            let mut f = fs::File::create(&file_path).unwrap();
            f.write_all(b"hello world").unwrap();
        }

        let tar_data = create_tar_from_file(file_path.to_str().unwrap()).unwrap();
        assert!(!tar_data.is_empty());

        // Verify tar contains the file
        let mut archive = tar::Archive::new(tar_data.as_slice());
        let entries: Vec<_> = archive.entries().unwrap().collect();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_create_tar_from_dir_roundtrip() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("subdir");
        fs::create_dir(&sub).unwrap();
        {
            let mut f = fs::File::create(sub.join("a.txt")).unwrap();
            f.write_all(b"aaa").unwrap();
        }
        {
            let mut f = fs::File::create(sub.join("b.txt")).unwrap();
            f.write_all(b"bbb").unwrap();
        }

        let tar_data = create_tar_from_dir(sub.to_str().unwrap()).unwrap();
        assert!(!tar_data.is_empty());

        // Verify tar contains both files
        let mut archive = tar::Archive::new(tar_data.as_slice());
        let entries: Vec<_> = archive.entries().unwrap().collect();
        // Should have at least the two files (may also have . directory entry)
        assert!(entries.len() >= 2);
    }

    #[test]
    fn test_extract_tar_to_local() {
        use std::io::Write;
        // Create a tar from a file
        let src_dir = tempfile::tempdir().unwrap();
        let file_path = src_dir.path().join("extract_me.txt");
        {
            let mut f = fs::File::create(&file_path).unwrap();
            f.write_all(b"extract this content").unwrap();
        }
        let tar_data = create_tar_from_file(file_path.to_str().unwrap()).unwrap();

        // Extract to a different directory
        let dest_dir = tempfile::tempdir().unwrap();
        extract_tar_to_local(dest_dir.path().to_str().unwrap(), &tar_data).unwrap();

        // Verify the extracted file exists
        let extracted = dest_dir.path().join("extract_me.txt");
        assert!(extracted.exists());
        let content = fs::read_to_string(&extracted).unwrap();
        assert_eq!(content, "extract this content");
    }

    #[test]
    fn test_urlencoding_basic() {
        assert_eq!(urlencoding::encode("tar"), "tar");
        assert_eq!(urlencoding::encode("-xf"), "-xf");
        assert_eq!(urlencoding::encode("-"), "-");
        assert_eq!(urlencoding::encode("hello world"), "hello+world");
    }

    #[test]
    fn test_urlencoding_special_chars() {
        assert_eq!(urlencoding::encode("/bin/sh"), "%2Fbin%2Fsh");
        assert_eq!(urlencoding::encode("a&b"), "a%26b");
        assert_eq!(urlencoding::encode(""), "");
        assert_eq!(urlencoding::encode("safe_name.txt"), "safe_name.txt");
        assert_eq!(urlencoding::encode("~tilde"), "~tilde");
    }

    #[test]
    fn test_create_tar_from_file_preserves_name() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("my-config.yaml");
        {
            let mut f = fs::File::create(&file_path).unwrap();
            f.write_all(b"key: value").unwrap();
        }

        let tar_data = create_tar_from_file(file_path.to_str().unwrap()).unwrap();
        let mut archive = tar::Archive::new(tar_data.as_slice());
        let entry = archive.entries().unwrap().next().unwrap().unwrap();
        let path = entry.path().unwrap();
        assert_eq!(path.to_str().unwrap(), "my-config.yaml");
    }

    // ===== 10 additional tests for untested functions =====

    fn make_test_client() -> ApiClient {
        ApiClient::new("http://127.0.0.1:1", true, None).unwrap()
    }

    #[tokio::test]
    async fn test_execute_upload_returns_err_for_nonexistent_local() {
        let client = make_test_client();
        let result = execute(
            &client,
            "/nonexistent/file.txt",
            "my-pod:/tmp/dest",
            "default",
            None,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_download_returns_err_on_unreachable() {
        let client = make_test_client();
        let dest = tempfile::tempdir().unwrap();
        let result = execute(
            &client,
            "my-pod:/var/log/app.log",
            dest.path().to_str().unwrap(),
            "default",
            None,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_copy_to_pod_nonexistent_local_path() {
        let client = make_test_client();
        let result = copy_to_pod(
            &client,
            "default",
            "my-pod",
            "/tmp",
            "/nonexistent/path/file.txt",
            None,
        )
        .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Local path not found"));
    }

    #[tokio::test]
    async fn test_copy_from_pod_returns_err_on_unreachable() {
        let client = make_test_client();
        let dest = tempfile::tempdir().unwrap();
        let result = copy_from_pod(
            &client,
            "default",
            "my-pod",
            "/var/log/app.log",
            dest.path().to_str().unwrap(),
            None,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_both_local_returns_err() {
        let client = make_test_client();
        let result = execute(&client, "/tmp/a", "/tmp/b", "default", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_both_pod_returns_err() {
        let client = make_test_client();
        let result = execute(&client, "pod1:/a", "pod2:/b", "default", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_with_container_returns_err_on_unreachable() {
        let client = make_test_client();
        let dest = tempfile::tempdir().unwrap();
        let result = execute(
            &client,
            "my-pod:/etc/config",
            dest.path().to_str().unwrap(),
            "default",
            Some("sidecar"),
        )
        .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_create_tar_from_file_nonexistent_returns_err() {
        let result = create_tar_from_file("/nonexistent/file.txt");
        assert!(result.is_err());
    }

    #[test]
    fn test_create_tar_from_dir_nonexistent_returns_err() {
        let result = create_tar_from_dir("/nonexistent/directory");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_tar_to_local_invalid_data_returns_err() {
        let dest = tempfile::tempdir().unwrap();
        let result = extract_tar_to_local(dest.path().to_str().unwrap(), b"not a tar");
        assert!(result.is_err());
    }
}
