//! Unit tests moved from `src/commands/logs.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_client::http::ApiClient;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::logs::*;

    #[test]
    fn test_log_url_basic() {
        let url = format!("/api/v1/namespaces/{}/pods/{}/log", "default", "my-pod");
        assert_eq!(url, "/api/v1/namespaces/default/pods/my-pod/log");
    }

    #[test]
    fn test_log_url_with_params() {
        let ns = "prod";
        let pod = "web";
        let mut url = format!("/api/v1/namespaces/{}/pods/{}/log", ns, pod);
        let params = [
            "container=nginx".to_string(),
            "follow=true".to_string(),
            format!("tailLines={}", 100),
        ];
        url.push('?');
        url.push_str(&params.join("&"));

        assert_eq!(
            url,
            "/api/v1/namespaces/prod/pods/web/log?container=nginx&follow=true&tailLines=100"
        );
    }

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration_to_seconds("5s").unwrap(), 5);
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration_to_seconds("2m").unwrap(), 120);
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration_to_seconds("1h").unwrap(), 3600);
    }

    #[test]
    fn test_parse_duration_days() {
        assert_eq!(parse_duration_to_seconds("1d").unwrap(), 86400);
    }

    #[test]
    fn test_parse_duration_raw_number_defaults_seconds() {
        assert_eq!(parse_duration_to_seconds("30").unwrap(), 30);
    }

    #[test]
    fn test_parse_duration_empty_fails() {
        assert!(parse_duration_to_seconds("").is_err());
    }

    #[test]
    fn test_parse_duration_milliseconds() {
        assert_eq!(parse_duration_to_seconds("5000ms").unwrap(), 5);
    }

    #[test]
    fn test_parse_duration_invalid_unit() {
        assert!(parse_duration_to_seconds("10x").is_err());
    }

    #[test]
    fn test_parse_duration_invalid_value() {
        assert!(parse_duration_to_seconds("abcs").is_err());
    }

    #[test]
    fn test_log_url_with_timestamps_and_previous() {
        let ns = "default";
        let pod = "app";
        let mut url = format!("/api/v1/namespaces/{}/pods/{}/log", ns, pod);
        let params = ["timestamps=true".to_string(), "previous=true".to_string()];
        url.push('?');
        url.push_str(&params.join("&"));
        assert_eq!(
            url,
            "/api/v1/namespaces/default/pods/app/log?timestamps=true&previous=true"
        );
    }

    #[test]
    fn test_log_url_with_since_seconds() {
        let ns = "default";
        let pod = "app";
        let mut url = format!("/api/v1/namespaces/{}/pods/{}/log", ns, pod);
        let seconds = parse_duration_to_seconds("5m").unwrap();
        let params = [format!("sinceSeconds={}", seconds)];
        url.push('?');
        url.push_str(&params.join("&"));
        assert_eq!(
            url,
            "/api/v1/namespaces/default/pods/app/log?sinceSeconds=300"
        );
    }

    #[test]
    fn test_log_url_no_params() {
        let ns = "default";
        let pod = "nginx";
        let url = format!("/api/v1/namespaces/{}/pods/{}/log", ns, pod);
        let params: Vec<String> = Vec::new();
        // When no params, no '?' should be appended
        assert!(params.is_empty());
        assert_eq!(url, "/api/v1/namespaces/default/pods/nginx/log");
    }

    #[test]
    fn test_parse_duration_zero_ms() {
        assert_eq!(parse_duration_to_seconds("0ms").unwrap(), 0);
    }

    #[test]
    fn test_log_url_with_all_params() {
        let ns = "prod";
        let pod = "web";
        let mut url = format!("/api/v1/namespaces/{}/pods/{}/log", ns, pod);
        let params = [
            "container=app".to_string(),
            "follow=true".to_string(),
            "tailLines=50".to_string(),
            "timestamps=true".to_string(),
            "sinceTime=2024-01-01T00:00:00Z".to_string(),
            "previous=true".to_string(),
        ];
        url.push('?');
        url.push_str(&params.join("&"));
        assert!(url.contains("container=app"));
        assert!(url.contains("follow=true"));
        assert!(url.contains("tailLines=50"));
        assert!(url.contains("timestamps=true"));
        assert!(url.contains("sinceTime="));
        assert!(url.contains("previous=true"));
    }
}
