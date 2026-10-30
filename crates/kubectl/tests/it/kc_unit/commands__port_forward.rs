//! Unit tests moved from `src/commands/port_forward.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_client::http::ApiClient;
    pub use rusternetes_kubectl::websocket;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::port_forward::*;

    #[test]
    fn test_parse_port_mapping_local_remote() {
        let (local, remote) = parse_port_mapping("8080:80").unwrap();
        assert_eq!(local, 8080);
        assert_eq!(remote, 80);
    }

    #[test]
    fn test_parse_port_mapping_same_port() {
        let (local, remote) = parse_port_mapping("3000").unwrap();
        assert_eq!(local, 3000);
        assert_eq!(remote, 3000);
    }

    #[test]
    fn test_parse_port_mapping_invalid() {
        let result = parse_port_mapping("abc");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_port_mapping_invalid_local() {
        let result = parse_port_mapping("abc:80");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_port_mapping_invalid_remote() {
        let result = parse_port_mapping("80:abc");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_port_mapping_high_port() {
        let (local, remote) = parse_port_mapping("65535:443").unwrap();
        assert_eq!(local, 65535);
        assert_eq!(remote, 443);
    }

    #[test]
    fn test_parse_port_mapping_overflow_fails() {
        // Port number exceeding u16 max should fail
        let result = parse_port_mapping("70000");
        assert!(result.is_err());
    }
}
