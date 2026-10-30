//! Unit tests moved from `src/etcd_endpoint.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::{Error, Result};
    pub use std::path::PathBuf;
    pub use tokio::net::{TcpListener, UnixStream};
}

mod tests {
    use super::__parent::*;
    use rusternetes_storage::etcd_endpoint::*;

    fn unix(p: &str, tls: bool) -> ParsedEndpoint {
        ParsedEndpoint::Unix {
            path: PathBuf::from(p),
            tls,
        }
    }

    #[test]
    fn absolute_path_form() {
        assert_eq!(
            parse_endpoint("unix:///run/kine.sock"),
            unix("/run/kine.sock", false)
        );
        assert_eq!(
            parse_endpoint("unixs:///run/kine.sock"),
            unix("/run/kine.sock", true)
        );
    }

    #[test]
    fn legacy_local_path_form() {
        assert_eq!(parse_endpoint("unix://kine.sock"), unix("kine.sock", false));
        assert_eq!(parse_endpoint("unixs://a/b.sock"), unix("a/b.sock", true));
    }

    #[test]
    fn canonical_grpc_form() {
        assert_eq!(parse_endpoint("unix:kine.sock"), unix("kine.sock", false));
        assert_eq!(
            parse_endpoint("unix:/run/kine.sock"),
            unix("/run/kine.sock", false)
        );
    }

    #[test]
    fn k0s_form_keeps_port_suffix_in_path() {
        assert_eq!(
            parse_endpoint("unix:///run/k0s/kine/kine.sock:2379"),
            unix("/run/k0s/kine/kine.sock:2379", false)
        );
    }

    #[test]
    fn tcp_endpoints_pass_through() {
        for ep in ["http://127.0.0.1:2379", "https://e:2379", "10.0.0.44:437"] {
            assert_eq!(parse_endpoint(ep), ParsedEndpoint::Passthrough(ep.into()));
        }
    }

    #[tokio::test]
    async fn bridge_splices_to_unix_socket() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let dir = std::env::temp_dir().join(format!("etcd-ep-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let sock = dir.join("echo.sock:2379");
        let l = tokio::net::UnixListener::bind(&sock).unwrap();
        tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            let mut b = [0u8; 4];
            s.read_exact(&mut b).await.unwrap();
            s.write_all(&b).await.unwrap();
        });
        let ep = format!("unix://{}", sock.display());
        let (eps, _bridges) = resolve_endpoints(vec![ep]).await.unwrap();
        let mut c = tokio::net::TcpStream::connect(&eps[0]).await.unwrap();
        c.write_all(b"ping").await.unwrap();
        let mut b = [0u8; 4];
        c.read_exact(&mut b).await.unwrap();
        assert_eq!(&b, b"ping");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn unixs_is_rejected() {
        assert!(resolve_endpoints(vec!["unixs:///x.sock".into()])
            .await
            .is_err());
    }
}
