//! Unit tests moved from `src/streaming.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::extract::ws::{Message, WebSocket};
    pub use base64::Engine;
    pub use futures::StreamExt;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::streaming::*;

    #[test]
    fn frame_log_chunk_binary_is_raw_no_channel_prefix() {
        // binary.k8s.io: raw bytes, sent verbatim as a Binary message, with
        // NO channel-byte prefix (unlike the exec channel protocol).
        let msg = frame_log_chunk(b"container is alive\n", false);
        match msg {
            Message::Binary(bytes) => assert_eq!(bytes, b"container is alive\n"),
            other => panic!("expected Binary, got {other:?}"),
        }
    }

    #[test]
    fn frame_log_chunk_base64_is_text_std_base64() {
        // base64.binary.k8s.io: standard-base64 of the bytes, as a Text message.
        // base64.StdEncoding.EncodeToString([]byte("foo\n")) == "Zm9vCg=="
        let msg = frame_log_chunk(b"foo\n", true);
        match msg {
            Message::Text(s) => assert_eq!(s, "Zm9vCg=="),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[test]
    fn frame_log_chunk_empty_ping() {
        // The leading ping frame is a zero-length message of the right kind.
        match frame_log_chunk(&[], false) {
            Message::Binary(b) => assert!(b.is_empty()),
            other => panic!("expected empty Binary, got {other:?}"),
        }
        match frame_log_chunk(&[], true) {
            Message::Text(s) => assert!(s.is_empty()),
            other => panic!("expected empty Text, got {other:?}"),
        }
    }

    #[test]
    fn log_protocol_selection_matches_upstream_handshake() {
        // No Sec-WebSocket-Protocol header -> empty protocol -> binary framing.
        assert!(!log_protocol_is_base64(None));
        // The e2e conformance client requests exactly "binary.k8s.io".
        assert!(!log_protocol_is_base64(Some("binary.k8s.io")));
        // Explicit base64 request.
        assert!(log_protocol_is_base64(Some("base64.binary.k8s.io")));
        // First supported requested protocol wins (upstream `handshake`).
        assert!(!log_protocol_is_base64(Some(
            "binary.k8s.io, base64.binary.k8s.io"
        )));
        assert!(log_protocol_is_base64(Some(
            "base64.binary.k8s.io, binary.k8s.io"
        )));
        // Unknown protocols are skipped until a supported one is found.
        assert!(log_protocol_is_base64(Some(
            "v5.channel.k8s.io, base64.binary.k8s.io"
        )));
        // Whitespace around the comma-separated entries is tolerated.
        assert!(log_protocol_is_base64(Some("  base64.binary.k8s.io  ")));
    }

    #[test]
    fn log_ws_protocols_advertises_named_subprotocols_in_preference_order() {
        assert_eq!(LOG_WS_PROTOCOLS, ["binary.k8s.io", "base64.binary.k8s.io"]);
    }
}
