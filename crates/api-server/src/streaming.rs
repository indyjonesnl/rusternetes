//! WebSocket streaming support for logs and port-forward.
//!
//! exec and attach are now upgrade-proxied to the pod's kubelet
//! via `rusternetes_streamproxy::proxy_upgrade` (Task 6).

use axum::extract::ws::{Message, WebSocket};
use base64::Engine;
use futures::StreamExt;
use rusternetes_common::resources::Pod;

/// WebSocket subprotocol that streams the raw log bytes to the client as
/// binary messages. Upstream:
/// `staging/src/k8s.io/apimachinery/pkg/util/httpstream/wsstream/stream.go:34`
/// (`const binaryWebSocketProtocol = "binary.k8s.io"`).
pub const BINARY_LOG_PROTOCOL: &str = "binary.k8s.io";

/// WebSocket subprotocol that base64-encodes the log bytes and streams them as
/// text messages. Upstream:
/// `staging/src/k8s.io/apimachinery/pkg/util/httpstream/wsstream/stream.go:40`
/// (`const base64BinaryWebSocketProtocol = "base64.binary.k8s.io"`).
pub const BASE64_LOG_PROTOCOL: &str = "base64.binary.k8s.io";

/// The ordered list of subprotocols the log endpoint offers, in decreasing
/// order of server preference. Mirrors upstream `NewDefaultReaderProtocols`
/// (`stream.go:49`), which supports `""` (empty), `binary.k8s.io` and
/// `base64.binary.k8s.io`. The empty protocol is handled implicitly by axum
/// (a client that requests no subprotocol gets the binary framing), so we only
/// advertise the two named ones here.
pub const LOG_WS_PROTOCOLS: [&str; 2] = [BINARY_LOG_PROTOCOL, BASE64_LOG_PROTOCOL];

/// Given the value of a request's `Sec-WebSocket-Protocol` header, decide
/// whether the negotiated log subprotocol base64-encodes the payload.
///
/// Mirrors upstream `handshake` (`wsstream/conn.go:142`): the server picks the
/// first client-requested protocol that it supports. For the log reader the
/// supported set is `{"", "binary.k8s.io", "base64.binary.k8s.io"}` where only
/// `base64.binary.k8s.io` is base64-encoded (`stream.go:49-55`). Any other
/// selection (including the empty / absent protocol) uses the raw binary
/// framing, matching `ReaderProtocolConfig{Binary: true}`.
///
/// Returns `true` when the negotiated protocol base64-encodes the stream.
pub fn log_protocol_is_base64(sec_websocket_protocol: Option<&str>) -> bool {
    let Some(header) = sec_websocket_protocol else {
        return false;
    };
    for requested in header.split(',') {
        let requested = requested.trim();
        match requested {
            BASE64_LOG_PROTOCOL => return true,
            BINARY_LOG_PROTOCOL | "" => return false,
            _ => continue,
        }
    }
    false
}

/// Frame a chunk of log bytes into the wire representation expected by the
/// negotiated log subprotocol, producing one WebSocket message.
///
/// The single-stream log protocols do NOT prefix a channel byte (unlike the
/// exec/attach `channel.k8s.io` framing in [`frame_channel`]). Upstream
/// `messageCopy` (`wsstream/stream.go:142`) sends `buf[:n]` verbatim as a
/// binary message for `binary.k8s.io`, or `base64.StdEncoding.EncodeToString`
/// as a text message for `base64.binary.k8s.io`.
///
/// - `base64 == false` -> [`Message::Binary`] with the raw bytes.
/// - `base64 == true`  -> [`Message::Text`] with the standard-base64 string.
pub fn frame_log_chunk(data: &[u8], base64: bool) -> Message {
    if base64 {
        Message::Text(base64::engine::general_purpose::STANDARD.encode(data))
    } else {
        Message::Binary(data.to_vec())
    }
}

/// Pump a plain-HTTP kubelet log byte stream to the client over a WebSocket,
/// using the wsstream single-stream framing.
///
/// The api-server terminates the WebSocket and reuses the exact same plain-HTTP
/// kubelet `containerLogs` fetch that the non-upgrade path uses — no kubelet
/// change is required. `body` yields the kubelet response body chunks.
///
/// Framing matches upstream `wsstream.NewReader(out, true, ...)`
/// (`responsewriters/writers.go:66`) driven by `messageCopy`
/// (`wsstream/stream.go:142`):
///   * `ping == true`: an initial zero-length message is sent before the stream
///     begins (empty binary frame, or empty text frame for base64). The e2e
///     client (`test/e2e/common/node/pods.go:673`) skips whitespace-only
///     messages, so this is harmless.
///   * each subsequent chunk is framed via [`frame_log_chunk`].
pub async fn handle_logs_websocket<S, E>(mut socket: WebSocket, mut body: S, base64: bool)
where
    S: futures::Stream<Item = std::result::Result<axum::body::Bytes, E>> + Unpin,
{
    // ping == true: send the leading zero-length message.
    if socket.send(frame_log_chunk(&[], base64)).await.is_err() {
        return;
    }

    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(bytes) => {
                if bytes.is_empty() {
                    continue;
                }
                if socket
                    .send(frame_log_chunk(bytes.as_ref(), base64))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Err(_) => break,
        }
    }

    let _ = socket.close().await;
}

/// Prefix a payload with a single channel byte, producing one
/// `channel.k8s.io`/`v4.channel.k8s.io` binary frame. Channel 1 = stdout,
/// 2 = stderr, 3 = error/status. This mirrors the same wire format the exec
/// handler uses for output frames so log / exec / attach speak the same
/// dialect — the client side (client-go `wsstream.Conn`) is shared.
#[inline]
pub fn frame_channel(channel: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + payload.len());
    out.push(channel);
    out.extend_from_slice(payload);
    out
}

/// Handle WebSocket port-forward
pub async fn handle_portforward_websocket(mut socket: WebSocket, pod: Pod, ports: Vec<u16>) {
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpStream;

    let pod_ip = match pod.status.as_ref().and_then(|s| s.pod_ip.as_ref()) {
        Some(ip) => ip.clone(),
        None => {
            let _ = socket.send(Message::Text("Pod has no IP".into())).await;
            let _ = socket.close().await;
            return;
        }
    };

    for port in &ports {
        let target = format!("{}:{}", pod_ip, port);
        match TcpStream::connect(&target).await {
            Ok(tcp) => {
                let (mut tcp_read, _tcp_write) = tcp.into_split();
                // Simple forward: read from TCP, send to WebSocket
                let mut buf = vec![0u8; 8192];
                loop {
                    match tcp_read.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            if socket
                                .send(Message::Binary(buf[..n].to_vec()))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
            Err(e) => {
                let _ = socket
                    .send(Message::Text(format!(
                        "Failed to connect to {}: {}",
                        target, e
                    )))
                    .await;
            }
        }
    }

    let _ = socket.close().await;
}

#[cfg(test)]
mod tests {
    use super::*;

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
