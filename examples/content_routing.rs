//! Demonstrates content-routed stream handling.
//!
//! The example peeks at buffered bytes to route complete "safe" prefixes, waits
//! for more bytes when the routing token is incomplete, then switches to a raw
//! tunnel with `take_tail()` so already-read bytes are not lost.

use bytes::Bytes;
use bytes_handoff::{HandoffBuffer, HandoffBufferConfig};
use tokio::io::AsyncWriteExt;

#[derive(Debug, PartialEq, Eq)]
enum RouteDecision {
    NeedMore,
    RoutePrefix(usize),
    SwitchToRawTunnel,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (mut client, mut server) = tokio::io::duplex(256);
    let mut buffer =
        HandoffBuffer::with_config(HandoffBufferConfig::new(1024).with_read_reserve(64));

    client.write_all(b"GET users\nGET accounts\nTUN").await?;
    buffer.read_available(&mut server).await?;

    let mut routed = Vec::new();
    loop {
        match classify(buffer.peek()) {
            RouteDecision::RoutePrefix(n) => {
                let frame = buffer.split_prefix(n)?;
                route_to_fast_path(frame, &mut routed);
            }
            RouteDecision::SwitchToRawTunnel => {
                let tail = buffer.take_tail();
                start_raw_tunnel(tail.freeze(), &mut routed);
                break;
            }
            RouteDecision::NeedMore => {
                client
                    .write_all(b"NEL opaque bytes that belong to tunnel")
                    .await?;
                buffer.read_available(&mut server).await?;
            }
        }
    }

    assert_eq!(
        routed,
        vec![
            "fast:GET users",
            "fast:GET accounts",
            "raw:TUNNEL opaque bytes that belong to tunnel",
        ]
    );

    Ok(())
}

fn classify(bytes: &[u8]) -> RouteDecision {
    if bytes.starts_with(b"TUNNEL") {
        return RouteDecision::SwitchToRawTunnel;
    }
    if bytes.starts_with(b"TUN") {
        return RouteDecision::NeedMore;
    }
    if let Some(newline) = bytes.iter().position(|b| *b == b'\n') {
        return RouteDecision::RoutePrefix(newline + 1);
    }
    RouteDecision::NeedMore
}

fn route_to_fast_path(frame: Bytes, routed: &mut Vec<String>) {
    let text = std::str::from_utf8(&frame)
        .expect("example uses utf8 frames")
        .trim_end();
    routed.push(format!("fast:{text}"));
}

fn start_raw_tunnel(tail: Bytes, routed: &mut Vec<String>) {
    let text = std::str::from_utf8(&tail).expect("example uses utf8 bytes");
    routed.push(format!("raw:{text}"));
}
