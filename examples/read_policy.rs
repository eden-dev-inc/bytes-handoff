//! Demonstrates configuring prefix handoff policy.

use bytes::BytesMut;
use bytes_handoff::{HandoffBuffer, HandoffBufferConfig, HandoffBufferPolicy};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let policy = HandoffBufferPolicy::new()
        .with_small_prefix_copy_max(0)
        .with_monoio_sparse_read_copy_denominator(2);
    let mut tail = BytesMut::from(&b"route\nremaining tunnel bytes"[..]);
    let capacity = tail.capacity();

    let mut buffer =
        HandoffBuffer::from_tail_with_policy(tail.split(), HandoffBufferConfig::new(1024), policy)?;
    let route = buffer.split_prefix(6)?;

    println!("route: {:?}", String::from_utf8_lossy(&route));
    println!("tail: {:?}", String::from_utf8_lossy(buffer.peek()));
    println!("input capacity before handoff: {capacity}");
    println!("policy: {:?}", buffer.policy());

    Ok(())
}
