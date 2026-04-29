//! Demonstrates parsing length-prefixed frames without consuming early.
//!
//! The example reads a frame header before the full payload has arrived,
//! inspects the buffered bytes, waits for more input, and only advances once a
//! complete frame is available.

use bytes::Buf;
use bytes_handoff::HandoffBuffer;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (mut client, mut server) = tokio::io::duplex(128);
    let mut buffer = HandoffBuffer::new(1024);

    client.write_all(&[0, 0, 0, 5, b'h', b'e']).await?;
    buffer.read_available(&mut server).await?;
    assert!(next_frame_len(buffer.peek()).is_none());

    client.write_all(b"llo").await?;
    buffer.read_available(&mut server).await?;

    if let Some(frame_len) = next_frame_len(buffer.peek()) {
        buffer.advance(4)?;
        let payload = buffer.split_prefix(frame_len)?;
        println!("{}", std::str::from_utf8(&payload)?);
    }

    Ok(())
}

fn next_frame_len(bytes: &[u8]) -> Option<usize> {
    if bytes.len() < 4 {
        return None;
    }
    let len = (&bytes[..4]).get_u32() as usize;
    (bytes.len() >= 4 + len).then_some(len)
}
