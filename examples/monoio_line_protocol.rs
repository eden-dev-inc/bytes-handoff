//! Demonstrates `HandoffBuffer` with Monoio's `AsyncReadRent`.
//!
//! Run with:
//!
//! ```bash
//! cargo run --features monoio --example monoio_line_protocol
//! ```

use bytes_handoff::{HandoffBuffer, HandoffBufferConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    monoio::start::<monoio::LegacyDriver, _>(async { run().await })
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut reader: &[u8] = b"ping\npong\npartial";
    let mut buffer =
        HandoffBuffer::with_config(HandoffBufferConfig::new(1024).with_read_reserve(8));

    loop {
        let read = buffer.read_available_monoio(&mut reader).await?;
        drain_lines(&mut buffer)?;
        if read == 0 {
            break;
        }
    }

    assert_eq!(buffer.peek(), b"partial");
    println!("tail={}", std::str::from_utf8(buffer.peek())?);

    Ok(())
}

fn drain_lines(buffer: &mut HandoffBuffer) -> Result<(), Box<dyn std::error::Error>> {
    while let Some(newline) = buffer.peek().iter().position(|b| *b == b'\n') {
        let line = buffer.split_prefix(newline + 1)?;
        println!("{}", std::str::from_utf8(&line)?.trim_end());
    }
    Ok(())
}
