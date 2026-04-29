//! Demonstrates incremental parsing of newline-delimited frames.
//!
//! This is useful when a TCP read returns a partial line, multiple lines, or
//! both. `HandoffBuffer` keeps the unconsumed tail and splits complete lines
//! into owned `Bytes` without copying each frame into a new `Vec`.

use bytes_handoff::HandoffBuffer;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (mut client, mut server) = tokio::io::duplex(128);
    let mut buffer = HandoffBuffer::new(1024);

    client.write_all(b"ping\npartial").await?;
    buffer.read_available(&mut server).await?;

    while let Some(newline) = buffer.peek().iter().position(|b| *b == b'\n') {
        let line = buffer.split_prefix(newline + 1)?;
        println!("{}", std::str::from_utf8(&line)?.trim_end());
    }

    client.write_all(b"-line\n").await?;
    buffer.read_available(&mut server).await?;

    while let Some(newline) = buffer.peek().iter().position(|b| *b == b'\n') {
        let line = buffer.split_prefix(newline + 1)?;
        println!("{}", std::str::from_utf8(&line)?.trim_end());
    }

    Ok(())
}
