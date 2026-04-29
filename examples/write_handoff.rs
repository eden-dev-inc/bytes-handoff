//! Demonstrates nonblocking owned write submission.
//!
//! Producers hand `Bytes` to a dedicated async writer without borrowing memory
//! until the socket write completes. The returned ticket can be awaited when
//! completion or failure matters.

use bytes::Bytes;
use bytes_handoff::{WriteHandoff, WriteHandoffConfig};
use tokio::io::AsyncReadExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (client, mut server) = tokio::io::duplex(128);
    let writer = WriteHandoff::spawn(client, WriteHandoffConfig::new(16, 1024));

    let ticket = writer.try_write(Bytes::from_static(b"large owned chunk"))?;
    ticket.wait().await?;

    let mut out = vec![0_u8; "large owned chunk".len()];
    server.read_exact(&mut out).await?;
    println!("{}", String::from_utf8(out)?);

    Ok(())
}
