//! Demonstrates nonblocking owned write submission.
//!
//! Producers hand `Bytes` to a dedicated async writer without borrowing memory
//! until the socket write completes. The returned ticket can be awaited when
//! completion or failure matters, and `flush` can drain prior fire-and-forget
//! writes.

use bytes::Bytes;
use bytes_handoff::{WriteHandoff, WriteHandoffConfig};
use tokio::io::AsyncReadExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (client, mut server) = tokio::io::duplex(128);
    let writer = WriteHandoff::spawn(client, WriteHandoffConfig::default());

    let ticket =
        writer.try_write_with_completion_stats(Bytes::from_static(b"large owned chunk"))?;
    let completion = ticket.wait_completion().await?;
    let stats = completion.stats();
    completion.into_result()?;

    let mut out = vec![0_u8; "large owned chunk".len()];
    server.read_exact(&mut out).await?;
    println!("{}", String::from_utf8(out)?);
    println!("completion stats: {stats:?}");

    writer
        .write_fire_and_forget(Bytes::from_static(b"fire-and-forget"))
        .await?;
    writer.flush().await?;

    Ok(())
}
