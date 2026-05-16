//! Demonstrates reading once, inspecting the buffered stream, and committing
//! only the bytes accepted by the parser.

use bytes_handoff::{BufferError, HandoffBuffer};
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (mut client, mut server) = tokio::io::duplex(128);
    let mut buffer = HandoffBuffer::new(1024);

    client
        .write_all(b"route-a\nroute-b\npartial")
        .await
        .expect("write input");

    let (read, frames) = buffer
        .read_and_drain(&mut server, |cursor| {
            let mut frames = Vec::new();
            while let Some(newline) = cursor.remaining().iter().position(|b| *b == b'\n') {
                let frame = &cursor.remaining()[..newline + 1];
                frames.push(String::from_utf8_lossy(frame).trim_end().to_owned());
                cursor.consume(newline + 1)?;
            }
            Ok::<_, BufferError>(frames)
        })
        .await?;

    println!("read {read} bytes");
    println!("frames: {frames:?}");
    println!("tail: {:?}", String::from_utf8_lossy(buffer.peek()));

    Ok(())
}
