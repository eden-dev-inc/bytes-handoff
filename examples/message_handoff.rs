use bytes_handoff::{MessageHandoff, MessageHandoffConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (sender, receiver) = MessageHandoff::new(MessageHandoffConfig::new(8, 64 * 1024))?;

    let request = String::from("GET /health");
    sender.try_send(request, "GET /health".len())?;

    let active = receiver.try_recv().expect("accepted message");
    assert_eq!(&**active, "GET /health");
    assert_eq!(receiver.pending_items(), 1);

    sender.close();
    drop(active);
    assert_eq!(receiver.pending_items(), 0);
    Ok(())
}
