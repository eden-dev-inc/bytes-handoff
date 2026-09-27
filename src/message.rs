//! Bounded transfer of owned messages without a runtime dependency.

use std::collections::VecDeque;
use std::fmt;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

/// Limits include queued messages and messages retained by a consumer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MessageHandoffConfig {
    pub max_items: usize,
    pub max_bytes: usize,
}

impl MessageHandoffConfig {
    pub const fn new(max_items: usize, max_bytes: usize) -> Self {
        Self {
            max_items,
            max_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageConfigError {
    ZeroItems,
    ZeroBytes,
}

impl fmt::Display for MessageConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroItems => f.write_str("message item limit must be positive"),
            Self::ZeroBytes => f.write_str("message byte limit must be positive"),
        }
    }
}

impl std::error::Error for MessageConfigError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageBackpressureReason {
    Closed,
    ItemLimitExceeded { limit: usize },
    ByteLimitExceeded { attempted: usize, limit: usize },
}

impl fmt::Display for MessageBackpressureReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("message handoff is closed"),
            Self::ItemLimitExceeded { limit } => write!(f, "message item limit exceeded: {limit}"),
            Self::ByteLimitExceeded { attempted, limit } => {
                write!(
                    f,
                    "message byte limit exceeded: attempted {attempted}, limit {limit}"
                )
            }
        }
    }
}

/// Rejected submissions retain ownership of their value.
pub struct MessageBackpressure<T> {
    reason: MessageBackpressureReason,
    value: T,
}

impl<T> MessageBackpressure<T> {
    pub fn reason(&self) -> MessageBackpressureReason {
        self.reason
    }

    pub fn into_value(self) -> T {
        self.value
    }
}

impl<T> fmt::Debug for MessageBackpressure<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessageBackpressure")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

impl<T> fmt::Display for MessageBackpressure<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.reason.fmt(f)
    }
}

impl<T> std::error::Error for MessageBackpressure<T> {}

/// Constructor namespace for a sender and receiver of owned messages.
pub struct MessageHandoff<T>(std::marker::PhantomData<T>);

pub struct MessageSender<T> {
    shared: Arc<Shared<T>>,
}

pub struct MessageReceiver<T> {
    shared: Arc<Shared<T>>,
}

/// A message whose admission charge remains held through its final wrapper clone.
///
/// This type has no owning extraction method. The caller chooses the declared
/// byte weight at submission; this is a logical budget, not physical memory or
/// transport allocation accounting. If `T` itself exposes clones or other
/// independently owned resources, those escape the wrapper's accounting.
pub struct TrackedMessage<T> {
    inner: Arc<TrackedInner<T>>,
}

struct Shared<T> {
    state: Mutex<State<T>>,
    budget: Arc<Budget>,
}

struct State<T> {
    queue: VecDeque<TrackedMessage<T>>,
    closed: bool,
    senders: usize,
    receivers: usize,
}

struct Budget {
    counts: Mutex<Counts>,
    config: MessageHandoffConfig,
}

#[derive(Default)]
struct Counts {
    items: usize,
    bytes: usize,
}

struct TrackedInner<T> {
    value: T,
    _charge: MessageCharge,
}

struct MessageCharge {
    byte_len: usize,
    budget: Arc<Budget>,
}

impl<T> MessageHandoff<T> {
    // Like a channel constructor, this yields its two independent endpoints.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        config: MessageHandoffConfig,
    ) -> Result<(MessageSender<T>, MessageReceiver<T>), MessageConfigError> {
        if config.max_items == 0 {
            return Err(MessageConfigError::ZeroItems);
        }
        if config.max_bytes == 0 {
            return Err(MessageConfigError::ZeroBytes);
        }
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                closed: false,
                senders: 1,
                receivers: 1,
            }),
            budget: Arc::new(Budget {
                counts: Mutex::new(Counts::default()),
                config,
            }),
        });
        Ok((
            MessageSender {
                shared: Arc::clone(&shared),
            },
            MessageReceiver { shared },
        ))
    }
}

impl<T> MessageSender<T> {
    /// Attempts to transfer ownership, charging one item and `byte_len` bytes.
    /// A rejected transfer returns the original value.
    pub fn try_send(&self, value: T, byte_len: usize) -> Result<(), MessageBackpressure<T>> {
        let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            return Err(MessageBackpressure {
                reason: MessageBackpressureReason::Closed,
                value,
            });
        }
        if let Err(reason) = self.shared.budget.reserve(byte_len) {
            return Err(MessageBackpressure { reason, value });
        }
        state.queue.push_back(TrackedMessage {
            inner: Arc::new(TrackedInner {
                value,
                _charge: MessageCharge {
                    byte_len,
                    budget: Arc::clone(&self.shared.budget),
                },
            }),
        });
        Ok(())
    }

    /// Rejects future submissions from every sender clone.
    pub fn close(&self) {
        self.shared.close();
    }

    pub fn is_closed(&self) -> bool {
        self.shared.is_closed()
    }

    pub fn pending_items(&self) -> usize {
        self.shared.budget.pending_items()
    }

    pub fn pending_bytes(&self) -> usize {
        self.shared.budget.pending_bytes()
    }
}

impl<T> Clone for MessageSender<T> {
    fn clone(&self) -> Self {
        let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        state.senders += 1;
        drop(state);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for MessageSender<T> {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        state.senders -= 1;
        if state.senders == 0 {
            state.closed = true;
        }
    }
}

impl<T> MessageReceiver<T> {
    /// Returns the next message, including after close until the queue empties.
    pub fn try_recv(&self) -> Option<TrackedMessage<T>> {
        self.shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .queue
            .pop_front()
    }

    /// Moves all queued messages into tracked consumer ownership.
    pub fn drain(&self) -> Vec<TrackedMessage<T>> {
        let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        state.queue.drain(..).collect()
    }

    pub fn close(&self) {
        self.shared.close();
    }

    pub fn is_closed(&self) -> bool {
        self.shared.is_closed()
    }

    pub fn pending_items(&self) -> usize {
        self.shared.budget.pending_items()
    }

    pub fn pending_bytes(&self) -> usize {
        self.shared.budget.pending_bytes()
    }
}

impl<T> Clone for MessageReceiver<T> {
    fn clone(&self) -> Self {
        let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        state.receivers += 1;
        drop(state);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for MessageReceiver<T> {
    fn drop(&mut self) {
        let queue = {
            let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
            state.receivers -= 1;
            if state.receivers == 0 {
                state.closed = true;
                std::mem::take(&mut state.queue)
            } else {
                VecDeque::new()
            }
        };
        drop(queue);
    }
}

impl<T> Shared<T> {
    fn close(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).closed = true;
    }

    fn is_closed(&self) -> bool {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).closed
    }
}

impl Budget {
    fn reserve(&self, byte_len: usize) -> Result<(), MessageBackpressureReason> {
        let mut counts = self.counts.lock().unwrap_or_else(|e| e.into_inner());
        if counts.items >= self.config.max_items {
            return Err(MessageBackpressureReason::ItemLimitExceeded {
                limit: self.config.max_items,
            });
        }
        let Some(next_bytes) = counts.bytes.checked_add(byte_len) else {
            return Err(MessageBackpressureReason::ByteLimitExceeded {
                attempted: usize::MAX,
                limit: self.config.max_bytes,
            });
        };
        if next_bytes > self.config.max_bytes {
            return Err(MessageBackpressureReason::ByteLimitExceeded {
                attempted: next_bytes,
                limit: self.config.max_bytes,
            });
        }
        counts.items += 1;
        counts.bytes = next_bytes;
        Ok(())
    }

    fn release(&self, byte_len: usize) {
        let mut counts = self.counts.lock().unwrap_or_else(|e| e.into_inner());
        debug_assert!(counts.items > 0 && counts.bytes >= byte_len);
        counts.items -= 1;
        counts.bytes -= byte_len;
    }

    fn pending_items(&self) -> usize {
        self.counts.lock().unwrap_or_else(|e| e.into_inner()).items
    }

    fn pending_bytes(&self) -> usize {
        self.counts.lock().unwrap_or_else(|e| e.into_inner()).bytes
    }
}

impl Drop for MessageCharge {
    fn drop(&mut self) {
        self.budget.release(self.byte_len);
    }
}

impl<T> Clone for TrackedMessage<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Deref for TrackedMessage<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner.value
    }
}

impl<T: fmt::Debug> fmt::Debug for TrackedMessage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("TrackedMessage")
            .field(&self.inner.value)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    struct Counted(Arc<AtomicUsize>);

    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn rejects_invalid_limits_and_counts_zero_byte_items() {
        assert!(matches!(
            MessageHandoff::<()>::new(MessageHandoffConfig::new(0, 1)),
            Err(MessageConfigError::ZeroItems)
        ));
        assert!(matches!(
            MessageHandoff::<()>::new(MessageHandoffConfig::new(1, 0)),
            Err(MessageConfigError::ZeroBytes)
        ));
        let (tx, rx) = MessageHandoff::new(MessageHandoffConfig::new(1, 1)).expect("valid limits");
        tx.try_send(7, 0).expect("first item accepted");
        assert_eq!(tx.pending_items(), 1);
        assert_eq!(tx.pending_bytes(), 0);
        let rejected = tx.try_send(8, 0).expect_err("item limit enforced");
        assert_eq!(
            rejected.reason(),
            MessageBackpressureReason::ItemLimitExceeded { limit: 1 }
        );
        assert_eq!(rejected.into_value(), 8);
        drop(rx.try_recv());
        assert_eq!(tx.pending_items(), 0);
    }

    #[test]
    fn active_consumers_and_clones_hold_both_limits() {
        let (tx, rx) = MessageHandoff::new(MessageHandoffConfig::new(1, 3)).expect("valid limits");
        tx.try_send("first", 3).expect("first item accepted");
        let active = rx.try_recv().expect("first item received");
        let clone = active.clone();
        assert_eq!(tx.pending_items(), 1);
        assert_eq!(tx.pending_bytes(), 3);
        assert_eq!(
            tx.try_send("second", 1)
                .expect_err("item retained")
                .into_value(),
            "second"
        );
        drop(active);
        assert_eq!(tx.pending_items(), 1);
        drop(clone);
        assert_eq!(tx.pending_items(), 0);
        tx.try_send("second", 1).expect("charge released");
        assert_eq!(*rx.try_recv().expect("second item received"), "second");
    }

    #[test]
    fn close_drains_and_last_receiver_discards_queue() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = MessageHandoff::new(MessageHandoffConfig::new(3, 8)).expect("valid limits");
        tx.try_send(Counted(Arc::clone(&drops)), 2)
            .expect("first item accepted");
        tx.try_send(Counted(Arc::clone(&drops)), 2)
            .expect("second item accepted");
        let other_rx = rx.clone();
        rx.close();
        assert_eq!(
            tx.try_send(Counted(Arc::clone(&drops)), 1)
                .expect_err("closed queue rejects item")
                .reason(),
            MessageBackpressureReason::Closed
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let active = rx.try_recv().expect("first item received");
        drop(rx);
        assert_eq!(tx.pending_items(), 2);
        drop(other_rx);
        assert_eq!(tx.pending_items(), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        drop(active);
        assert_eq!(tx.pending_items(), 0);
        assert_eq!(tx.pending_bytes(), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn concurrent_clones_retain_charge_until_final_drop() {
        let (tx, rx) = MessageHandoff::new(MessageHandoffConfig::new(1, 4)).expect("valid limits");
        tx.try_send([1_u8; 4], 4).expect("item accepted");
        let active = rx.try_recv().expect("item received");
        let other = active.clone();
        thread::scope(|scope| {
            scope.spawn(move || drop(other));
            assert_eq!(tx.pending_bytes(), 4);
        });
        assert_eq!(tx.pending_bytes(), 4);
        drop(active);
        assert_eq!(tx.pending_bytes(), 0);
    }

    #[test]
    fn concurrent_send_close_and_receive_preserve_accounting() {
        let (tx, rx) =
            MessageHandoff::new(MessageHandoffConfig::new(32, 32)).expect("valid limits");
        thread::scope(|scope| {
            for _ in 0..4 {
                let producer = tx.clone();
                scope.spawn(move || {
                    for _ in 0..1000 {
                        let _ = producer.try_send((), 1);
                    }
                });
            }
            let consumer = rx.clone();
            scope.spawn(move || {
                for _ in 0..1000 {
                    drop(consumer.try_recv());
                }
                consumer.close();
            });
        });
        drop(rx.drain());
        assert_eq!(tx.pending_items(), 0);
        assert_eq!(tx.pending_bytes(), 0);
    }

    #[test]
    fn randomized_lifecycle_matches_retained_message_model() {
        let (tx, rx) = MessageHandoff::new(MessageHandoffConfig::new(8, 20)).expect("valid limits");
        let mut seed = 0x5eed_u64;
        let mut next_id = 0usize;
        let mut queued = VecDeque::new();
        let mut active: Vec<TrackedMessage<(usize, usize)>> = Vec::new();
        let mut retained: HashMap<usize, (usize, usize)> = HashMap::new();

        for _ in 0..10_000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            match (seed >> 32) % 4 {
                0 => {
                    let weight = ((seed >> 48) % 6) as usize;
                    if tx.try_send((next_id, weight), weight).is_ok() {
                        queued.push_back(next_id);
                        retained.insert(next_id, (weight, 1));
                    }
                    next_id += 1;
                }
                1 => {
                    let received = rx.try_recv();
                    assert_eq!(received.is_some(), !queued.is_empty());
                    if let Some(message) = received {
                        assert_eq!(message.0, queued.pop_front().expect("modeled queue item"));
                        active.push(message);
                    }
                }
                2 if !active.is_empty() => {
                    let index = (seed as usize) % active.len();
                    let id = active[index].0;
                    retained.get_mut(&id).expect("modeled active item").1 += 1;
                    active.push(active[index].clone());
                }
                3 if !active.is_empty() => {
                    let index = (seed as usize) % active.len();
                    let message = active.swap_remove(index);
                    let id = message.0;
                    drop(message);
                    let entry = retained.get_mut(&id).expect("modeled active item");
                    entry.1 -= 1;
                    if entry.1 == 0 {
                        retained.remove(&id);
                    }
                }
                _ => {}
            }
            assert_eq!(tx.pending_items(), retained.len());
            assert_eq!(
                tx.pending_bytes(),
                retained.values().map(|(weight, _)| weight).sum::<usize>()
            );
        }

        drop(active);
        drop(rx.drain());
        assert_eq!(tx.pending_items(), 0);
        assert_eq!(tx.pending_bytes(), 0);
    }
}
