use bytes::{Buf, Bytes, BytesMut};
#[cfg(feature = "monoio")]
use monoio::buf::IoBufMut as _;
#[cfg(feature = "monoio")]
use std::cmp::Ordering;
use std::future::poll_fn;
use std::pin::Pin;
use tokio::io::{AsyncRead, ReadBuf};

use crate::BufferError;

pub const DEFAULT_SMALL_PREFIX_COPY_MAX: usize = 256;
pub const DEFAULT_MONOIO_SPARSE_READ_COPY_DENOMINATOR: usize = 4;

#[derive(Clone, Copy, Debug)]
pub struct HandoffBufferConfig {
    pub max_len: usize,
    pub read_reserve: usize,
}

impl HandoffBufferConfig {
    pub fn new(max_len: usize) -> Self {
        Self {
            max_len,
            read_reserve: 16 * 1024,
        }
    }

    pub fn with_read_reserve(mut self, read_reserve: usize) -> Self {
        self.read_reserve = read_reserve;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HandoffBufferPolicy {
    pub small_prefix_copy_max: usize,
    pub monoio_sparse_read_copy_denominator: usize,
}

impl HandoffBufferPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_small_prefix_copy_max(mut self, max: usize) -> Self {
        self.small_prefix_copy_max = max;
        self
    }

    pub fn with_monoio_sparse_read_copy_denominator(mut self, denominator: usize) -> Self {
        self.monoio_sparse_read_copy_denominator = denominator.max(1);
        self
    }

    fn should_copy_prefix(self, prefix_len: usize) -> bool {
        prefix_len <= self.small_prefix_copy_max
    }

    #[cfg(feature = "monoio")]
    fn should_swap_monoio_read_buffer(self, read: usize, capacity: usize) -> bool {
        read.saturating_mul(self.monoio_sparse_read_copy_denominator) >= capacity
    }
}

impl Default for HandoffBufferPolicy {
    fn default() -> Self {
        Self {
            small_prefix_copy_max: DEFAULT_SMALL_PREFIX_COPY_MAX,
            monoio_sparse_read_copy_denominator: DEFAULT_MONOIO_SPARSE_READ_COPY_DENOMINATOR,
        }
    }
}

#[derive(Debug)]
pub struct HandoffBuffer {
    buf: BytesMut,
    config: HandoffBufferConfig,
    policy: HandoffBufferPolicy,
    #[cfg(feature = "monoio")]
    monoio_read_buf: BytesMut,
}

#[derive(Debug)]
pub struct HandoffDrainCursor<'a> {
    bytes: &'a [u8],
    consumed: usize,
}

impl HandoffDrainCursor<'_> {
    pub fn remaining(&self) -> &[u8] {
        &self.bytes[self.consumed..]
    }

    pub fn consumed(&self) -> usize {
        self.consumed
    }

    pub fn is_empty(&self) -> bool {
        self.remaining().is_empty()
    }

    pub fn consume(&mut self, cnt: usize) -> Result<(), BufferError> {
        let remaining = self.remaining().len();
        if cnt > remaining {
            return Err(BufferError::SplitOutOfBounds {
                requested: cnt,
                available: remaining,
            });
        }
        self.consumed += cnt;
        Ok(())
    }

    pub fn consume_all(&mut self) {
        self.consumed = self.bytes.len();
    }
}

impl HandoffBuffer {
    pub fn new(max_len: usize) -> Self {
        Self::with_config(HandoffBufferConfig::new(max_len))
    }

    pub fn with_config(config: HandoffBufferConfig) -> Self {
        Self::with_config_and_policy(config, HandoffBufferPolicy::default())
    }

    pub fn with_config_and_policy(
        config: HandoffBufferConfig,
        policy: HandoffBufferPolicy,
    ) -> Self {
        Self {
            buf: BytesMut::new(),
            config,
            policy,
            #[cfg(feature = "monoio")]
            monoio_read_buf: BytesMut::with_capacity(config.read_reserve),
        }
    }

    pub fn from_tail(tail: BytesMut, config: HandoffBufferConfig) -> Result<Self, BufferError> {
        Self::from_tail_with_policy(tail, config, HandoffBufferPolicy::default())
    }

    pub fn from_tail_with_policy(
        tail: BytesMut,
        config: HandoffBufferConfig,
        policy: HandoffBufferPolicy,
    ) -> Result<Self, BufferError> {
        if tail.len() > config.max_len {
            return Err(BufferError::LimitExceeded {
                attempted: tail.len(),
                limit: config.max_len,
            });
        }
        Ok(Self {
            buf: tail,
            config,
            policy,
            #[cfg(feature = "monoio")]
            monoio_read_buf: BytesMut::with_capacity(config.read_reserve),
        })
    }

    pub fn policy(&self) -> HandoffBufferPolicy {
        self.policy
    }

    pub fn set_policy(&mut self, policy: HandoffBufferPolicy) {
        self.policy = policy;
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    pub fn peek(&self) -> &[u8] {
        &self.buf
    }

    pub fn reserve_read_capacity(&mut self, additional: usize) -> Result<(), BufferError> {
        self.check_limit(additional)?;
        self.buf.reserve(additional);
        Ok(())
    }

    pub fn drain<F, T, E>(&mut self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut HandoffDrainCursor<'_>) -> Result<T, E>,
        E: From<BufferError>,
    {
        let (consumed, output) = {
            let mut cursor = HandoffDrainCursor {
                bytes: &self.buf,
                consumed: 0,
            };
            let output = f(&mut cursor)?;
            (cursor.consumed(), output)
        };
        self.advance(consumed).map_err(E::from)?;
        Ok(output)
    }

    pub async fn read_and_drain<R, F, T, E>(
        &mut self,
        reader: &mut R,
        f: F,
    ) -> Result<(usize, T), E>
    where
        R: AsyncRead + Unpin,
        F: FnOnce(&mut HandoffDrainCursor<'_>) -> Result<T, E>,
        E: From<BufferError>,
    {
        let read = self.read_available(reader).await.map_err(E::from)?;
        let output = self.drain(f)?;
        Ok((read, output))
    }

    pub async fn read_available<R>(&mut self, reader: &mut R) -> Result<usize, BufferError>
    where
        R: AsyncRead + Unpin,
    {
        let reserve = self.read_reserve()?;
        self.reserve_spare_capacity(reserve);
        let len = self.buf.len();
        let read = poll_fn(|cx| {
            let spare = &mut self.buf.spare_capacity_mut()[..reserve];
            let mut read_buf = ReadBuf::uninit(spare);
            match Pin::new(&mut *reader).poll_read(cx, &mut read_buf) {
                std::task::Poll::Ready(Ok(())) => {
                    std::task::Poll::Ready(Ok(read_buf.filled().len()))
                }
                std::task::Poll::Ready(Err(err)) => std::task::Poll::Ready(Err(err)),
                std::task::Poll::Pending => std::task::Poll::Pending,
            }
        })
        .await?;
        // SAFETY: `poll_read` initialized exactly `read` bytes in the spare
        // capacity exposed through `ReadBuf`.
        unsafe {
            self.buf.set_len(len + read);
        }
        Ok(read)
    }

    #[cfg(feature = "monoio")]
    pub async fn read_available_monoio<R>(&mut self, reader: &mut R) -> Result<usize, BufferError>
    where
        R: monoio::io::AsyncReadRent + ?Sized,
    {
        let reserve = self.read_reserve()?;
        let read_buf = self.take_monoio_read_buffer(reserve);
        let read_slice = read_buf.slice_mut(..reserve);
        let (result, read_slice) = reader.read(read_slice).await;
        let read = result?;
        let mut read_buf = read_slice.into_inner();
        self.check_monoio_read_len(read, reserve)?;
        normalize_monoio_read_buffer(&mut read_buf, read);
        self.store_monoio_read(read_buf, read);
        Ok(read)
    }

    #[cfg(feature = "monoio")]
    pub async fn read_and_drain_monoio<R, F, T, E>(
        &mut self,
        reader: &mut R,
        f: F,
    ) -> Result<(usize, T), E>
    where
        R: monoio::io::AsyncReadRent + ?Sized,
        F: FnOnce(&mut HandoffDrainCursor<'_>) -> Result<T, E>,
        E: From<BufferError>,
    {
        let read = self.read_available_monoio(reader).await.map_err(E::from)?;
        let output = self.drain(f)?;
        Ok((read, output))
    }

    pub fn split_prefix(&mut self, n: usize) -> Result<Bytes, BufferError> {
        if n > self.buf.len() {
            return Err(BufferError::SplitOutOfBounds {
                requested: n,
                available: self.buf.len(),
            });
        }
        if self.policy.should_copy_prefix(n) {
            let prefix = Bytes::copy_from_slice(&self.buf[..n]);
            self.buf.advance(n);
            return Ok(prefix);
        }
        Ok(self.buf.split_to(n).freeze())
    }

    pub fn split_prefix_mut(&mut self, n: usize) -> Result<BytesMut, BufferError> {
        if n > self.buf.len() {
            return Err(BufferError::SplitOutOfBounds {
                requested: n,
                available: self.buf.len(),
            });
        }
        Ok(self.buf.split_to(n))
    }

    pub fn freeze_all(&mut self) -> Bytes {
        if self.policy.should_copy_prefix(self.buf.len()) {
            let bytes = Bytes::copy_from_slice(&self.buf);
            self.buf.clear();
            return bytes;
        }
        self.buf.split().freeze()
    }

    pub fn take_tail(&mut self) -> BytesMut {
        self.buf.split()
    }

    pub fn advance(&mut self, cnt: usize) -> Result<(), BufferError> {
        if cnt > self.buf.len() {
            return Err(BufferError::SplitOutOfBounds {
                requested: cnt,
                available: self.buf.len(),
            });
        }
        self.buf.advance(cnt);
        Ok(())
    }

    fn remaining_capacity(&self) -> usize {
        self.config.max_len.saturating_sub(self.buf.len())
    }

    fn read_reserve(&self) -> Result<usize, BufferError> {
        match self.remaining_capacity().min(self.config.read_reserve) {
            0 => Err(BufferError::LimitExceeded {
                attempted: self.buf.len().saturating_add(1),
                limit: self.config.max_len,
            }),
            reserve => Ok(reserve),
        }
    }

    fn reserve_spare_capacity(&mut self, reserve: usize) {
        let spare = self.buf.capacity().saturating_sub(self.buf.len());
        if spare < reserve {
            self.buf.reserve(reserve);
        }
    }

    fn check_limit(&self, additional: usize) -> Result<(), BufferError> {
        let attempted = self.buf.len().saturating_add(additional);
        if attempted > self.config.max_len {
            return Err(BufferError::LimitExceeded {
                attempted,
                limit: self.config.max_len,
            });
        }
        Ok(())
    }

    #[cfg(feature = "monoio")]
    fn take_monoio_read_buffer(&mut self, reserve: usize) -> BytesMut {
        let mut read_buf = std::mem::take(&mut self.monoio_read_buf);
        read_buf.clear();
        if read_buf.capacity() < reserve {
            read_buf.reserve(reserve);
        }
        read_buf
    }

    #[cfg(feature = "monoio")]
    fn check_monoio_read_len(&self, read: usize, reserve: usize) -> Result<(), BufferError> {
        match read.cmp(&reserve) {
            Ordering::Less | Ordering::Equal => Ok(()),
            Ordering::Greater => Err(BufferError::LimitExceeded {
                attempted: self.buf.len().saturating_add(read),
                limit: self.config.max_len,
            }),
        }
    }

    #[cfg(feature = "monoio")]
    fn store_monoio_read(&mut self, mut read_buf: BytesMut, read: usize) {
        match self.monoio_read_destination(read, read_buf.capacity()) {
            MonoioReadDestination::Empty => {}
            MonoioReadDestination::Swap => {
                std::mem::swap(&mut self.buf, &mut read_buf);
            }
            MonoioReadDestination::Append => {
                self.buf.extend_from_slice(&read_buf[..read]);
            }
        }
        read_buf.clear();
        self.monoio_read_buf = read_buf;
    }

    #[cfg(feature = "monoio")]
    fn monoio_read_destination(&self, read: usize, capacity: usize) -> MonoioReadDestination {
        match read {
            0 => MonoioReadDestination::Empty,
            _ if self.buf.is_empty()
                && read > self.policy.small_prefix_copy_max
                && self.policy.should_swap_monoio_read_buffer(read, capacity) =>
            {
                MonoioReadDestination::Swap
            }
            _ => MonoioReadDestination::Append,
        }
    }
}

#[cfg(feature = "monoio")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MonoioReadDestination {
    Empty,
    Swap,
    Append,
}

#[cfg(feature = "monoio")]
fn normalize_monoio_read_buffer(read_buf: &mut BytesMut, read: usize) {
    match read_buf.len().cmp(&read) {
        Ordering::Greater => read_buf.truncate(read),
        Ordering::Equal => {}
        Ordering::Less => {
            // SAFETY: a successful monoio read of `read` bytes means that the
            // returned buffer has that many initialized bytes.
            unsafe {
                read_buf.set_init(read);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn reads_incrementally_and_preserves_tail() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let mut buffer = HandoffBuffer::new(128);

        client
            .write_all(b"hello\npar")
            .await
            .expect("write to duplex");
        assert_eq!(
            buffer
                .read_available(&mut server)
                .await
                .expect("read first chunk"),
            9
        );

        let newline = buffer
            .peek()
            .iter()
            .position(|b| *b == b'\n')
            .expect("newline present");
        let frame = buffer.split_prefix(newline + 1).expect("split frame");
        assert_eq!(frame, Bytes::from_static(b"hello\n"));
        assert_eq!(buffer.peek(), b"par");

        client
            .write_all(b"tial\n")
            .await
            .expect("write second chunk");
        assert_eq!(
            buffer
                .read_available(&mut server)
                .await
                .expect("read second chunk"),
            5
        );
        assert_eq!(buffer.freeze_all(), Bytes::from_static(b"partial\n"));
    }

    #[tokio::test]
    async fn enforces_buffer_limit_before_reading_more() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let mut buffer =
            HandoffBuffer::with_config(HandoffBufferConfig::new(4).with_read_reserve(4));

        client.write_all(b"abcd").await.expect("write within limit");
        assert_eq!(
            buffer
                .read_available(&mut server)
                .await
                .expect("read within limit"),
            4
        );

        let err = buffer
            .read_available(&mut server)
            .await
            .expect_err("buffer is full");
        assert!(matches!(
            err,
            BufferError::LimitExceeded {
                attempted: 5,
                limit: 4
            }
        ));
    }

    #[cfg(feature = "monoio")]
    #[test]
    fn reads_from_monoio_reader_and_preserves_tail() {
        monoio::start::<monoio::LegacyDriver, _>(async {
            let mut reader: &[u8] = b"hello\npartial";
            let mut buffer = HandoffBuffer::new(128);

            assert_eq!(
                buffer
                    .read_available_monoio(&mut reader)
                    .await
                    .expect("read monoio chunk"),
                13
            );

            let newline = buffer
                .peek()
                .iter()
                .position(|b| *b == b'\n')
                .expect("newline present");
            let frame = buffer.split_prefix(newline + 1).expect("split frame");

            assert_eq!(frame, Bytes::from_static(b"hello\n"));
            assert_eq!(buffer.peek(), b"partial");
        });
    }

    #[cfg(feature = "monoio")]
    #[test]
    fn monoio_sparse_reads_copy_instead_of_swapping_large_reserve() {
        let policy = HandoffBufferPolicy::default();
        assert!(!policy.should_swap_monoio_read_buffer(1460, 16 * 1024));
        assert!(policy.should_swap_monoio_read_buffer(16 * 1024, 16 * 1024));
        assert!(policy.should_swap_monoio_read_buffer(4 * 1024, 16 * 1024));

        let more_conservative = policy.with_monoio_sparse_read_copy_denominator(2);
        assert!(!more_conservative.should_swap_monoio_read_buffer(4 * 1024, 16 * 1024));
        assert!(more_conservative.should_swap_monoio_read_buffer(8 * 1024, 16 * 1024));
    }

    #[test]
    fn take_tail_moves_buffered_state() {
        let mut buffer = HandoffBuffer::new(64);
        buffer.buf.extend_from_slice(b"stateful bytes");

        let tail = buffer.take_tail();
        assert!(buffer.is_empty());
        assert_eq!(&tail[..], b"stateful bytes");

        let inherited =
            HandoffBuffer::from_tail(tail, HandoffBufferConfig::new(64)).expect("tail fits");
        assert_eq!(inherited.peek(), b"stateful bytes");
    }

    #[test]
    fn split_prefix_checks_bounds() {
        let mut buffer = HandoffBuffer::new(64);
        buffer.buf.extend_from_slice(b"abc");

        let err = buffer.split_prefix(4).expect_err("prefix too large");
        assert!(matches!(
            err,
            BufferError::SplitOutOfBounds {
                requested: 4,
                available: 3
            }
        ));
    }

    #[test]
    fn split_prefix_mut_returns_mutable_bytes_without_freezing() {
        let mut buffer = HandoffBuffer::new(64);
        buffer.buf.extend_from_slice(b"abcdef");

        let mut prefix = buffer.split_prefix_mut(3).expect("split prefix");
        prefix[0] = b'X';

        assert_eq!(&prefix[..], b"Xbc");
        assert_eq!(buffer.peek(), b"def");
    }

    #[test]
    fn drain_cursor_commits_consumed_bytes_once() {
        let mut buffer = HandoffBuffer::new(64);
        buffer.buf.extend_from_slice(b"one\ntwo\npartial");

        let frames = buffer
            .drain(|cursor| {
                let mut frames = 0;
                while let Some(newline) = cursor.remaining().iter().position(|b| *b == b'\n') {
                    cursor.consume(newline + 1)?;
                    frames += 1;
                }
                Ok::<_, BufferError>(frames)
            })
            .expect("drain complete frames");

        assert_eq!(frames, 2);
        assert_eq!(buffer.peek(), b"partial");
    }

    #[test]
    fn drain_cursor_does_not_commit_on_error() {
        let mut buffer = HandoffBuffer::new(64);
        buffer.buf.extend_from_slice(b"one\npartial");

        let err = buffer
            .drain(|cursor| {
                cursor.consume(4)?;
                Err::<(), _>(BufferError::SplitOutOfBounds {
                    requested: 100,
                    available: cursor.remaining().len(),
                })
            })
            .expect_err("drain should fail");

        assert!(matches!(err, BufferError::SplitOutOfBounds { .. }));
        assert_eq!(buffer.peek(), b"one\npartial");
    }

    #[tokio::test]
    async fn read_and_drain_reads_once_and_preserves_tail() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let mut buffer = HandoffBuffer::new(128);

        client
            .write_all(b"one\ntwo\npartial")
            .await
            .expect("write frames");
        let (read, frames) = buffer
            .read_and_drain(&mut server, |cursor| {
                let mut frames = 0;
                while let Some(newline) = cursor.remaining().iter().position(|b| *b == b'\n') {
                    cursor.consume(newline + 1)?;
                    frames += 1;
                }
                Ok::<_, BufferError>(frames)
            })
            .await
            .expect("read and drain");

        assert_eq!(read, 15);
        assert_eq!(frames, 2);
        assert_eq!(buffer.peek(), b"partial");
    }

    #[cfg(feature = "monoio")]
    #[test]
    fn read_and_drain_monoio_reads_once_and_preserves_tail() {
        monoio::start::<monoio::LegacyDriver, _>(async {
            let mut reader: &[u8] = b"one\ntwo\npartial";
            let mut buffer = HandoffBuffer::new(128);

            let (read, frames) = buffer
                .read_and_drain_monoio(&mut reader, |cursor| {
                    let mut frames = 0;
                    while let Some(newline) = cursor.remaining().iter().position(|b| *b == b'\n') {
                        cursor.consume(newline + 1)?;
                        frames += 1;
                    }
                    Ok::<_, BufferError>(frames)
                })
                .await
                .expect("read and drain monoio");

            assert_eq!(read, 15);
            assert_eq!(frames, 2);
            assert_eq!(buffer.peek(), b"partial");
        });
    }

    #[test]
    fn split_prefix_copies_small_prefix_before_large_tail() {
        let mut buffer = HandoffBuffer::new(8 * 1024);
        buffer.buf.extend_from_slice(b"route\n");
        buffer.buf.extend_from_slice(&vec![b'x'; 4 * 1024]);

        let prefix = buffer.split_prefix(6).expect("split small prefix");

        assert_eq!(prefix, Bytes::from_static(b"route\n"));
        assert_eq!(buffer.len(), 4 * 1024);
    }

    #[test]
    fn split_prefix_policy_can_disable_small_prefix_copy() {
        let policy = HandoffBufferPolicy::new().with_small_prefix_copy_max(0);
        let mut buffer =
            HandoffBuffer::with_config_and_policy(HandoffBufferConfig::new(64), policy);
        buffer.buf.extend_from_slice(b"route\n");

        let prefix = buffer.split_prefix(6).expect("split prefix");

        assert_eq!(prefix, Bytes::from_static(b"route\n"));
        assert_eq!(buffer.policy(), policy);
        assert!(buffer.is_empty());
    }

    #[test]
    fn split_prefix_copies_small_complete_buffer_and_reuses_capacity() {
        let mut buffer = HandoffBuffer::new(16 * 1024);
        buffer.reserve_read_capacity(16 * 1024).expect("reserve");
        let capacity = buffer.capacity();
        buffer.buf.extend_from_slice(b"route\n");

        let prefix = buffer.split_prefix(6).expect("split small prefix");

        assert_eq!(prefix, Bytes::from_static(b"route\n"));
        assert!(buffer.is_empty());
        assert_eq!(buffer.capacity(), capacity - 6);
    }

    #[test]
    fn freeze_all_copies_small_buffer_and_reuses_capacity() {
        let mut buffer = HandoffBuffer::new(16 * 1024);
        buffer.reserve_read_capacity(16 * 1024).expect("reserve");
        let capacity = buffer.capacity();
        buffer.buf.extend_from_slice(b"tiny tunnel chunk");

        let bytes = buffer.freeze_all();

        assert_eq!(bytes, Bytes::from_static(b"tiny tunnel chunk"));
        assert!(buffer.is_empty());
        assert_eq!(buffer.capacity(), capacity);
    }
}
