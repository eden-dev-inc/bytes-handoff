use bytes::{Buf, Bytes, BytesMut};
use std::future::poll_fn;
use std::pin::Pin;
use tokio::io::{AsyncRead, ReadBuf};

use crate::BufferError;

const SMALL_PREFIX_COPY_MAX: usize = 256;
const SMALL_PREFIX_COPY_REMAINING_MIN: usize = 4 * 1024;

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

#[derive(Debug)]
pub struct HandoffBuffer {
    buf: BytesMut,
    config: HandoffBufferConfig,
}

impl HandoffBuffer {
    pub fn new(max_len: usize) -> Self {
        Self::with_config(HandoffBufferConfig::new(max_len))
    }

    pub fn with_config(config: HandoffBufferConfig) -> Self {
        Self {
            buf: BytesMut::new(),
            config,
        }
    }

    pub fn from_tail(tail: BytesMut, config: HandoffBufferConfig) -> Result<Self, BufferError> {
        if tail.len() > config.max_len {
            return Err(BufferError::LimitExceeded {
                attempted: tail.len(),
                limit: config.max_len,
            });
        }
        Ok(Self { buf: tail, config })
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

    pub async fn read_available<R>(&mut self, reader: &mut R) -> Result<usize, BufferError>
    where
        R: AsyncRead + Unpin,
    {
        let reserve = self.remaining_capacity().min(self.config.read_reserve);
        if reserve == 0 {
            return Err(BufferError::LimitExceeded {
                attempted: self.buf.len() + 1,
                limit: self.config.max_len,
            });
        }
        if self.buf.capacity() - self.buf.len() < reserve {
            self.buf.reserve(reserve);
        }
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

    pub fn split_prefix(&mut self, n: usize) -> Result<Bytes, BufferError> {
        if n > self.buf.len() {
            return Err(BufferError::SplitOutOfBounds {
                requested: n,
                available: self.buf.len(),
            });
        }
        if should_copy_prefix(n, self.buf.len() - n) {
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
}

fn should_copy_prefix(prefix_len: usize, remaining_len: usize) -> bool {
    prefix_len <= SMALL_PREFIX_COPY_MAX && remaining_len >= SMALL_PREFIX_COPY_REMAINING_MIN
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
    fn split_prefix_copies_small_prefix_before_large_tail() {
        let mut buffer = HandoffBuffer::new(8 * 1024);
        buffer.buf.extend_from_slice(b"route\n");
        buffer.buf.extend_from_slice(&vec![b'x'; 4 * 1024]);

        let prefix = buffer.split_prefix(6).expect("split small prefix");

        assert_eq!(prefix, Bytes::from_static(b"route\n"));
        assert_eq!(buffer.len(), 4 * 1024);
    }
}
