use bytes::Bytes;
use std::io::{self, IoSlice};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::{Notify, mpsc, oneshot};

use crate::{WriteBackpressure, WriteError};

const MAX_BATCH_ITEMS: usize = 64;
const MAX_BATCH_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub struct WriteHandoffConfig {
    pub max_items: usize,
    pub max_pending_bytes: usize,
}

impl WriteHandoffConfig {
    pub fn new(max_items: usize, max_pending_bytes: usize) -> Self {
        Self {
            max_items,
            max_pending_bytes,
        }
    }
}

pub struct WriteHandoff {
    tx: mpsc::Sender<WriteMessage>,
    budget: Arc<Budget>,
    closed: Arc<AtomicBool>,
}

#[derive(Debug)]
pub struct WriteTicket {
    rx: oneshot::Receiver<WriteCompletion>,
}

#[derive(Debug)]
pub struct WriteCompletion {
    result: Result<(), WriteError>,
}

struct WriteRequest {
    bytes: Bytes,
    completion: Option<oneshot::Sender<WriteCompletion>>,
    budget_bytes: usize,
}

enum WriteMessage {
    Write(WriteRequest),
    Shutdown,
}

struct Budget {
    pending: AtomicUsize,
    closed: AtomicBool,
    notify: Notify,
    limit: usize,
}

#[derive(Debug)]
enum BudgetAcquireError {
    Closed,
    LimitExceeded { attempted: usize, limit: usize },
}

impl WriteHandoff {
    pub fn spawn<W>(writer: W, config: WriteHandoffConfig) -> Self
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (tx, rx) = mpsc::channel(config.max_items);
        let budget = Arc::new(Budget::new(config.max_pending_bytes));
        let closed = Arc::new(AtomicBool::new(false));
        tokio::spawn(writer_loop(writer, rx, closed.clone(), budget.clone()));

        Self { tx, budget, closed }
    }

    pub fn try_write(&self, bytes: Bytes) -> Result<WriteTicket, WriteBackpressure> {
        if self.closed.load(Ordering::Acquire) {
            return Err(WriteBackpressure::closed(bytes));
        }
        let permit = match self.budget.try_acquire(bytes.len()) {
            Ok(permit) => permit,
            Err(BudgetAcquireError::Closed) => return Err(WriteBackpressure::closed(bytes)),
            Err(BudgetAcquireError::LimitExceeded { attempted, limit }) => {
                return Err(WriteBackpressure::byte_budget_exceeded(
                    bytes, attempted, limit,
                ));
            }
        };
        if self.closed.load(Ordering::Acquire) {
            self.budget.release(permit);
            return Err(WriteBackpressure::closed(bytes));
        }

        let (completion, rx) = oneshot::channel();
        let request = WriteRequest {
            bytes,
            completion: Some(completion),
            budget_bytes: permit,
        };
        match self.tx.try_send(WriteMessage::Write(request)) {
            Ok(()) => Ok(WriteTicket { rx }),
            Err(mpsc::error::TrySendError::Full(WriteMessage::Write(mut request))) => {
                self.budget.release(request.budget_bytes);
                request.budget_bytes = 0;
                Err(WriteBackpressure::queue_full(request.bytes))
            }
            Err(mpsc::error::TrySendError::Closed(WriteMessage::Write(mut request))) => {
                self.budget.release(request.budget_bytes);
                request.budget_bytes = 0;
                self.closed.store(true, Ordering::Release);
                self.budget.close();
                Err(WriteBackpressure::closed(request.bytes))
            }
            Err(mpsc::error::TrySendError::Full(WriteMessage::Shutdown))
            | Err(mpsc::error::TrySendError::Closed(WriteMessage::Shutdown)) => unreachable!(),
        }
    }

    pub fn try_write_fire_and_forget(&self, bytes: Bytes) -> Result<(), WriteBackpressure> {
        if self.closed.load(Ordering::Acquire) {
            return Err(WriteBackpressure::closed(bytes));
        }
        let permit = match self.budget.try_acquire(bytes.len()) {
            Ok(permit) => permit,
            Err(BudgetAcquireError::Closed) => return Err(WriteBackpressure::closed(bytes)),
            Err(BudgetAcquireError::LimitExceeded { attempted, limit }) => {
                return Err(WriteBackpressure::byte_budget_exceeded(
                    bytes, attempted, limit,
                ));
            }
        };
        if self.closed.load(Ordering::Acquire) {
            self.budget.release(permit);
            return Err(WriteBackpressure::closed(bytes));
        }

        let request = WriteRequest {
            bytes,
            completion: None,
            budget_bytes: permit,
        };
        match self.tx.try_send(WriteMessage::Write(request)) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(WriteMessage::Write(mut request))) => {
                self.budget.release(request.budget_bytes);
                request.budget_bytes = 0;
                Err(WriteBackpressure::queue_full(request.bytes))
            }
            Err(mpsc::error::TrySendError::Closed(WriteMessage::Write(mut request))) => {
                self.budget.release(request.budget_bytes);
                request.budget_bytes = 0;
                self.closed.store(true, Ordering::Release);
                self.budget.close();
                Err(WriteBackpressure::closed(request.bytes))
            }
            Err(mpsc::error::TrySendError::Full(WriteMessage::Shutdown))
            | Err(mpsc::error::TrySendError::Closed(WriteMessage::Shutdown)) => unreachable!(),
        }
    }

    pub async fn write(&self, bytes: Bytes) -> Result<WriteTicket, WriteError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(WriteError::Closed);
        }
        let permit = match self.budget.acquire(bytes.len()).await {
            Ok(permit) => permit,
            Err(BudgetAcquireError::Closed) => return Err(WriteError::Closed),
            Err(BudgetAcquireError::LimitExceeded { attempted, limit }) => {
                return Err(WriteError::ByteBudgetExceeded { attempted, limit });
            }
        };
        if self.closed.load(Ordering::Acquire) {
            self.budget.release(permit);
            return Err(WriteError::Closed);
        }

        let (completion, rx) = oneshot::channel();
        let request = WriteRequest {
            bytes,
            completion: Some(completion),
            budget_bytes: permit,
        };
        if let Err(err) = self.tx.send(WriteMessage::Write(request)).await {
            let WriteMessage::Write(mut request) = err.0 else {
                unreachable!();
            };
            self.budget.release(request.budget_bytes);
            request.budget_bytes = 0;
            self.closed.store(true, Ordering::Release);
            self.budget.close();
            return Err(WriteError::Closed);
        }
        Ok(WriteTicket { rx })
    }

    pub async fn write_fire_and_forget(&self, bytes: Bytes) -> Result<(), WriteError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(WriteError::Closed);
        }
        let permit = match self.budget.acquire(bytes.len()).await {
            Ok(permit) => permit,
            Err(BudgetAcquireError::Closed) => return Err(WriteError::Closed),
            Err(BudgetAcquireError::LimitExceeded { attempted, limit }) => {
                return Err(WriteError::ByteBudgetExceeded { attempted, limit });
            }
        };
        if self.closed.load(Ordering::Acquire) {
            self.budget.release(permit);
            return Err(WriteError::Closed);
        }

        let request = WriteRequest {
            bytes,
            completion: None,
            budget_bytes: permit,
        };
        if let Err(err) = self.tx.send(WriteMessage::Write(request)).await {
            let WriteMessage::Write(mut request) = err.0 else {
                unreachable!();
            };
            self.budget.release(request.budget_bytes);
            request.budget_bytes = 0;
            self.closed.store(true, Ordering::Release);
            self.budget.close();
            return Err(WriteError::Closed);
        }
        Ok(())
    }

    pub fn pending_bytes(&self) -> usize {
        self.budget.pending()
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.budget.close();
        let _ = self.tx.try_send(WriteMessage::Shutdown);
    }
}

impl Clone for WriteHandoff {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            budget: self.budget.clone(),
            closed: self.closed.clone(),
        }
    }
}

impl WriteTicket {
    pub async fn wait(self) -> Result<(), WriteError> {
        match self.rx.await {
            Ok(completion) => completion.result,
            Err(_) => Err(WriteError::Closed),
        }
    }
}

impl Budget {
    fn new(limit: usize) -> Self {
        Self {
            pending: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            notify: Notify::new(),
            limit,
        }
    }

    fn try_acquire(&self, bytes: usize) -> Result<usize, BudgetAcquireError> {
        if bytes > self.limit {
            return Err(BudgetAcquireError::LimitExceeded {
                attempted: self.pending().saturating_add(bytes),
                limit: self.limit,
            });
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(BudgetAcquireError::Closed);
        }

        let mut current = self.pending.load(Ordering::Relaxed);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return Err(BudgetAcquireError::LimitExceeded {
                    attempted: usize::MAX,
                    limit: self.limit,
                });
            };
            if next > self.limit {
                return Err(BudgetAcquireError::LimitExceeded {
                    attempted: next,
                    limit: self.limit,
                });
            }
            match self.pending.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(bytes),
                Err(actual) => {
                    current = actual;
                    if self.closed.load(Ordering::Acquire) {
                        return Err(BudgetAcquireError::Closed);
                    }
                }
            }
        }
    }

    async fn acquire(&self, bytes: usize) -> Result<usize, BudgetAcquireError> {
        if bytes > self.limit {
            return Err(BudgetAcquireError::LimitExceeded {
                attempted: self.pending().saturating_add(bytes),
                limit: self.limit,
            });
        }

        loop {
            let notified = self.notify.notified();
            match self.try_acquire(bytes) {
                Ok(acquired) => return Ok(acquired),
                Err(BudgetAcquireError::Closed) => return Err(BudgetAcquireError::Closed),
                Err(BudgetAcquireError::LimitExceeded { .. }) => {}
            }
            notified.await;
        }
    }

    fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let previous = self.pending.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(previous >= bytes, "released more bytes than acquired");
        self.notify.notify_waiters();
    }

    fn pending(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

async fn writer_loop<W>(
    mut writer: W,
    mut rx: mpsc::Receiver<WriteMessage>,
    closed: Arc<AtomicBool>,
    budget: Arc<Budget>,
) where
    W: AsyncWrite + Unpin,
{
    let mut messages = Vec::with_capacity(MAX_BATCH_ITEMS);
    let mut requests = Vec::with_capacity(MAX_BATCH_ITEMS);

    loop {
        messages.clear();
        let received = rx.recv_many(&mut messages, MAX_BATCH_ITEMS).await;
        if received == 0 {
            break;
        }

        let mut shutdown = false;
        for message in messages.drain(..) {
            match message {
                WriteMessage::Write(request) if !shutdown => requests.push(request),
                WriteMessage::Write(mut request) => {
                    complete_request(&budget, &mut request, Err(WriteError::Closed));
                }
                WriteMessage::Shutdown => shutdown = true,
            }
        }

        if write_request_batches(&mut writer, &budget, &mut requests)
            .await
            .is_err()
        {
            closed.store(true, Ordering::Release);
            budget.close();
            drain_closed(&budget, &mut rx);
            return;
        }
        requests.clear();

        if shutdown || (closed.load(Ordering::Acquire) && rx.is_empty()) {
            break;
        }
    }
    closed.store(true, Ordering::Release);
    budget.close();
    drain_closed(&budget, &mut rx);
}

async fn write_request_batches<W>(
    writer: &mut W,
    budget: &Budget,
    requests: &mut [WriteRequest],
) -> Result<(), ()>
where
    W: AsyncWrite + Unpin,
{
    let mut start = 0;
    while start < requests.len() {
        let end = batch_end(requests, start);
        match write_batch(writer, &requests[start..end]).await {
            Ok(written) => {
                debug_assert_eq!(written, end - start);
                complete_ok(budget, &mut requests[start..end]);
            }
            Err((written, err)) => {
                complete_ok(budget, &mut requests[start..start + written]);
                if start + written < end {
                    complete_request(
                        budget,
                        &mut requests[start + written],
                        Err(WriteError::Io(err)),
                    );
                }
                if start + written + 1 < requests.len() {
                    complete_closed(budget, &mut requests[start + written + 1..]);
                }
                return Err(());
            }
        }
        start = end;
    }

    Ok(())
}

fn batch_end(requests: &[WriteRequest], start: usize) -> usize {
    let mut bytes = 0usize;
    let mut end = start;
    while end < requests.len() && end - start < MAX_BATCH_ITEMS {
        let request_len = requests[end].bytes.len();
        if end > start && bytes.saturating_add(request_len) > MAX_BATCH_BYTES {
            break;
        }
        bytes = bytes.saturating_add(request_len);
        end += 1;
        if bytes >= MAX_BATCH_BYTES {
            break;
        }
    }
    end.max(start + 1)
}

async fn write_batch<W>(
    writer: &mut W,
    requests: &[WriteRequest],
) -> Result<usize, (usize, io::Error)>
where
    W: AsyncWrite + Unpin,
{
    if let [request] = requests {
        writer
            .write_all(&request.bytes)
            .await
            .map_err(|err| (0, err))?;
        return Ok(1);
    }

    let mut request_index = 0;
    let mut offset = 0;
    let mut slices = [IoSlice::new(&[]); MAX_BATCH_ITEMS];

    loop {
        while request_index < requests.len() && offset == requests[request_index].bytes.len() {
            request_index += 1;
            offset = 0;
        }
        if request_index == requests.len() {
            return Ok(requests.len());
        }

        let slice_count = fill_io_slices(&mut slices, requests, request_index, offset);
        let written = writer
            .write_vectored(&slices[..slice_count])
            .await
            .map_err(|err| (request_index, err))?;
        if written == 0 {
            return Err((
                request_index,
                io::Error::new(io::ErrorKind::WriteZero, "failed to write batch"),
            ));
        }

        let mut remaining = written;
        while remaining > 0 && request_index < requests.len() {
            let available = requests[request_index].bytes.len() - offset;
            if remaining < available {
                offset += remaining;
                break;
            }
            remaining -= available;
            request_index += 1;
            offset = 0;
        }
    }
}

fn fill_io_slices<'a>(
    slices: &mut [IoSlice<'a>; MAX_BATCH_ITEMS],
    requests: &'a [WriteRequest],
    request_index: usize,
    offset: usize,
) -> usize {
    let mut slice_count = 0;
    for (index, request) in requests[request_index..].iter().enumerate() {
        let bytes = if index == 0 {
            &request.bytes[offset..]
        } else {
            &request.bytes
        };
        slices[slice_count] = IoSlice::new(bytes);
        slice_count += 1;
    }
    slice_count
}

fn complete_ok(budget: &Budget, requests: &mut [WriteRequest]) {
    for request in requests {
        complete_request(budget, request, Ok(()));
    }
}

fn complete_closed(budget: &Budget, requests: &mut [WriteRequest]) {
    for request in requests {
        complete_request(budget, request, Err(WriteError::Closed));
    }
}

fn complete_request(budget: &Budget, request: &mut WriteRequest, result: Result<(), WriteError>) {
    budget.release(request.budget_bytes);
    request.budget_bytes = 0;
    if let Some(completion) = request.completion.take() {
        let _ = completion.send(WriteCompletion { result });
    }
}

fn drain_closed(budget: &Budget, rx: &mut mpsc::Receiver<WriteMessage>) {
    while let Ok(message) = rx.try_recv() {
        if let WriteMessage::Write(mut request) = message {
            complete_request(budget, &mut request, Err(WriteError::Closed));
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use tokio::io::AsyncReadExt;

    use super::*;

    #[tokio::test]
    async fn writes_owned_bytes_in_order() {
        let (client, mut server) = tokio::io::duplex(64);
        let handoff = WriteHandoff::spawn(client, WriteHandoffConfig::new(4, 64));

        let first = handoff
            .try_write(Bytes::from_static(b"abc"))
            .expect("first handoff");
        let second = handoff
            .try_write(Bytes::from_static(b"def"))
            .expect("second handoff");

        first.wait().await.expect("first write completes");
        second.wait().await.expect("second write completes");

        let mut out = [0_u8; 6];
        server
            .read_exact(&mut out)
            .await
            .expect("read written bytes");
        assert_eq!(&out, b"abcdef");
        assert_eq!(handoff.pending_bytes(), 0);
    }

    #[tokio::test]
    async fn try_write_reports_byte_backpressure_without_losing_bytes() {
        let (client, _server) = tokio::io::duplex(64);
        let handoff = WriteHandoff::spawn(client, WriteHandoffConfig::new(4, 3));

        let err = handoff
            .try_write(Bytes::from_static(b"abcd"))
            .expect_err("over budget");
        assert_eq!(err.into_bytes(), Bytes::from_static(b"abcd"));
        assert_eq!(handoff.pending_bytes(), 0);
    }

    #[tokio::test]
    async fn async_write_reserves_byte_budget() {
        let (client, mut server) = tokio::io::duplex(64);
        let handoff = WriteHandoff::spawn(client, WriteHandoffConfig::new(4, 8));

        let ticket = handoff
            .write(Bytes::from_static(b"hello"))
            .await
            .expect("handoff");
        assert!(handoff.pending_bytes() <= 8);
        ticket.wait().await.expect("completion");

        let mut out = [0_u8; 5];
        server
            .read_exact(&mut out)
            .await
            .expect("read written bytes");
        assert_eq!(&out, b"hello");
    }

    #[tokio::test]
    async fn fire_and_forget_writes_without_completion_ticket() {
        let (client, mut server) = tokio::io::duplex(64);
        let handoff = WriteHandoff::spawn(client, WriteHandoffConfig::new(4, 64));

        handoff
            .try_write_fire_and_forget(Bytes::from_static(b"abc"))
            .expect("fire-and-forget handoff");
        handoff
            .try_write_fire_and_forget(Bytes::from_static(b"def"))
            .expect("fire-and-forget handoff");

        let mut out = [0_u8; 6];
        server
            .read_exact(&mut out)
            .await
            .expect("read written bytes");
        assert_eq!(&out, b"abcdef");
    }

    #[tokio::test]
    async fn async_fire_and_forget_waits_for_budget_without_completion_ticket() {
        let (client, mut server) = tokio::io::duplex(64);
        let handoff = WriteHandoff::spawn(client, WriteHandoffConfig::new(4, 4));

        handoff
            .write_fire_and_forget(Bytes::from_static(b"abcd"))
            .await
            .expect("first write fits budget");
        assert_eq!(handoff.pending_bytes(), 4);

        let second = {
            let handoff = handoff.clone();
            tokio::spawn(async move {
                handoff
                    .write_fire_and_forget(Bytes::from_static(b"efgh"))
                    .await
                    .expect("second write waits for budget");
            })
        };

        let mut first = [0_u8; 4];
        server
            .read_exact(&mut first)
            .await
            .expect("read first write");
        assert_eq!(&first, b"abcd");

        second.await.expect("second write task joins");
        let mut second = [0_u8; 4];
        server
            .read_exact(&mut second)
            .await
            .expect("read second write");
        assert_eq!(&second, b"efgh");
        assert_eq!(handoff.pending_bytes(), 0);
    }

    #[tokio::test]
    async fn close_rejects_new_writes() {
        let (client, _server) = tokio::io::duplex(64);
        let handoff = WriteHandoff::spawn(client, WriteHandoffConfig::new(4, 64));

        handoff.close();

        let err = handoff
            .try_write(Bytes::from_static(b"closed"))
            .expect_err("closed handoff rejects writes");
        assert_eq!(err.into_bytes(), Bytes::from_static(b"closed"));
        assert!(matches!(
            handoff.write(Bytes::from_static(b"closed")).await,
            Err(WriteError::Closed)
        ));
    }

    #[tokio::test]
    async fn request_batches_use_vectored_writes() {
        let writer = CountingWriter::default();
        let calls = writer.vectored_calls.clone();
        let output = writer.output.clone();
        let budget = Budget::new(64);
        let mut requests = vec![
            request(Bytes::from_static(b"abc"), &budget),
            request(Bytes::from_static(b"def"), &budget),
            request(Bytes::from_static(b"ghi"), &budget),
        ];

        let mut writer = writer;
        write_request_batches(&mut writer, &budget, &mut requests)
            .await
            .expect("batch writes");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(&*output.lock().expect("output mutex"), b"abcdefghi");
        assert_eq!(budget.pending(), 0);
    }

    fn request(bytes: Bytes, budget: &Budget) -> WriteRequest {
        let budget_bytes = budget
            .try_acquire(bytes.len())
            .expect("test budget has capacity");
        WriteRequest {
            bytes,
            completion: None,
            budget_bytes,
        }
    }

    #[derive(Default)]
    struct CountingWriter {
        output: Arc<Mutex<Vec<u8>>>,
        vectored_calls: Arc<AtomicUsize>,
    }

    impl AsyncWrite for CountingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.output
                .lock()
                .expect("output mutex")
                .extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.vectored_calls.fetch_add(1, Ordering::SeqCst);
            let mut output = self.output.lock().expect("output mutex");
            let mut written = 0;
            for buf in bufs {
                output.extend_from_slice(buf);
                written += buf.len();
            }
            Poll::Ready(Ok(written))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }
    }
}
