use bytes::{Bytes, BytesMut};
#[cfg(feature = "monoio")]
use std::cell::RefCell;
#[cfg(feature = "monoio")]
use std::collections::VecDeque;
use std::io::{self, IoSlice};
#[cfg(feature = "monoio")]
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[cfg(feature = "monoio")]
use std::task::Waker;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::{Notify, mpsc, oneshot};

use crate::{WriteBackpressure, WriteError};

const MAX_BATCH_ITEMS: usize = 64;
const MAX_BATCH_BYTES: usize = 1024 * 1024;
pub const DEFAULT_WRITE_COALESCE_THRESHOLD: usize = 1024;

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

#[derive(Clone, Copy, Debug)]
pub struct WriteCoalescerConfig {
    pub threshold_bytes: usize,
}

impl WriteCoalescerConfig {
    pub fn new(threshold_bytes: usize) -> Self {
        Self {
            threshold_bytes: threshold_bytes.max(1),
        }
    }

    pub fn immediate() -> Self {
        Self { threshold_bytes: 1 }
    }
}

impl Default for WriteCoalescerConfig {
    fn default() -> Self {
        Self {
            threshold_bytes: DEFAULT_WRITE_COALESCE_THRESHOLD,
        }
    }
}

pub struct WriteHandoff {
    tx: mpsc::Sender<WriteMessage>,
    budget: Arc<Budget>,
    closed: Arc<AtomicBool>,
}

pub struct WriteCoalescer {
    handoff: WriteHandoff,
    threshold_bytes: usize,
    pending: BytesMut,
}

#[cfg(feature = "monoio")]
pub struct MonoioWriteHandoff {
    inner: Rc<RefCell<MonoioWriteState>>,
}

#[cfg(feature = "monoio")]
pub struct MonoioWriteCoalescer {
    handoff: MonoioWriteHandoff,
    threshold_bytes: usize,
    pending: BytesMut,
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

#[cfg(feature = "monoio")]
struct MonoioWriteState {
    queue: VecDeque<WriteRequest>,
    pending_bytes: usize,
    closed: bool,
    queue_limit: usize,
    byte_limit: usize,
    writer_waker: Option<Waker>,
    sender_wakers: Vec<Waker>,
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

    #[cfg(feature = "monoio")]
    pub fn spawn_monoio<W>(writer: W, config: WriteHandoffConfig) -> MonoioWriteHandoff
    where
        W: monoio::io::AsyncWriteRent + Unpin + 'static,
    {
        MonoioWriteHandoff::spawn(writer, config)
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

impl WriteCoalescer {
    pub fn new(handoff: WriteHandoff) -> Self {
        Self::with_config(handoff, WriteCoalescerConfig::default())
    }

    pub fn with_threshold(handoff: WriteHandoff, threshold_bytes: usize) -> Self {
        Self::with_config(handoff, WriteCoalescerConfig::new(threshold_bytes))
    }

    pub fn with_config(handoff: WriteHandoff, config: WriteCoalescerConfig) -> Self {
        Self {
            handoff,
            threshold_bytes: config.threshold_bytes.max(1),
            pending: BytesMut::new(),
        }
    }

    pub fn threshold_bytes(&self) -> usize {
        self.threshold_bytes
    }

    pub fn pending_bytes(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub fn handoff(&self) -> &WriteHandoff {
        &self.handoff
    }

    pub async fn write_fire_and_forget(&mut self, bytes: Bytes) -> Result<(), WriteError> {
        if bytes.is_empty() {
            return self.flush().await;
        }
        if self.threshold_bytes == 1 {
            self.flush().await?;
            return self.handoff.write_fire_and_forget(bytes).await;
        }
        if self.pending.is_empty() && bytes.len() >= self.threshold_bytes {
            return self.handoff.write_fire_and_forget(bytes).await;
        }
        if !self.pending.is_empty()
            && bytes.len() >= self.threshold_bytes
            && self.pending.len() < self.threshold_bytes
        {
            self.flush().await?;
            return self.handoff.write_fire_and_forget(bytes).await;
        }

        self.pending.extend_from_slice(&bytes);
        if self.pending.len() >= self.threshold_bytes {
            self.flush().await?;
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), WriteError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let bytes = self.pending.split().freeze();
        let restore = bytes.clone();
        if let Err(err) = self.handoff.write_fire_and_forget(bytes).await {
            self.pending.extend_from_slice(&restore);
            return Err(err);
        }
        Ok(())
    }
}

#[cfg(feature = "monoio")]
impl MonoioWriteHandoff {
    pub fn spawn<W>(writer: W, config: WriteHandoffConfig) -> Self
    where
        W: monoio::io::AsyncWriteRent + Unpin + 'static,
    {
        let inner = Rc::new(RefCell::new(MonoioWriteState::new(config)));
        monoio::spawn(writer_loop_monoio_local(writer, inner.clone()));
        Self { inner }
    }

    pub fn try_write(&self, bytes: Bytes) -> Result<WriteTicket, WriteBackpressure> {
        let (completion, rx) = oneshot::channel();
        self.try_enqueue(bytes, Some(completion))?;
        Ok(WriteTicket { rx })
    }

    pub fn try_write_fire_and_forget(&self, bytes: Bytes) -> Result<(), WriteBackpressure> {
        self.try_enqueue(bytes, None)
    }

    pub async fn write(&self, bytes: Bytes) -> Result<WriteTicket, WriteError> {
        self.enqueue(bytes, true)
            .await
            .map(|ticket| ticket.expect("ticket requested"))
    }

    pub async fn write_fire_and_forget(&self, bytes: Bytes) -> Result<(), WriteError> {
        self.enqueue(bytes, false).await.map(|_| ())
    }

    pub fn pending_bytes(&self) -> usize {
        self.inner.borrow().pending_bytes
    }

    pub fn close(&self) {
        close_monoio_state(&self.inner);
    }

    fn try_enqueue(
        &self,
        bytes: Bytes,
        completion: Option<oneshot::Sender<WriteCompletion>>,
    ) -> Result<(), WriteBackpressure> {
        let len = bytes.len();
        let mut state = self.inner.borrow_mut();
        if state.closed {
            return Err(WriteBackpressure::closed(bytes));
        }
        if len > state.byte_limit {
            return Err(WriteBackpressure::byte_budget_exceeded(
                bytes,
                state.pending_bytes.saturating_add(len),
                state.byte_limit,
            ));
        }
        let attempted = state.pending_bytes.saturating_add(len);
        if attempted > state.byte_limit {
            return Err(WriteBackpressure::byte_budget_exceeded(
                bytes,
                attempted,
                state.byte_limit,
            ));
        }
        if state.queue.len() >= state.queue_limit {
            return Err(WriteBackpressure::queue_full(bytes));
        }

        state.pending_bytes = attempted;
        state.queue.push_back(WriteRequest {
            bytes,
            completion,
            budget_bytes: len,
        });
        let writer_waker = state.writer_waker.take();
        drop(state);
        wake_one(writer_waker);
        Ok(())
    }

    async fn enqueue(
        &self,
        bytes: Bytes,
        wants_ticket: bool,
    ) -> Result<Option<WriteTicket>, WriteError> {
        let mut bytes = Some(bytes);
        std::future::poll_fn(|cx| {
            let len = bytes.as_ref().expect("bytes available until enqueue").len();
            let mut state = self.inner.borrow_mut();
            if state.closed {
                return std::task::Poll::Ready(Err(WriteError::Closed));
            }
            if len > state.byte_limit {
                return std::task::Poll::Ready(Err(WriteError::ByteBudgetExceeded {
                    attempted: state.pending_bytes.saturating_add(len),
                    limit: state.byte_limit,
                }));
            }

            let attempted = state.pending_bytes.saturating_add(len);
            if attempted > state.byte_limit || state.queue.len() >= state.queue_limit {
                state.store_sender_waker(cx.waker());
                return std::task::Poll::Pending;
            }

            let bytes = bytes.take().expect("bytes enqueued once");
            let (completion, ticket) = if wants_ticket {
                let (completion, rx) = oneshot::channel();
                (Some(completion), Some(WriteTicket { rx }))
            } else {
                (None, None)
            };
            state.pending_bytes = attempted;
            state.queue.push_back(WriteRequest {
                bytes,
                completion,
                budget_bytes: len,
            });
            let writer_waker = state.writer_waker.take();
            drop(state);
            wake_one(writer_waker);
            std::task::Poll::Ready(Ok(ticket))
        })
        .await
    }
}

#[cfg(feature = "monoio")]
impl MonoioWriteCoalescer {
    pub fn new(handoff: MonoioWriteHandoff) -> Self {
        Self::with_config(handoff, WriteCoalescerConfig::default())
    }

    pub fn with_threshold(handoff: MonoioWriteHandoff, threshold_bytes: usize) -> Self {
        Self::with_config(handoff, WriteCoalescerConfig::new(threshold_bytes))
    }

    pub fn with_config(handoff: MonoioWriteHandoff, config: WriteCoalescerConfig) -> Self {
        Self {
            handoff,
            threshold_bytes: config.threshold_bytes.max(1),
            pending: BytesMut::new(),
        }
    }

    pub fn threshold_bytes(&self) -> usize {
        self.threshold_bytes
    }

    pub fn pending_bytes(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub fn handoff(&self) -> &MonoioWriteHandoff {
        &self.handoff
    }

    pub async fn write_fire_and_forget(&mut self, bytes: Bytes) -> Result<(), WriteError> {
        if bytes.is_empty() {
            return self.flush().await;
        }
        if self.threshold_bytes == 1 {
            self.flush().await?;
            return self.handoff.write_fire_and_forget(bytes).await;
        }
        if self.pending.is_empty() && bytes.len() >= self.threshold_bytes {
            return self.handoff.write_fire_and_forget(bytes).await;
        }
        if !self.pending.is_empty()
            && bytes.len() >= self.threshold_bytes
            && self.pending.len() < self.threshold_bytes
        {
            self.flush().await?;
            return self.handoff.write_fire_and_forget(bytes).await;
        }

        self.pending.extend_from_slice(&bytes);
        if self.pending.len() >= self.threshold_bytes {
            self.flush().await?;
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), WriteError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let bytes = self.pending.split().freeze();
        let restore = bytes.clone();
        if let Err(err) = self.handoff.write_fire_and_forget(bytes).await {
            self.pending.extend_from_slice(&restore);
            return Err(err);
        }
        Ok(())
    }
}

#[cfg(feature = "monoio")]
impl Clone for MonoioWriteHandoff {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[cfg(feature = "monoio")]
impl Drop for MonoioWriteHandoff {
    fn drop(&mut self) {
        if Rc::strong_count(&self.inner) <= 2 {
            close_monoio_state(&self.inner);
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

#[cfg(feature = "monoio")]
impl MonoioWriteState {
    fn new(config: WriteHandoffConfig) -> Self {
        Self {
            queue: VecDeque::with_capacity(config.max_items),
            pending_bytes: 0,
            closed: false,
            queue_limit: config.max_items,
            byte_limit: config.max_pending_bytes,
            writer_waker: None,
            sender_wakers: Vec::new(),
        }
    }

    fn store_sender_waker(&mut self, waker: &Waker) {
        if self
            .sender_wakers
            .iter()
            .any(|stored| stored.will_wake(waker))
        {
            return;
        }
        self.sender_wakers.push(waker.clone());
    }

    fn take_sender_wakers(&mut self) -> Vec<Waker> {
        std::mem::take(&mut self.sender_wakers)
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

#[cfg(feature = "monoio")]
async fn writer_loop_monoio_local<W>(mut writer: W, inner: Rc<RefCell<MonoioWriteState>>)
where
    W: monoio::io::AsyncWriteRent + Unpin,
{
    let mut requests = Vec::with_capacity(MAX_BATCH_ITEMS);
    let mut batch_buffers = MonoioWriteBatchBuffers::new();

    loop {
        requests.clear();
        let mut sender_wakers = Vec::new();
        let closed_and_empty = std::future::poll_fn(|cx| {
            let mut state = inner.borrow_mut();
            if state.queue.is_empty() {
                if state.closed {
                    return std::task::Poll::Ready(true);
                }
                state.writer_waker = Some(cx.waker().clone());
                return std::task::Poll::Pending;
            }

            let received = state.queue.len().min(MAX_BATCH_ITEMS);
            for _ in 0..received {
                requests.push(state.queue.pop_front().expect("queue length checked"));
            }
            sender_wakers = state.take_sender_wakers();
            std::task::Poll::Ready(false)
        })
        .await;
        wake_all(sender_wakers);
        if closed_and_empty {
            break;
        }

        if write_request_batches_monoio_local(
            &mut writer,
            &inner,
            &mut requests,
            &mut batch_buffers,
        )
        .await
        .is_err()
        {
            close_monoio_state(&inner);
            drain_monoio_closed(&inner);
            return;
        }

        let state = inner.borrow();
        if state.closed && state.queue.is_empty() {
            break;
        }
    }

    close_monoio_state(&inner);
    drain_monoio_closed(&inner);
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

#[cfg(feature = "monoio")]
async fn write_request_batches_monoio_local<W>(
    writer: &mut W,
    inner: &Rc<RefCell<MonoioWriteState>>,
    requests: &mut [WriteRequest],
    batch_buffers: &mut MonoioWriteBatchBuffers,
) -> Result<(), ()>
where
    W: monoio::io::AsyncWriteRent + Unpin,
{
    let mut start = 0;
    while start < requests.len() {
        let end = batch_end(requests, start);
        match write_monoio_batch(writer, &mut requests[start..end], batch_buffers).await {
            Ok(()) => complete_monoio_ok(inner, &mut requests[start..end]),
            Err(err) => {
                complete_monoio_request(inner, &mut requests[start], Err(WriteError::Io(err)));
                if start + 1 < requests.len() {
                    complete_monoio_closed(inner, &mut requests[start + 1..]);
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

#[cfg(feature = "monoio")]
async fn write_monoio_batch<W>(
    writer: &mut W,
    requests: &mut [WriteRequest],
    batch_buffers: &mut MonoioWriteBatchBuffers,
) -> io::Result<()>
where
    W: monoio::io::AsyncWriteRent + Unpin,
{
    use monoio::io::AsyncWriteRentExt as _;

    if let [request] = requests {
        let bytes = std::mem::take(&mut request.bytes);
        let (result, bytes) = writer.write_all(bytes).await;
        request.bytes = bytes;
        result.map(|_| ())
    } else {
        write_monoio_vectored_batch(writer, requests, batch_buffers).await
    }
}

#[cfg(all(feature = "monoio", unix))]
async fn write_monoio_vectored_batch<W>(
    writer: &mut W,
    requests: &mut [WriteRequest],
    batch_buffers: &mut MonoioWriteBatchBuffers,
) -> io::Result<()>
where
    W: monoio::io::AsyncWriteRent + Unpin,
{
    use monoio::io::AsyncWriteRentExt as _;

    let batch = batch_buffers.build(requests);
    let (result, batch) = writer.write_vectored_all(batch).await;
    batch_buffers.reclaim(batch);
    result.map(|_| ())
}

#[cfg(all(feature = "monoio", not(unix)))]
async fn write_monoio_vectored_batch<W>(
    writer: &mut W,
    requests: &mut [WriteRequest],
    _batch_buffers: &mut MonoioWriteBatchBuffers,
) -> io::Result<()>
where
    W: monoio::io::AsyncWriteRent + Unpin,
{
    use monoio::io::AsyncWriteRentExt as _;

    for request in requests {
        let bytes = std::mem::take(&mut request.bytes);
        let (result, bytes) = writer.write_all(bytes).await;
        request.bytes = bytes;
        result?;
    }
    Ok(())
}

#[cfg(all(feature = "monoio", unix))]
struct MonoioWriteBatchBuffers {
    iovecs: Vec<libc::iovec>,
}

#[cfg(all(feature = "monoio", unix))]
struct MonoioWriteBatch {
    iovecs: Vec<libc::iovec>,
}

#[cfg(all(feature = "monoio", unix))]
impl MonoioWriteBatchBuffers {
    fn new() -> Self {
        Self {
            iovecs: Vec::with_capacity(MAX_BATCH_ITEMS),
        }
    }

    fn build(&mut self, requests: &[WriteRequest]) -> MonoioWriteBatch {
        let mut iovecs = std::mem::take(&mut self.iovecs);
        iovecs.clear();
        if iovecs.capacity() < requests.len() {
            iovecs.reserve(requests.len() - iovecs.capacity());
        }
        for request in requests {
            if !request.bytes.is_empty() {
                iovecs.push(libc::iovec {
                    iov_base: request.bytes.as_ptr() as *mut libc::c_void,
                    iov_len: request.bytes.len(),
                });
            }
        }
        MonoioWriteBatch { iovecs }
    }

    fn reclaim(&mut self, mut batch: MonoioWriteBatch) {
        batch.iovecs.clear();
        self.iovecs = batch.iovecs;
    }
}

#[cfg(all(feature = "monoio", not(unix)))]
struct MonoioWriteBatchBuffers;

#[cfg(all(feature = "monoio", not(unix)))]
impl MonoioWriteBatchBuffers {
    fn new() -> Self {
        Self
    }
}

#[cfg(all(feature = "monoio", unix))]
unsafe impl monoio::buf::IoVecBuf for MonoioWriteBatch {
    // The iovec entries point into the `WriteRequest` bytes slice borrowed by
    // `write_monoio_vectored_batch`; those requests are not mutated or dropped
    // until the vectored write future returns.
    fn read_iovec_ptr(&self) -> *const libc::iovec {
        self.iovecs.as_ptr()
    }

    fn read_iovec_len(&self) -> usize {
        self.iovecs.len()
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

#[cfg(feature = "monoio")]
fn complete_monoio_ok(inner: &Rc<RefCell<MonoioWriteState>>, requests: &mut [WriteRequest]) {
    let released = take_monoio_budget(requests);
    release_monoio_budget(inner, released);
    for request in requests {
        if let Some(completion) = request.completion.take() {
            let _ = completion.send(WriteCompletion { result: Ok(()) });
        }
    }
}

#[cfg(feature = "monoio")]
fn complete_monoio_closed(inner: &Rc<RefCell<MonoioWriteState>>, requests: &mut [WriteRequest]) {
    let released = take_monoio_budget(requests);
    release_monoio_budget(inner, released);
    for request in requests {
        if let Some(completion) = request.completion.take() {
            let _ = completion.send(WriteCompletion {
                result: Err(WriteError::Closed),
            });
        }
    }
}

#[cfg(feature = "monoio")]
fn complete_monoio_request(
    inner: &Rc<RefCell<MonoioWriteState>>,
    request: &mut WriteRequest,
    result: Result<(), WriteError>,
) {
    release_monoio_budget(inner, request.budget_bytes);
    request.budget_bytes = 0;
    if let Some(completion) = request.completion.take() {
        let _ = completion.send(WriteCompletion { result });
    }
}

#[cfg(feature = "monoio")]
fn take_monoio_budget(requests: &mut [WriteRequest]) -> usize {
    let mut released = 0usize;
    for request in requests {
        released = released.saturating_add(request.budget_bytes);
        request.budget_bytes = 0;
    }
    released
}

fn drain_closed(budget: &Budget, rx: &mut mpsc::Receiver<WriteMessage>) {
    while let Ok(message) = rx.try_recv() {
        if let WriteMessage::Write(mut request) = message {
            complete_request(budget, &mut request, Err(WriteError::Closed));
        }
    }
}

#[cfg(feature = "monoio")]
fn release_monoio_budget(inner: &Rc<RefCell<MonoioWriteState>>, bytes: usize) {
    if bytes == 0 {
        return;
    }
    let sender_wakers = {
        let mut state = inner.borrow_mut();
        debug_assert!(
            state.pending_bytes >= bytes,
            "released more bytes than acquired"
        );
        state.pending_bytes -= bytes;
        state.take_sender_wakers()
    };
    wake_all(sender_wakers);
}

#[cfg(feature = "monoio")]
fn drain_monoio_closed(inner: &Rc<RefCell<MonoioWriteState>>) {
    let requests: Vec<_> = inner.borrow_mut().queue.drain(..).collect();
    for mut request in requests {
        complete_monoio_request(inner, &mut request, Err(WriteError::Closed));
    }
}

#[cfg(feature = "monoio")]
fn close_monoio_state(inner: &Rc<RefCell<MonoioWriteState>>) {
    let (writer_waker, sender_wakers) = {
        let mut state = inner.borrow_mut();
        state.closed = true;
        (state.writer_waker.take(), state.take_sender_wakers())
    };
    wake_one(writer_waker);
    wake_all(sender_wakers);
}

#[cfg(feature = "monoio")]
fn wake_one(waker: Option<Waker>) {
    if let Some(waker) = waker {
        waker.wake();
    }
}

#[cfg(feature = "monoio")]
fn wake_all(wakers: Vec<Waker>) {
    for waker in wakers {
        waker.wake();
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
    async fn coalescer_buffers_until_threshold_or_flush() {
        let writer = CountingWriter::default();
        let output = writer.output.clone();
        let handoff = WriteHandoff::spawn(writer, WriteHandoffConfig::new(4, 64));
        let mut coalescer = WriteCoalescer::with_threshold(handoff, 4);

        coalescer
            .write_fire_and_forget(Bytes::from_static(b"ab"))
            .await
            .expect("buffer small write");
        tokio::task::yield_now().await;
        assert_eq!(&*output.lock().expect("output mutex"), b"");
        assert_eq!(coalescer.pending_bytes(), 2);

        coalescer
            .write_fire_and_forget(Bytes::from_static(b"cd"))
            .await
            .expect("threshold flush");
        let barrier = coalescer
            .handoff()
            .write(Bytes::new())
            .await
            .expect("submit barrier");
        barrier.wait().await.expect("barrier completes");

        assert_eq!(&*output.lock().expect("output mutex"), b"abcd");
        assert_eq!(coalescer.pending_bytes(), 0);
    }

    #[tokio::test]
    async fn coalescer_flushes_tail_explicitly() {
        let writer = CountingWriter::default();
        let output = writer.output.clone();
        let handoff = WriteHandoff::spawn(writer, WriteHandoffConfig::new(4, 64));
        let mut coalescer = WriteCoalescer::with_threshold(handoff, 1024);

        coalescer
            .write_fire_and_forget(Bytes::from_static(b"tail"))
            .await
            .expect("buffer tail");
        coalescer.flush().await.expect("flush tail");
        let barrier = coalescer
            .handoff()
            .write(Bytes::new())
            .await
            .expect("submit barrier");
        barrier.wait().await.expect("barrier completes");

        assert_eq!(&*output.lock().expect("output mutex"), b"tail");
        assert_eq!(coalescer.pending_bytes(), 0);
    }

    #[tokio::test]
    async fn coalescer_immediate_threshold_preserves_immediate_submission() {
        let writer = CountingWriter::default();
        let output = writer.output.clone();
        let handoff = WriteHandoff::spawn(writer, WriteHandoffConfig::new(4, 64));
        let mut coalescer = WriteCoalescer::with_config(handoff, WriteCoalescerConfig::immediate());

        coalescer
            .write_fire_and_forget(Bytes::from_static(b"abc"))
            .await
            .expect("submit immediately");
        let barrier = coalescer
            .handoff()
            .write(Bytes::new())
            .await
            .expect("submit barrier");
        barrier.wait().await.expect("barrier completes");

        assert_eq!(&*output.lock().expect("output mutex"), b"abc");
        assert_eq!(coalescer.pending_bytes(), 0);
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

    #[cfg(feature = "monoio")]
    #[test]
    fn spawn_monoio_writes_owned_bytes_in_order() {
        monoio::start::<monoio::LegacyDriver, _>(async {
            let writer = MonoioCaptureWriter::default();
            let output = writer.output.clone();
            let handoff = WriteHandoff::spawn_monoio(writer, WriteHandoffConfig::new(4, 64));

            let first = handoff
                .try_write(Bytes::from_static(b"abc"))
                .expect("first handoff");
            let second = handoff
                .try_write(Bytes::from_static(b"def"))
                .expect("second handoff");

            first.wait().await.expect("first write completes");
            second.wait().await.expect("second write completes");

            assert_eq!(&*output.lock().expect("output mutex"), b"abcdef");
            assert_eq!(handoff.pending_bytes(), 0);
        });
    }

    #[cfg(feature = "monoio")]
    #[derive(Default)]
    struct MonoioCaptureWriter {
        output: Arc<Mutex<Vec<u8>>>,
    }

    #[cfg(feature = "monoio")]
    impl monoio::io::AsyncWriteRent for MonoioCaptureWriter {
        async fn write<T: monoio::buf::IoBuf>(&mut self, buf: T) -> monoio::BufResult<usize, T> {
            let len = monoio::buf::IoBuf::bytes_init(&buf);
            // SAFETY: `bytes_init` is the readable prefix of `buf`, and `buf`
            // remains alive while the bytes are copied into test-owned output.
            let bytes =
                unsafe { std::slice::from_raw_parts(monoio::buf::IoBuf::read_ptr(&buf), len) };
            self.output
                .lock()
                .expect("output mutex")
                .extend_from_slice(bytes);

            (Ok(len), buf)
        }

        async fn writev<T: monoio::buf::IoVecBuf>(
            &mut self,
            buf_vec: T,
        ) -> monoio::BufResult<usize, T> {
            #[cfg(unix)]
            {
                let iovecs = unsafe {
                    std::slice::from_raw_parts(
                        monoio::buf::IoVecBuf::read_iovec_ptr(&buf_vec),
                        monoio::buf::IoVecBuf::read_iovec_len(&buf_vec),
                    )
                };
                let mut output = self.output.lock().expect("output mutex");
                let mut written = 0usize;
                for iovec in iovecs {
                    let bytes = unsafe {
                        std::slice::from_raw_parts(iovec.iov_base.cast::<u8>(), iovec.iov_len)
                    };
                    output.extend_from_slice(bytes);
                    written += bytes.len();
                }
                (Ok(written), buf_vec)
            }
            #[cfg(not(unix))]
            {
                (
                    Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "vectored writes are not used in monoio tests",
                    )),
                    buf_vec,
                )
            }
        }

        async fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }

        async fn shutdown(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}
