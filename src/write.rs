use bytes::{Bytes, BytesMut};
use std::io::{self, IoSlice};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::{Notify, mpsc, oneshot};

use crate::{WriteBackpressure, WriteError};

const MAX_BATCH_ITEMS: usize = 64;
const MAX_BATCH_BYTES: usize = 1024 * 1024;
pub const DEFAULT_WRITE_COALESCE_THRESHOLD: usize = 16 * 1024;

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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteCoalescerStats {
    pub input_chunks: usize,
    pub input_bytes: usize,
    pub flushes: usize,
    pub flush_bytes: usize,
    pub buffered_flushes: usize,
    pub direct_flushes: usize,
    pub buffered_input_chunks: usize,
    pub max_chunks_per_flush: usize,
    pub max_bytes_per_flush: usize,
    pub max_pending_bytes: usize,
    pub total_flush_wait_nanos: u128,
    pub max_flush_wait_nanos: u64,
}

impl WriteCoalescerStats {
    pub fn merge(&mut self, other: Self) {
        self.input_chunks = self.input_chunks.saturating_add(other.input_chunks);
        self.input_bytes = self.input_bytes.saturating_add(other.input_bytes);
        self.flushes = self.flushes.saturating_add(other.flushes);
        self.flush_bytes = self.flush_bytes.saturating_add(other.flush_bytes);
        self.buffered_flushes = self.buffered_flushes.saturating_add(other.buffered_flushes);
        self.direct_flushes = self.direct_flushes.saturating_add(other.direct_flushes);
        self.buffered_input_chunks = self
            .buffered_input_chunks
            .saturating_add(other.buffered_input_chunks);
        self.max_chunks_per_flush = self.max_chunks_per_flush.max(other.max_chunks_per_flush);
        self.max_bytes_per_flush = self.max_bytes_per_flush.max(other.max_bytes_per_flush);
        self.max_pending_bytes = self.max_pending_bytes.max(other.max_pending_bytes);
        self.total_flush_wait_nanos = self
            .total_flush_wait_nanos
            .saturating_add(other.total_flush_wait_nanos);
        self.max_flush_wait_nanos = self.max_flush_wait_nanos.max(other.max_flush_wait_nanos);
    }

    pub fn avg_bytes_per_flush(self) -> f64 {
        average(self.flush_bytes, self.flushes)
    }

    pub fn avg_chunks_per_flush(self) -> f64 {
        average(self.input_chunks, self.flushes)
    }

    pub fn avg_buffered_chunks_per_flush(self) -> f64 {
        average(self.buffered_input_chunks, self.buffered_flushes)
    }

    pub fn avg_flush_wait_nanos(self) -> f64 {
        if self.buffered_flushes == 0 {
            0.0
        } else {
            (self.total_flush_wait_nanos as f64) / (self.buffered_flushes as f64)
        }
    }
}

fn average(total: usize, count: usize) -> f64 {
    if count == 0 {
        0.0
    } else {
        (total as f64) / (count as f64)
    }
}

fn nanos_u64(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
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
    pending_started_at: Option<Instant>,
    pending_chunks: usize,
    stats: Option<WriteCoalescerStats>,
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

struct BudgetPermit<'a> {
    budget: &'a Budget,
    bytes: usize,
}

enum WriteMessage {
    Write(WriteRequest),
    Shutdown,
}

enum CoalescerWriteAction {
    Flush,
    Direct,
    FlushThenDirect,
    Buffer,
}

struct Budget {
    pending: AtomicUsize,
    waiters: AtomicUsize,
    closed: AtomicBool,
    notify: Notify,
    limit: usize,
}

struct BudgetWaiter<'a>(&'a AtomicUsize);

impl Drop for BudgetWaiter<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug)]
enum BudgetAcquireError {
    Closed,
    LimitExceeded { attempted: usize, limit: usize },
}

impl WriteRequest {
    fn new(
        bytes: Bytes,
        completion: Option<oneshot::Sender<WriteCompletion>>,
        budget_bytes: usize,
    ) -> Self {
        Self {
            bytes,
            completion,
            budget_bytes,
        }
    }

    fn release_budget(&mut self, budget: &Budget) {
        budget.release(std::mem::take(&mut self.budget_bytes));
    }

    fn release_into_bytes(mut self, budget: &Budget) -> Bytes {
        self.release_budget(budget);
        self.bytes
    }
}

impl<'a> BudgetPermit<'a> {
    fn new(budget: &'a Budget, bytes: usize) -> Self {
        Self { budget, bytes }
    }

    fn commit(mut self) -> usize {
        std::mem::take(&mut self.bytes)
    }
}

impl Drop for BudgetPermit<'_> {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

impl BudgetAcquireError {
    fn into_backpressure(self, bytes: Bytes) -> WriteBackpressure {
        match self {
            Self::Closed => WriteBackpressure::closed(bytes),
            Self::LimitExceeded { attempted, limit } => {
                WriteBackpressure::byte_budget_exceeded(bytes, attempted, limit)
            }
        }
    }

    fn into_write_error(self) -> WriteError {
        match self {
            Self::Closed => WriteError::Closed,
            Self::LimitExceeded { attempted, limit } => {
                WriteError::ByteBudgetExceeded { attempted, limit }
            }
        }
    }
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
        let (completion, rx) = oneshot::channel();
        self.try_enqueue(bytes, Some(completion))?;
        Ok(WriteTicket { rx })
    }

    pub fn try_write_fire_and_forget(&self, bytes: Bytes) -> Result<(), WriteBackpressure> {
        self.try_enqueue(bytes, None)
    }

    pub async fn write(&self, bytes: Bytes) -> Result<WriteTicket, WriteError> {
        let (completion, rx) = oneshot::channel();
        self.enqueue(bytes, Some(completion)).await?;
        Ok(WriteTicket { rx })
    }

    pub async fn write_fire_and_forget(&self, bytes: Bytes) -> Result<(), WriteError> {
        self.enqueue(bytes, None).await
    }

    pub fn pending_bytes(&self) -> usize {
        self.budget.pending()
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.budget.close();
        let _ = self.tx.try_send(WriteMessage::Shutdown);
    }

    fn try_enqueue(
        &self,
        bytes: Bytes,
        completion: Option<oneshot::Sender<WriteCompletion>>,
    ) -> Result<(), WriteBackpressure> {
        let request = self.try_request(bytes, completion)?;
        self.try_send_request(request)
    }

    async fn enqueue(
        &self,
        bytes: Bytes,
        completion: Option<oneshot::Sender<WriteCompletion>>,
    ) -> Result<(), WriteError> {
        let request = self.request(bytes, completion).await?;
        self.send_request(request).await
    }

    fn try_request(
        &self,
        bytes: Bytes,
        completion: Option<oneshot::Sender<WriteCompletion>>,
    ) -> Result<WriteRequest, WriteBackpressure> {
        match self.try_budget_permit(bytes.len()) {
            Ok(permit) => self.request_from_permit(bytes, completion, permit),
            Err(err) => Err(err.into_backpressure(bytes)),
        }
    }

    async fn request(
        &self,
        bytes: Bytes,
        completion: Option<oneshot::Sender<WriteCompletion>>,
    ) -> Result<WriteRequest, WriteError> {
        match self.budget_permit(bytes.len()).await {
            Ok(permit) => self.request_from_permit_or_closed(bytes, completion, permit),
            Err(err) => Err(err.into_write_error()),
        }
    }

    fn request_from_permit(
        &self,
        bytes: Bytes,
        completion: Option<oneshot::Sender<WriteCompletion>>,
        permit: BudgetPermit<'_>,
    ) -> Result<WriteRequest, WriteBackpressure> {
        match self.closed.load(Ordering::Acquire) {
            false => Ok(WriteRequest::new(bytes, completion, permit.commit())),
            true => Err(WriteBackpressure::closed(bytes)),
        }
    }

    fn request_from_permit_or_closed(
        &self,
        bytes: Bytes,
        completion: Option<oneshot::Sender<WriteCompletion>>,
        permit: BudgetPermit<'_>,
    ) -> Result<WriteRequest, WriteError> {
        match self.closed.load(Ordering::Acquire) {
            false => Ok(WriteRequest::new(bytes, completion, permit.commit())),
            true => Err(WriteError::Closed),
        }
    }

    fn try_budget_permit(&self, bytes: usize) -> Result<BudgetPermit<'_>, BudgetAcquireError> {
        match self.closed.load(Ordering::Acquire) {
            false => self
                .budget
                .try_acquire(bytes)
                .map(|permit| BudgetPermit::new(self.budget.as_ref(), permit)),
            true => Err(BudgetAcquireError::Closed),
        }
    }

    async fn budget_permit(&self, bytes: usize) -> Result<BudgetPermit<'_>, BudgetAcquireError> {
        match self.closed.load(Ordering::Acquire) {
            false => self
                .budget
                .acquire(bytes)
                .await
                .map(|permit| BudgetPermit::new(self.budget.as_ref(), permit)),
            true => Err(BudgetAcquireError::Closed),
        }
    }

    fn try_send_request(&self, request: WriteRequest) -> Result<(), WriteBackpressure> {
        match self.tx.try_send(WriteMessage::Write(request)) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(WriteMessage::Write(request))) => Err(
                WriteBackpressure::queue_full(request.release_into_bytes(&self.budget)),
            ),
            Err(mpsc::error::TrySendError::Closed(WriteMessage::Write(request))) => {
                self.mark_closed();
                Err(WriteBackpressure::closed(
                    request.release_into_bytes(&self.budget),
                ))
            }
            Err(mpsc::error::TrySendError::Full(WriteMessage::Shutdown))
            | Err(mpsc::error::TrySendError::Closed(WriteMessage::Shutdown)) => unreachable!(),
        }
    }

    async fn send_request(&self, request: WriteRequest) -> Result<(), WriteError> {
        match self.tx.send(WriteMessage::Write(request)).await {
            Ok(()) => Ok(()),
            Err(err) => match err.0 {
                WriteMessage::Write(mut request) => {
                    request.release_budget(&self.budget);
                    self.mark_closed();
                    Err(WriteError::Closed)
                }
                WriteMessage::Shutdown => unreachable!(),
            },
        }
    }

    fn mark_closed(&self) {
        self.closed.store(true, Ordering::Release);
        self.budget.close();
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

    pub fn with_threshold_and_stats(handoff: WriteHandoff, threshold_bytes: usize) -> Self {
        Self::with_config_and_stats(handoff, WriteCoalescerConfig::new(threshold_bytes))
    }

    pub fn with_config(handoff: WriteHandoff, config: WriteCoalescerConfig) -> Self {
        Self {
            handoff,
            threshold_bytes: config.threshold_bytes.max(1),
            pending: BytesMut::new(),
            pending_started_at: None,
            pending_chunks: 0,
            stats: None,
        }
    }

    pub fn with_config_and_stats(handoff: WriteHandoff, config: WriteCoalescerConfig) -> Self {
        let mut coalescer = Self::with_config(handoff, config);
        coalescer.stats = Some(WriteCoalescerStats::default());
        coalescer
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

    pub fn stats(&self) -> WriteCoalescerStats {
        self.stats.unwrap_or_default()
    }

    pub fn stats_enabled(&self) -> bool {
        self.stats.is_some()
    }

    pub async fn write_fire_and_forget(&mut self, bytes: Bytes) -> Result<(), WriteError> {
        let action = self.write_action(bytes.len());
        if !bytes.is_empty() {
            self.record_input(bytes.len());
        }

        match action {
            CoalescerWriteAction::Flush => self.flush().await,
            CoalescerWriteAction::Direct => self.write_direct(bytes).await,
            CoalescerWriteAction::FlushThenDirect => {
                self.flush().await?;
                self.write_direct(bytes).await
            }
            CoalescerWriteAction::Buffer => self.buffer_write(bytes).await,
        }
    }

    pub async fn flush(&mut self) -> Result<(), WriteError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let bytes = self.pending.split().freeze();
        let chunks = std::mem::take(&mut self.pending_chunks);
        let started_at = self.pending_started_at.take();
        let restore = bytes.clone();
        if let Err(err) = self.enqueue_fire_and_forget(bytes).await {
            self.pending.extend_from_slice(&restore);
            self.pending_chunks = chunks;
            self.pending_started_at = started_at;
            return Err(err);
        }
        self.record_buffered_flush(restore.len(), chunks, started_at);
        Ok(())
    }

    async fn write_direct(&mut self, bytes: Bytes) -> Result<(), WriteError> {
        let len = bytes.len();
        self.enqueue_fire_and_forget(bytes).await?;
        self.record_direct_flush(len);
        Ok(())
    }

    async fn buffer_write(&mut self, bytes: Bytes) -> Result<(), WriteError> {
        self.start_buffered_flush_timer();
        self.pending.extend_from_slice(&bytes);
        self.record_pending_chunk();

        if self.pending.len() >= self.threshold_bytes {
            self.flush().await
        } else {
            Ok(())
        }
    }

    fn write_action(&self, len: usize) -> CoalescerWriteAction {
        match (len, self.threshold_bytes, self.pending.is_empty()) {
            (0, _, _) => CoalescerWriteAction::Flush,
            (_, 1, _) => CoalescerWriteAction::FlushThenDirect,
            (len, threshold, true) if len >= threshold => CoalescerWriteAction::Direct,
            (len, threshold, false) if len >= threshold && self.pending.len() < threshold => {
                CoalescerWriteAction::FlushThenDirect
            }
            _ => CoalescerWriteAction::Buffer,
        }
    }

    async fn enqueue_fire_and_forget(&self, bytes: Bytes) -> Result<(), WriteError> {
        if let Err(err) = self.handoff.try_write_fire_and_forget(bytes) {
            self.handoff.write_fire_and_forget(err.into_bytes()).await?;
        }
        Ok(())
    }

    fn start_buffered_flush_timer(&mut self) {
        if self.pending.is_empty() && self.stats.is_some() {
            self.pending_started_at = Some(Instant::now());
        }
    }

    fn record_input(&mut self, bytes: usize) {
        let Some(stats) = &mut self.stats else {
            return;
        };
        stats.input_chunks = stats.input_chunks.saturating_add(1);
        stats.input_bytes = stats.input_bytes.saturating_add(bytes);
    }

    fn record_pending_chunk(&mut self) {
        let Some(stats) = &mut self.stats else {
            return;
        };
        self.pending_chunks = self.pending_chunks.saturating_add(1);
        stats.max_pending_bytes = stats.max_pending_bytes.max(self.pending.len());
    }

    fn record_direct_flush(&mut self, bytes: usize) {
        let Some(stats) = &mut self.stats else {
            return;
        };
        stats.flushes = stats.flushes.saturating_add(1);
        stats.flush_bytes = stats.flush_bytes.saturating_add(bytes);
        stats.direct_flushes = stats.direct_flushes.saturating_add(1);
        stats.max_chunks_per_flush = stats.max_chunks_per_flush.max(1);
        stats.max_bytes_per_flush = stats.max_bytes_per_flush.max(bytes);
    }

    fn record_buffered_flush(&mut self, bytes: usize, chunks: usize, started_at: Option<Instant>) {
        let Some(stats) = &mut self.stats else {
            return;
        };
        stats.flushes = stats.flushes.saturating_add(1);
        stats.flush_bytes = stats.flush_bytes.saturating_add(bytes);
        stats.buffered_flushes = stats.buffered_flushes.saturating_add(1);
        stats.buffered_input_chunks = stats.buffered_input_chunks.saturating_add(chunks);
        stats.max_chunks_per_flush = stats.max_chunks_per_flush.max(chunks);
        stats.max_bytes_per_flush = stats.max_bytes_per_flush.max(bytes);
        if let Some(started_at) = started_at {
            let wait = nanos_u64(started_at.elapsed());
            stats.total_flush_wait_nanos = stats
                .total_flush_wait_nanos
                .saturating_add(u128::from(wait));
            stats.max_flush_wait_nanos = stats.max_flush_wait_nanos.max(wait);
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
            waiters: AtomicUsize::new(0),
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
            let waiter = self.waiter();
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.try_acquire(bytes) {
                Ok(acquired) => {
                    drop(waiter);
                    return Ok(acquired);
                }
                Err(BudgetAcquireError::Closed) => {
                    drop(waiter);
                    return Err(BudgetAcquireError::Closed);
                }
                Err(BudgetAcquireError::LimitExceeded { .. }) => {}
            }
            notified.await;
            drop(waiter);
        }
    }

    fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let previous = self.pending.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(previous >= bytes, "released more bytes than acquired");
        if self.waiters.load(Ordering::Acquire) > 0 {
            self.notify.notify_waiters();
        }
    }

    fn pending(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn waiter(&self) -> BudgetWaiter<'_> {
        self.waiters.fetch_add(1, Ordering::AcqRel);
        BudgetWaiter(&self.waiters)
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
    let released = take_budget(requests);
    budget.release(released);
    for request in requests {
        if let Some(completion) = request.completion.take() {
            let _ = completion.send(WriteCompletion { result: Ok(()) });
        }
    }
}

fn complete_closed(budget: &Budget, requests: &mut [WriteRequest]) {
    let released = take_budget(requests);
    budget.release(released);
    for request in requests {
        if let Some(completion) = request.completion.take() {
            let _ = completion.send(WriteCompletion {
                result: Err(WriteError::Closed),
            });
        }
    }
}

fn complete_request(budget: &Budget, request: &mut WriteRequest, result: Result<(), WriteError>) {
    budget.release(request.budget_bytes);
    request.budget_bytes = 0;
    if let Some(completion) = request.completion.take() {
        let _ = completion.send(WriteCompletion { result });
    }
}

fn take_budget(requests: &mut [WriteRequest]) -> usize {
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
        let mut coalescer = WriteCoalescer::with_threshold_and_stats(handoff, 4);

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
        let stats = coalescer.stats();
        assert_eq!(stats.input_chunks, 2);
        assert_eq!(stats.input_bytes, 4);
        assert_eq!(stats.flushes, 1);
        assert_eq!(stats.buffered_flushes, 1);
        assert_eq!(stats.direct_flushes, 0);
        assert_eq!(stats.flush_bytes, 4);
        assert_eq!(stats.max_chunks_per_flush, 2);
        assert_eq!(stats.max_bytes_per_flush, 4);
        assert_eq!(stats.max_pending_bytes, 4);
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
        let mut coalescer =
            WriteCoalescer::with_config_and_stats(handoff, WriteCoalescerConfig::immediate());

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
        let stats = coalescer.stats();
        assert_eq!(stats.input_chunks, 1);
        assert_eq!(stats.flushes, 1);
        assert_eq!(stats.buffered_flushes, 0);
        assert_eq!(stats.direct_flushes, 1);
        assert_eq!(stats.avg_bytes_per_flush(), 3.0);
        assert_eq!(stats.avg_chunks_per_flush(), 1.0);
    }

    #[test]
    fn coalescer_stats_merge_preserves_totals_and_maxima() {
        let mut left = WriteCoalescerStats {
            input_chunks: 2,
            input_bytes: 8,
            flushes: 1,
            flush_bytes: 8,
            buffered_flushes: 1,
            direct_flushes: 0,
            buffered_input_chunks: 2,
            max_chunks_per_flush: 2,
            max_bytes_per_flush: 8,
            max_pending_bytes: 8,
            total_flush_wait_nanos: 10,
            max_flush_wait_nanos: 10,
        };
        left.merge(WriteCoalescerStats {
            input_chunks: 1,
            input_bytes: 16,
            flushes: 1,
            flush_bytes: 16,
            buffered_flushes: 0,
            direct_flushes: 1,
            buffered_input_chunks: 0,
            max_chunks_per_flush: 1,
            max_bytes_per_flush: 16,
            max_pending_bytes: 0,
            total_flush_wait_nanos: 0,
            max_flush_wait_nanos: 0,
        });

        assert_eq!(left.input_chunks, 3);
        assert_eq!(left.input_bytes, 24);
        assert_eq!(left.flushes, 2);
        assert_eq!(left.flush_bytes, 24);
        assert_eq!(left.buffered_flushes, 1);
        assert_eq!(left.direct_flushes, 1);
        assert_eq!(left.buffered_input_chunks, 2);
        assert_eq!(left.max_chunks_per_flush, 2);
        assert_eq!(left.max_bytes_per_flush, 16);
        assert_eq!(left.max_pending_bytes, 8);
        assert_eq!(left.total_flush_wait_nanos, 10);
        assert_eq!(left.max_flush_wait_nanos, 10);
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
