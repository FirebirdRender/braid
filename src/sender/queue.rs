use std::cell::Cell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use tokio::sync::mpsc;

/// Per-worker queue statistics, accessible via `Arc<QueueWorkerStats>`.
#[derive(Debug, Default)]
pub struct QueueWorkerStats {
    /// Total bytes of fragment payload + header data enqueued.
    pub pending_bytes: AtomicU64,
    /// Total number of fragments enqueued.
    pub pending_fragments: AtomicU64,
    /// Total number of send errors encountered by this worker.
    pub errors: AtomicU64,
}

impl QueueWorkerStats {
    fn record_enqueue(&self, bytes: usize) {
        self.pending_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.pending_fragments.fetch_add(1, Ordering::Relaxed);
    }

    fn record_dequeue(&self, bytes: usize) {
        self.pending_bytes
            .fetch_sub(bytes as u64, Ordering::Relaxed);
        self.pending_fragments.fetch_sub(1, Ordering::Relaxed);
    }

    fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
}

/// Aggregate queue manager statistics.
#[derive(Debug, Default)]
pub struct QueueManagerStats {
    /// Total fragments dispatched to workers.
    pub fragments_dispatched: AtomicU64,
    /// Total fragments retransmitted (pushed to head of queue).
    pub fragments_retransmitted: AtomicU64,
    /// Total fragments redistributed from failed workers.
    pub fragments_redistributed: AtomicU64,
    /// Number of times all workers were down (shutdown signal sent).
    pub all_workers_down_events: AtomicU64,
    /// Number of times backpressure was signalled.
    pub backpressure_events: AtomicU64,
    /// Total bytes rate-limited (delayed by token bucket).
    pub rate_limited_bytes: AtomicU64,
    /// Total time spent rate-limiting in nanoseconds.
    pub rate_limit_ns: AtomicU64,
}

impl QueueManagerStats {
    fn record_dispatch(&self) {
        self.fragments_dispatched.fetch_add(1, Ordering::Relaxed);
    }

    fn record_retransmit(&self) {
        self.fragments_retransmitted.fetch_add(1, Ordering::Relaxed);
    }

    fn record_redistribution(&self) {
        self.fragments_redistributed.fetch_add(1, Ordering::Relaxed);
    }

    fn record_all_down(&self) {
        self.all_workers_down_events.fetch_add(1, Ordering::Relaxed);
    }

    fn record_backpressure(&self) {
        self.backpressure_events.fetch_add(1, Ordering::Relaxed);
    }

    fn record_rate_limited(&self, bytes: u64, nanos: u64) {
        self.rate_limited_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.rate_limit_ns.fetch_add(nanos, Ordering::Relaxed);
    }
}

/// Per-worker state tracked by the queue manager.
struct WorkerQueue {
    /// The queue of fragments awaiting dispatch to this worker.
    queue: VecDeque<Bytes>,
    /// Whether this worker is currently active (not failed).
    active: bool,
    /// Channel sender to push fragments to the worker's UDP send task.
    tx: mpsc::Sender<Bytes>,
    /// Per-worker statistics.
    stats: Arc<QueueWorkerStats>,
}

/// LACP-like sender queue manager.
///
/// Dispatches fragments to workers using a next-available (least-loaded) strategy.
/// Supports retransmit-to-head, worker failure redistribution, all-workers-failure
/// shutdown, and backpressure signalling to the splitter.
pub struct QueueManager {
    workers: Vec<WorkerQueue>,
    last_worker: Cell<usize>,
    high_watermark: u64,
    backpressure_tx: Option<mpsc::Sender<bool>>,
    stats: Arc<QueueManagerStats>,
    max_rate_bytes_per_sec: u64,
    rate_bucket: Cell<u64>,
    rate_last_check: Cell<Instant>,
    progress_bytes: Option<Arc<AtomicU64>>,
    last_chunk_id: Cell<u32>,
}

impl QueueManager {
    /// Create a new `QueueManager`.
    ///
    /// * `worker_count` - Number of worker queues to manage.
    /// * `high_watermark` - Per-worker pending byte threshold for backpressure.
    /// * `backpressure_tx` - Optional channel to signal splitter to pause/resume.
    pub fn new(
        worker_count: usize,
        high_watermark: u64,
        backpressure_tx: Option<mpsc::Sender<bool>>,
    ) -> Self {
        assert!(worker_count > 0, "worker_count must be positive");

        Self {
            workers: Vec::with_capacity(worker_count),
            last_worker: Cell::new(0),
            high_watermark,
            backpressure_tx,
            stats: Arc::new(QueueManagerStats::default()),
            max_rate_bytes_per_sec: 0,
            rate_bucket: Cell::new(0),
            rate_last_check: Cell::new(Instant::now()),
            progress_bytes: None,
            last_chunk_id: Cell::new(0),
        }
    }

    pub fn set_progress_bytes(&mut self, counter: Arc<AtomicU64>) {
        self.progress_bytes = Some(counter);
    }

    fn report_progress(&self, bytes: usize) {
        if let Some(ref counter) = self.progress_bytes {
            counter.fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }

    /// Set the maximum send rate in bytes per second.
    /// A value of 0 disables rate limiting.
    pub fn set_max_rate(&mut self, bytes_per_sec: u64) {
        self.max_rate_bytes_per_sec = bytes_per_sec;
        self.rate_bucket.set(0);
        self.rate_last_check.set(Instant::now());
    }

    /// Register a worker with the queue manager.
    ///
    /// Each worker provides its channel sender and optional per-worker stats.
    pub fn add_worker(&mut self, tx: mpsc::Sender<Bytes>, stats: Arc<QueueWorkerStats>) {
        self.workers.push(WorkerQueue {
            queue: VecDeque::new(),
            active: true,
            tx,
            stats,
        });
    }

    /// Returns a reference to the aggregate stats.
    pub fn stats(&self) -> &Arc<QueueManagerStats> {
        &self.stats
    }

    /// Dispatch a fragment to the least-loaded active worker.
    ///
    /// Returns `Ok(worker_index)` on success, or `Err` if all workers are down.
    pub fn dispatch(&mut self, fragment: Bytes) -> Result<usize, &'static str> {
        let frag_bytes = fragment.len();

        // Rate limit check: block if we're exceeding the configured max rate.
        self.check_rate_limit(frag_bytes);

        // Find the active worker with the smallest pending_bytes.
        let selected = self.select_worker();
        let idx = match selected {
            Some(i) => i,
            None => {
                // All workers are down — signal shutdown.
                self.stats.record_all_down();
                return Err("all workers are down");
            }
        };

        // Enqueue the fragment to the selected worker's local queue.
        self.workers[idx].queue.push_back(fragment);
        self.workers[idx].stats.record_enqueue(frag_bytes);
        self.stats.record_dispatch();
        self.report_progress(frag_bytes);

        // Try to flush the selected worker's queue to its channel.
        self.flush_worker(idx);

        // Check backpressure after dispatch.
        self.check_backpressure();

        Ok(idx)
    }

    /// Dispatch an entire batch of fragments to a single least-loaded worker.
    ///
    /// Selects one worker via `select_worker()`, enqueues all fragments to that
    /// worker's local queue, flushes once, then checks backpressure.
    ///
    /// Returns `Ok(worker_index)` on success, or `Err` if all workers are down.
    pub fn dispatch_batch(&mut self, batch: Vec<Bytes>) -> Result<usize, &'static str> {
        // Compute total batch size for rate limiting.
        let batch_bytes: usize = batch.iter().map(|f| f.len()).sum();
        self.check_rate_limit(batch_bytes);

        let selected = self.select_worker();
        let idx = match selected {
            Some(i) => i,
            None => {
                self.stats.record_all_down();
                return Err("all workers are down");
            }
        };

        // Enqueue all fragments to the selected worker's local queue.
        for fragment in &batch {
            let frag_bytes = fragment.len();
            self.workers[idx].stats.record_enqueue(frag_bytes);
            self.stats.record_dispatch();
            let cid = Self::extract_chunk_id(fragment);
            if cid > self.last_chunk_id.get() {
                self.last_chunk_id.set(cid);
            }
        }
        self.report_progress(batch_bytes);
        for fragment in batch {
            self.workers[idx].queue.push_back(fragment);
        }

        // Flush the selected worker's queue to its channel.
        self.flush_worker(idx);

        // Check backpressure: if the selected worker now exceeds the watermark,
        // check whether ALL active workers exceed it (same logic as dispatch).
        if self.workers[idx]
            .stats
            .pending_bytes
            .load(Ordering::Relaxed)
            >= self.high_watermark
        {
            self.check_backpressure();
        }

        Ok(idx)
    }

    /// Retransmit a fragment to the head of a specific worker's queue.
    ///
    /// The worker must be active. Returns `Ok(())` or `Err` if the worker is down.
    pub fn retransmit(
        &mut self,
        worker_index: usize,
        fragment: Bytes,
    ) -> Result<(), &'static str> {
        if worker_index >= self.workers.len() {
            return Err("worker index out of bounds");
        }
        if !self.workers[worker_index].active {
            return Err("worker is down");
        }

        let frag_bytes = fragment.len();
        self.workers[worker_index].queue.push_front(fragment);
        self.workers[worker_index].stats.record_enqueue(frag_bytes);
        self.stats.record_retransmit();

        self.flush_worker(worker_index);
        Ok(())
    }

    /// Mark a worker as failed and redistribute its pending fragments to
    /// other active workers.
    ///
    /// Returns the number of fragments redistributed.
    pub fn mark_worker_failed(&mut self, worker_index: usize) -> usize {
        if worker_index >= self.workers.len() {
            return 0;
        }
        if !self.workers[worker_index].active {
            return 0; // Already failed
        }

        self.workers[worker_index].active = false;
        self.workers[worker_index].stats.record_error();

        // Redistribute pending fragments to other active workers.
        let pending = self.workers[worker_index].queue.drain(..).collect::<Vec<Bytes>>();
        let redistributed = pending.len();

        for fragment in pending {
            // Try to dispatch to another active worker.
            match self.dispatch(fragment) {
                Ok(_) => {
                    self.stats.record_redistribution();
                }
                Err(_) => {
                    // All workers down — fragment is lost.
                    break;
                }
            }
        }

        redistributed
    }

    /// Check and enforce the rate limit by sleeping if necessary.
    /// Uses a token-bucket-like approach: tracks bytes dispatched over time
    /// and sleeps when the configured max rate would be exceeded.
    fn check_rate_limit(&self, bytes: usize) {
        let max_rate = self.max_rate_bytes_per_sec;
        if max_rate == 0 || bytes == 0 {
            return;
        }

        let now = Instant::now();
        let last = self.rate_last_check.get();
        let elapsed_ns = now.duration_since(last).as_nanos() as u64;

        // Accumulate tokens based on elapsed time.
        // max_rate bytes/sec = max_rate / 1_000_000_000 bytes/nanosecond
        let new_tokens = (max_rate as u128 * elapsed_ns as u128 / 1_000_000_000) as u64;
        let mut bucket = self.rate_bucket.get();
        // Cap bucket to 1 second worth of tokens (max burst).
        let max_bucket = max_rate;
        bucket = bucket.saturating_add(new_tokens).min(max_bucket);

        if bytes as u64 > bucket {
            // We need to sleep to stay within the rate limit.
            let deficit = bytes as u64 - bucket;
            // nanos to wait = deficit / (max_rate / 1_000_000_000) = deficit * 1_000_000_000 / max_rate
            let sleep_ns = deficit.saturating_mul(1_000_000_000) / max_rate;
            let sleep_dur = std::time::Duration::from_nanos(sleep_ns);

            // Record stats before sleeping.
            self.stats.record_rate_limited(bytes as u64, sleep_ns);

            let handle = tokio::runtime::Handle::current();
            let _ = handle.block_on(tokio::task::spawn_blocking(move || {
                std::thread::sleep(sleep_dur);
            }));

            // Reset bucket after sleeping — we've waited long enough.
            self.rate_bucket.set(0);
            self.rate_last_check.set(Instant::now());
        } else {
            // Deduct from bucket and update last check time.
            self.rate_bucket.set(bucket - bytes as u64);
            self.rate_last_check.set(now);
        }
    }

    /// Returns the number of active workers.
    pub fn active_worker_count(&self) -> usize {
        self.workers.iter().filter(|w| w.active).count()
    }

    /// Returns the total number of workers (active + failed).
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Check whether all workers are down.
    pub fn all_workers_down(&self) -> bool {
        self.active_worker_count() == 0
    }

    /// Returns the last seen chunk ID (sequence number) from dispatched fragments.
    /// Used by the reconnect flow to determine the resume point.
    pub fn last_chunk_id(&self) -> u32 {
        self.last_chunk_id.get()
    }

    fn extract_chunk_id(fragment: &[u8]) -> u32 {
        if fragment.len() >= 4 {
            u32::from_be_bytes([fragment[0], fragment[1], fragment[2], fragment[3]])
        } else {
            0
        }
    }

    /// Signal the splitter to pause (backpressure).
    pub fn signal_pause(&self) {
        if let Some(ref tx) = self.backpressure_tx {
            let _ = tx.try_send(true);
            self.stats.record_backpressure();
        }
    }

    /// Signal the splitter to resume.
    pub fn signal_resume(&self) {
        if let Some(ref tx) = self.backpressure_tx {
            let _ = tx.try_send(false);
        }
    }

    /// Select the active worker with the least pending load.
    ///
    /// Uses round-robin as a tie-breaker when loads are equal.
    fn select_worker(&self) -> Option<usize> {
        let mut best: Option<usize> = None;
        let mut best_load = u64::MAX;

        // Start searching from last_worker + 1 for fairness.
        let n = self.workers.len();
        let last = self.last_worker.get();
        for offset in 0..n {
            let idx = (last + 1 + offset) % n;
            if !self.workers[idx].active {
                continue;
            }
            let load = self.workers[idx]
                .stats
                .pending_bytes
                .load(Ordering::Relaxed);

            if load < best_load {
                best_load = load;
                best = Some(idx);
                if load == 0 {
                    // Can't beat zero — take it.
                    break;
                }
            }
        }

        if let Some(idx) = best {
            self.last_worker.set(idx);
        }

        best
    }

    /// Flush as many fragments as possible from a worker's local queue to its
    /// mpsc channel (non-blocking via try_send).
    fn flush_worker(&mut self, idx: usize) {
        let worker = &mut self.workers[idx];
        while let Some(fragment) = worker.queue.pop_front() {
            let frag_bytes = fragment.len();
            match worker.tx.try_send(fragment) {
                Ok(()) => {
                    worker.stats.record_dequeue(frag_bytes);
                }
                Err(mpsc::error::TrySendError::Full(item)) => {
                    // Channel full — put it back and stop flushing.
                    worker.queue.push_front(item);
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(item)) => {
                    // Channel closed — worker is effectively dead.
                    worker.active = false;
                    worker.queue.push_front(item);
                    break;
                }
            }
        }
    }

    /// Check whether all active workers exceed the high-watermark and signal
    /// backpressure if so. Also signals resume when backpressure clears.
    pub fn check_backpressure(&self) {
        let active_count = self.active_worker_count();
        if active_count == 0 {
            return;
        }

        let all_exceed = self
            .workers
            .iter()
            .filter(|w| w.active)
            .all(|w| w.stats.pending_bytes.load(Ordering::Relaxed) >= self.high_watermark);

        if all_exceed {
            self.signal_pause();
        } else {
            self.signal_resume();
        }
    }
}

/// Builder for constructing a `QueueManager` with a set of workers.
///
/// This is the primary way to create a fully-configured `QueueManager` with
/// worker channels and stats already wired up.
pub struct QueueManagerBuilder {
    worker_count: usize,
    high_watermark: u64,
    channel_capacity: usize,
    backpressure_tx: Option<mpsc::Sender<bool>>,
    max_rate_bytes_per_sec: u64,
}

/// A pair of a fragment receiver and its associated worker stats.
pub type WorkerReceiver = (mpsc::Receiver<Bytes>, Arc<QueueWorkerStats>);

impl QueueManagerBuilder {
    /// Create a new builder.
    ///
    /// * `worker_count` - Number of workers to manage.
    pub fn new(worker_count: usize) -> Self {
        Self {
            worker_count,
            high_watermark: 1024 * 1024, // 1 MB default
            channel_capacity: 1024,
            backpressure_tx: None,
            max_rate_bytes_per_sec: 0,
        }
    }

    /// Set the per-worker high-watermark for backpressure.
    pub fn high_watermark(mut self, bytes: u64) -> Self {
        self.high_watermark = bytes;
        self
    }

    /// Set the mpsc channel capacity per worker.
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Set the backpressure signal sender.
    pub fn backpressure_tx(mut self, tx: mpsc::Sender<bool>) -> Self {
        self.backpressure_tx = Some(tx);
        self
    }

    /// Set the maximum send rate in bytes per second (0 = unlimited).
    pub fn max_rate(mut self, bytes_per_sec: u64) -> Self {
        self.max_rate_bytes_per_sec = bytes_per_sec;
        self
    }

    /// Build the `QueueManager` and return it along with the per-worker
    /// receivers and stats.
    pub fn build(self) -> (QueueManager, Vec<WorkerReceiver>) {
        let mut manager =
            QueueManager::new(self.worker_count, self.high_watermark, self.backpressure_tx);
        if self.max_rate_bytes_per_sec > 0 {
            manager.set_max_rate(self.max_rate_bytes_per_sec);
        }

        let mut receivers = Vec::with_capacity(self.worker_count);

        for _ in 0..self.worker_count {
            let (tx, rx) = mpsc::channel(self.channel_capacity);
            let stats = Arc::new(QueueWorkerStats::default());
            manager.add_worker(tx, stats.clone());
            receivers.push((rx, stats));
        }

        (manager, receivers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // ─── Helper: create a QueueManager with N workers ───────────────────────

    fn make_manager(worker_count: usize) -> (QueueManager, Vec<WorkerReceiver>) {
        QueueManagerBuilder::new(worker_count)
            .channel_capacity(64)
            .build()
    }

    fn make_manager_with_bp(
        worker_count: usize,
        bp_tx: mpsc::Sender<bool>,
    ) -> (QueueManager, Vec<WorkerReceiver>) {
        QueueManagerBuilder::new(worker_count)
            .channel_capacity(64)
            .backpressure_tx(bp_tx)
            .build()
    }

    // ─── Dispatch to least-loaded worker ────────────────────────────────────

    #[test]
    fn dispatches_to_first_worker_when_all_empty() {
        let (mut mgr, _rx) = make_manager(3);
        let result = mgr.dispatch(Bytes::from(vec![0u8; 100]));
        assert!(result.is_ok());
        assert_eq!(mgr.stats.fragments_dispatched.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn dispatches_to_least_loaded_worker() {
        let (mut mgr, receivers) = make_manager(3);

        // Manually add load to worker 0 and 1 by pushing to their local queues
        mgr.workers[0]
            .stats
            .pending_bytes
            .store(500, Ordering::Relaxed);
        mgr.workers[1]
            .stats
            .pending_bytes
            .store(300, Ordering::Relaxed);
        // Worker 2 has 0 load

        let result = mgr.dispatch(Bytes::from(vec![0u8; 100]));
        assert!(result.is_ok());
        // Worker 2 has least load (0)
        assert_eq!(result.unwrap(), 2);

        // Clean up: drop receivers to avoid test hanging
        drop(receivers);
    }

    #[test]
    fn dispatch_returns_error_when_all_workers_down() {
        let (mut mgr, receivers) = make_manager(3);

        // Mark all workers as failed
        for i in 0..3 {
            mgr.workers[i].active = false;
        }

        let result = mgr.dispatch(Bytes::from(vec![0u8; 100]));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "all workers are down");

        // Verify stats
        assert_eq!(mgr.stats.all_workers_down_events.load(Ordering::Relaxed), 1);

        drop(receivers);
    }

    // ─── Retransmit to head of queue ────────────────────────────────────────

    #[test]
    fn retransmit_pushes_to_head_of_worker_queue() {
        // Use tiny channel capacity so fragments stay in local queues
        let (mut mgr, receivers): (QueueManager, _) =
            QueueManagerBuilder::new(2).channel_capacity(1).build();

        // First dispatch fills worker 0's channel
        mgr.dispatch(Bytes::from(vec![1u8; 10])).ok();
        // Second dispatch stays in local queue
        mgr.dispatch(Bytes::from(vec![2u8; 10])).ok();

        // Retransmit a fragment to worker 0's head
        let retransmit_data = Bytes::from(vec![99u8; 10]);
        let result = mgr.retransmit(0, retransmit_data);
        assert!(result.is_ok());

        // Verify the retransmitted fragment is at the head
        let head = mgr.workers[0].queue.front().unwrap();
        assert_eq!(head[0], 99);

        assert_eq!(mgr.stats.fragments_retransmitted.load(Ordering::Relaxed), 1);

        drop(receivers);
    }

    #[test]
    fn retransmit_to_invalid_worker_returns_error() {
        let (mut mgr, receivers) = make_manager(2);
        let result = mgr.retransmit(5, Bytes::from(vec![0u8; 10]));
        assert!(result.is_err());
        drop(receivers);
    }

    #[test]
    fn retransmit_to_failed_worker_returns_error() {
        let (mut mgr, receivers) = make_manager(2);
        mgr.workers[0].active = false;
        let result = mgr.retransmit(0, Bytes::from(vec![0u8; 10]));
        assert!(result.is_err());
        drop(receivers);
    }

    // ─── Worker failure redistribution ──────────────────────────────────────

    #[test]
    fn mark_worker_failed_redistributes_pending() {
        // Use tiny channel capacity so fragments stay in local queues
        let (mut mgr, receivers): (QueueManager, _) =
            QueueManagerBuilder::new(2).channel_capacity(1).build();

        // Manually push fragments to worker 0's local queue
        mgr.workers[0].queue.push_back(Bytes::from(vec![1u8; 10]));
        mgr.workers[0].queue.push_back(Bytes::from(vec![2u8; 10]));
        mgr.workers[0].stats.record_enqueue(10);
        mgr.workers[0].stats.record_enqueue(10);

        let pending_before = mgr.workers[0].queue.len();
        assert_eq!(pending_before, 2);

        let redistributed = mgr.mark_worker_failed(0);
        assert_eq!(redistributed, pending_before);
        assert!(!mgr.workers[0].active);
        assert_eq!(mgr.workers[0].queue.len(), 0);

        assert!(mgr.stats.fragments_redistributed.load(Ordering::Relaxed) >= pending_before as u64);

        drop(receivers);
    }

    #[test]
    fn mark_worker_failed_already_failed_returns_zero() {
        let (mut mgr, receivers) = make_manager(2);
        mgr.workers[0].active = false;
        let result = mgr.mark_worker_failed(0);
        assert_eq!(result, 0);
        drop(receivers);
    }

    #[test]
    fn mark_worker_failed_invalid_index_returns_zero() {
        let (mut mgr, receivers) = make_manager(2);
        let result = mgr.mark_worker_failed(10);
        assert_eq!(result, 0);
        drop(receivers);
    }

    // ─── All-workers-down detection ─────────────────────────────────────────

    #[test]
    fn all_workers_down_detected() {
        let (mut mgr, receivers) = make_manager(3);
        assert!(!mgr.all_workers_down());

        mgr.workers[0].active = false;
        mgr.workers[1].active = false;
        mgr.workers[2].active = false;

        assert!(mgr.all_workers_down());
        assert_eq!(mgr.active_worker_count(), 0);

        drop(receivers);
    }

    // ─── Active worker count ────────────────────────────────────────────────

    #[test]
    fn active_worker_count_tracks_failures() {
        let (mut mgr, receivers) = make_manager(4);
        assert_eq!(mgr.active_worker_count(), 4);

        mgr.workers[0].active = false;
        assert_eq!(mgr.active_worker_count(), 3);

        mgr.workers[2].active = false;
        assert_eq!(mgr.active_worker_count(), 2);

        drop(receivers);
    }

    // ─── Backpressure signal ────────────────────────────────────────────────

    #[tokio::test]
    async fn backpressure_signalled_when_all_workers_exceed_watermark() {
        let (bp_tx, mut bp_rx) = mpsc::channel::<bool>(16);
        let (mut mgr, receivers) = make_manager_with_bp(2, bp_tx);

        // Set low watermark and high pending bytes on both workers
        mgr.high_watermark = 50;
        mgr.workers[0]
            .stats
            .pending_bytes
            .store(100, Ordering::Relaxed);
        mgr.workers[1]
            .stats
            .pending_bytes
            .store(100, Ordering::Relaxed);

        // Trigger backpressure check
        mgr.check_backpressure();

        // Should receive a pause signal
        let signal = tokio::time::timeout(Duration::from_secs(1), bp_rx.recv())
            .await
            .expect("should receive backpressure signal")
            .expect("channel should be open");
        assert!(signal); // true = pause

        assert!(mgr.stats.backpressure_events.load(Ordering::Relaxed) >= 1);

        drop(receivers);
    }

    #[tokio::test]
    async fn check_backpressure_full_lifecycle_pause_and_resume() {
        let (bp_tx, mut bp_rx) = mpsc::channel::<bool>(16);
        let (mut mgr, receivers) = make_manager_with_bp(2, bp_tx);
        mgr.high_watermark = 100;

        // Phase 1: All workers below watermark → resume signal
        mgr.workers[0].stats.pending_bytes.store(50, Ordering::Relaxed);
        mgr.workers[1].stats.pending_bytes.store(50, Ordering::Relaxed);
        mgr.check_backpressure();
        let sig = tokio::time::timeout(Duration::from_millis(200), bp_rx.recv())
            .await
            .expect("phase 1: timeout")
            .expect("phase 1: channel closed");
        assert!(!sig, "phase 1: should be resume (false) when all below watermark");

        // Phase 2: All workers exceed watermark → pause signal
        mgr.workers[0].stats.pending_bytes.store(200, Ordering::Relaxed);
        mgr.workers[1].stats.pending_bytes.store(200, Ordering::Relaxed);
        mgr.check_backpressure();
        let sig = tokio::time::timeout(Duration::from_millis(200), bp_rx.recv())
            .await
            .expect("phase 2: timeout")
            .expect("phase 2: channel closed");
        assert!(sig, "phase 2: should be pause (true) when all exceed watermark");

        // Phase 3: Workers drain below watermark → resume signal again
        mgr.workers[0].stats.pending_bytes.store(50, Ordering::Relaxed);
        mgr.workers[1].stats.pending_bytes.store(50, Ordering::Relaxed);
        mgr.check_backpressure();
        let sig = tokio::time::timeout(Duration::from_millis(200), bp_rx.recv())
            .await
            .expect("phase 3: timeout")
            .expect("phase 3: channel closed");
        assert!(!sig, "phase 3: should be resume (false) when workers drain below watermark");

        // Phase 4: Still below watermark → resume is idempotent (duplicate false is harmless)
        mgr.workers[0].stats.pending_bytes.store(30, Ordering::Relaxed);
        mgr.check_backpressure();
        let sig = tokio::time::timeout(Duration::from_millis(200), bp_rx.recv())
            .await
            .expect("phase 4: timeout")
            .expect("phase 4: channel closed");
        assert!(!sig, "phase 4: should be resume (false) — idempotent");

        drop(receivers);
    }

    #[tokio::test]
    async fn backpressure_not_signalled_when_one_worker_below_watermark() {
        let (bp_tx, mut bp_rx) = mpsc::channel::<bool>(16);
        let (mut mgr, receivers) = make_manager_with_bp(2, bp_tx);

        mgr.high_watermark = 50;
        mgr.workers[0]
            .stats
            .pending_bytes
            .store(100, Ordering::Relaxed);
        mgr.workers[1]
            .stats
            .pending_bytes
            .store(10, Ordering::Relaxed); // Below watermark

        mgr.check_backpressure();

        // Should receive a resume signal (false) since not all workers exceed watermark
        let result = tokio::time::timeout(Duration::from_millis(200), bp_rx.recv()).await;
        let sig = result
            .expect("check_backpressure should send a signal quickly")
            .expect("bp_tx should not be closed");
        assert!(!sig, "should receive false (resume) signal");

        drop(receivers);
    }

    // ─── Signal pause/resume ────────────────────────────────────────────────

    #[tokio::test]
    async fn signal_pause_and_resume() {
        let (bp_tx, mut bp_rx) = mpsc::channel::<bool>(16);
        let (mgr, receivers) = make_manager_with_bp(2, bp_tx);

        mgr.signal_pause();
        let sig = bp_rx.recv().await.unwrap();
        assert!(sig);

        mgr.signal_resume();
        let sig = bp_rx.recv().await.unwrap();
        assert!(!sig);

        drop(receivers);
    }

    // ─── Builder creates correct number of workers ──────────────────────────

    #[test]
    fn builder_creates_correct_worker_count() {
        let (mgr, receivers) = make_manager(5);
        assert_eq!(mgr.worker_count(), 5);
        assert_eq!(receivers.len(), 5);
    }

    #[test]
    fn builder_sets_high_watermark() {
        let (mgr, receivers): (QueueManager, _) = QueueManagerBuilder::new(2)
            .high_watermark(999)
            .channel_capacity(64)
            .build();
        assert_eq!(mgr.high_watermark, 999);
        drop(receivers);
    }

    // ─── Dispatch with channel flush ────────────────────────────────────────

    #[tokio::test]
    async fn dispatch_flushes_to_channel_when_space_available() {
        let (mut mgr, mut receivers) = make_manager(2);

        // Dispatch a fragment — it should flush to the channel
        let data = Bytes::from(vec![42u8; 64]);
        let idx = mgr.dispatch(data.clone()).expect("dispatch ok");

        // The selected worker's local queue should be empty (flushed to channel)
        assert_eq!(mgr.workers[idx].queue.len(), 0);

        // Verify it arrived on the correct receiver side
        let received = tokio::time::timeout(Duration::from_secs(1), receivers[idx].0.recv())
            .await
            .expect("should receive fragment")
            .expect("should be Some");
        assert_eq!(received, data);

        drop(receivers);
    }

    // ─── Stats tracking ─────────────────────────────────────────────────────

    #[test]
    fn queue_worker_stats_track_enqueue_dequeue() {
        let stats = Arc::new(QueueWorkerStats::default());
        stats.record_enqueue(100);
        assert_eq!(stats.pending_bytes.load(Ordering::Relaxed), 100);
        assert_eq!(stats.pending_fragments.load(Ordering::Relaxed), 1);

        stats.record_dequeue(100);
        assert_eq!(stats.pending_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(stats.pending_fragments.load(Ordering::Relaxed), 0);

        stats.record_error();
        assert_eq!(stats.errors.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn queue_manager_stats_track_all_operations() {
        let stats = Arc::new(QueueManagerStats::default());
        stats.record_dispatch();
        stats.record_retransmit();
        stats.record_redistribution();
        stats.record_all_down();
        stats.record_backpressure();

        assert_eq!(stats.fragments_dispatched.load(Ordering::Relaxed), 1);
        assert_eq!(stats.fragments_retransmitted.load(Ordering::Relaxed), 1);
        assert_eq!(stats.fragments_redistributed.load(Ordering::Relaxed), 1);
        assert_eq!(stats.all_workers_down_events.load(Ordering::Relaxed), 1);
        assert_eq!(stats.backpressure_events.load(Ordering::Relaxed), 1);
    }

    // ─── Dispatch with full channel stays in local queue ────────────────────

    #[tokio::test]
    async fn dispatch_queues_locally_when_channel_full() {
        let (mut mgr, receivers): (QueueManager, _) = QueueManagerBuilder::new(1)
            .channel_capacity(1) // Tiny channel
            .build();

        // First dispatch fills the channel
        mgr.dispatch(Bytes::from(vec![1u8; 10])).ok();

        // Second dispatch should stay in local queue since channel is full
        mgr.dispatch(Bytes::from(vec![2u8; 10])).ok();

        // Worker 0 should have 1 item in local queue (channel capacity is 1)
        assert_eq!(mgr.workers[0].queue.len(), 1);

        // Drain the channel
        drop(receivers);
    }

    // ─── last_chunk_id tracking ───────────────────────────────────────────

    #[test]
    fn last_chunk_id_tracks_max_chunk_id_from_batch() {
        let (mut mgr, receivers) = make_manager(2);
        assert_eq!(mgr.last_chunk_id(), 0, "initial last_chunk_id should be 0");

        // Create fragments with known chunk_ids in their first 4 bytes (BE).
        let fragment_1 = Bytes::from(vec![0u8, 0u8, 0u8, 5u8, 0xAA, 0xBB]); // chunk_id=5
        let fragment_2 = Bytes::from(vec![0u8, 0u8, 0u8, 3u8, 0xCC, 0xDD]); // chunk_id=3
        let batch = vec![fragment_1, fragment_2];
        let _ = mgr.dispatch_batch(batch).ok();
        assert_eq!(mgr.last_chunk_id(), 5, "should track max chunk_id=5");

        let fragment_3 = Bytes::from(vec![0u8, 0u8, 0u8, 10u8]); // chunk_id=10
        let batch = vec![fragment_3];
        let _ = mgr.dispatch_batch(batch).ok();
        assert_eq!(mgr.last_chunk_id(), 10, "should update to chunk_id=10");

        drop(receivers);
    }

    #[test]
    fn extract_chunk_id_parses_big_endian_u32() {
        // chunk_id=0x01020304 encoded as BE bytes
        let fragment = Bytes::from(vec![0x01, 0x02, 0x03, 0x04, 0xFF]);
        assert_eq!(QueueManager::extract_chunk_id(&fragment), 0x01020304);
    }

    #[test]
    fn extract_chunk_id_returns_zero_for_short_slice() {
        let fragment = Bytes::from(vec![0x01, 0x02]); // less than 4 bytes
        assert_eq!(QueueManager::extract_chunk_id(&fragment), 0);
    }

    #[test]
    fn extract_chunk_id_zero_for_empty() {
        let fragment: Bytes = Bytes::new();
        assert_eq!(QueueManager::extract_chunk_id(&fragment), 0);
    }
}
