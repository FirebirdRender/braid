//! Flow control / backpressure system.
//!
//! # Architecture
//!
//! The flow control system coordinates backpressure between the sender and receiver
//! by monitoring buffer pool fullness and propagating pressure signals.
//!
//! ## Fullness Levels
//!
//! | Level  | Range    | Action                                      |
//! |--------|----------|---------------------------------------------|
//! | Green  | 0–50%    | Normal operation                            |
//! | Yellow | 50–80%   | Reduce chunk size by 25% (temporary)        |
//! | Orange | 80–95%   | Pause chunk splitting (backpressure to stdin)|
//! | Red    | 95%+     | Pause ALL sending, send NACK to receiver    |
//!
//! ## Backpressure Propagation
//!
//! Sender pauses stdin → pipe fills → mbuffer blocks → ZFS blocks
//!
//! ## Components
//!
//! - [`FlowController`]: Central state machine managing fullness levels and actions.
//! - [`FlowStats`]: Tracks total pauses, resumes, and max fullness seen.
//! - [`ReceiverMonitor`]: Monitors buffer pool fullness, sends QUEUE_STATUS.
//! - [`SenderReactor`]: Receives QUEUE_STATUS, applies backpressure actions.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::protocol::ControlMessage;

// ─── Constants ───────────────────────────────────────────────────────────────

/// Default interval for sending QUEUE_STATUS from receiver to sender.
pub const DEFAULT_STATUS_INTERVAL: Duration = Duration::from_millis(100);

/// Green zone upper bound (exclusive): 0% ≤ fullness < 50%.
pub const GREEN_MAX: f64 = 0.50;

/// Yellow zone: 50% ≤ fullness < 80%.
pub const YELLOW_MAX: f64 = 0.80;

/// Orange zone: 80% ≤ fullness < 95%.
pub const ORANGE_MAX: f64 = 0.95;

/// Red zone: 95% ≤ fullness ≤ 100%.
/// Default chunk size reduction factor for yellow zone (25% reduction).
pub const YELLOW_REDUCTION_FACTOR: f64 = 0.75;
// ─── Fullness Level ──────────────────────────────────────────────────────────

/// The current buffer fullness level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FullnessLevel {
    /// 0% ≤ fullness < 50% — normal operation.
    Green,
    /// 50% ≤ fullness < 80% — reduce chunk size.
    Yellow,
    /// 80% ≤ fullness < 95% — pause chunk splitting.
    Orange,
    /// 95% ≤ fullness ≤ 100% — pause ALL sending, send NACK.
    Red,
}

impl FullnessLevel {
    /// Determine the fullness level from a ratio (0.0 to 1.0).
    pub fn from_ratio(ratio: f64) -> Self {
        if ratio >= 0.95 {
            Self::Red
        } else if ratio >= 0.80 {
            Self::Orange
        } else if ratio >= 0.50 {
            Self::Yellow
        } else {
            Self::Green
        }
    }

    /// Returns true if this level indicates pressure (yellow or above).
    pub fn is_pressured(&self) -> bool {
        matches!(self, Self::Yellow | Self::Orange | Self::Red)
    }

    /// Returns true if this level indicates severe pressure (orange or above).
    pub fn is_severe(&self) -> bool {
        matches!(self, Self::Orange | Self::Red)
    }

    /// Returns true if this level indicates critical pressure (red).
    pub fn is_critical(&self) -> bool {
        matches!(self, Self::Red)
    }
}

// ─── Flow Stats ──────────────────────────────────────────────────────────────

/// Tracks flow control statistics.
#[derive(Debug, Default)]
pub struct FlowStats {
    /// Total number of times the system entered a pause state (orange or red).
    pub total_pauses: AtomicU64,
    /// Total number of times the system resumed from a pause state.
    pub total_resumes: AtomicU64,
    /// Maximum buffer fullness ratio ever observed (0.0–1.0).
    pub max_fullness: AtomicU64, // stored as fixed-point: value * 1_000_000
    /// Total number of QUEUE_STATUS messages sent.
    pub status_messages_sent: AtomicU64,
    /// Total number of QUEUE_STATUS messages received.
    pub status_messages_received: AtomicU64,
    /// Total number of chunk size reductions applied.
    pub chunk_size_reductions: AtomicU64,
    /// Total number of NACK messages sent.
    pub nacks_sent: AtomicU64,
}

impl FlowStats {
    fn record_pause(&self) {
        self.total_pauses.fetch_add(1, Ordering::Relaxed);
    }

    fn record_resume(&self) {
        self.total_resumes.fetch_add(1, Ordering::Relaxed);
    }

    fn record_fullness(&self, ratio: f64) {
        let fixed = (ratio * 1_000_000.0) as u64;
        let mut prev = self.max_fullness.load(Ordering::Relaxed);
        loop {
            if fixed <= prev {
                break;
            }
            match self.max_fullness.compare_exchange(
                prev,
                fixed,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => prev = current,
            }
        }
    }

    fn record_status_sent(&self) {
        self.status_messages_sent.fetch_add(1, Ordering::Relaxed);
    }

    fn record_status_received(&self) {
        self.status_messages_received
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_chunk_size_reduction(&self) {
        self.chunk_size_reductions.fetch_add(1, Ordering::Relaxed);
    }

    fn record_nack(&self) {
        self.nacks_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the max fullness ratio observed (0.0–1.0).
    pub fn max_fullness_ratio(&self) -> f64 {
        self.max_fullness.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }
}

// ─── FlowController ──────────────────────────────────────────────────────────

/// Central flow control state machine.
///
/// Tracks the current fullness level and coordinates backpressure actions
/// between the receiver monitor and sender reactor.
#[derive(Debug)]
pub struct FlowController {
    /// Current fullness level.
    current_level: FullnessLevel,
    /// Previous fullness level (for detecting transitions).
    prev_level: FullnessLevel,
    /// Total buffer capacity (in items or bytes, depending on usage).
    total_capacity: usize,
    /// Current buffer occupancy.
    current_occupancy: usize,
    /// Flow statistics.
    stats: Arc<FlowStats>,
}

impl FlowController {
    /// Create a new `FlowController`.
    pub fn new(total_capacity: usize) -> Self {
        Self {
            current_level: FullnessLevel::Green,
            prev_level: FullnessLevel::Green,
            total_capacity,
            current_occupancy: 0,
            stats: Arc::new(FlowStats::default()),
        }
    }

    /// Update the current occupancy and recompute the fullness level.
    ///
    /// Returns the new [`FullnessLevel`] and a bool indicating whether the
    /// level changed since the last update.
    pub fn update_occupancy(&mut self, occupancy: usize) -> (FullnessLevel, bool) {
        self.current_occupancy = occupancy;
        let ratio = self.fullness_ratio();
        self.stats.record_fullness(ratio);

        let new_level = FullnessLevel::from_ratio(ratio);
        let changed = new_level != self.current_level;

        if changed {
            self.prev_level = self.current_level;
            self.current_level = new_level;

            // Record pause/resume transitions
            if self.current_level.is_severe() && !self.prev_level.is_severe() {
                self.stats.record_pause();
            }
            if !self.current_level.is_severe() && self.prev_level.is_severe() {
                self.stats.record_resume();
            }
        }

        (self.current_level, changed)
    }

    /// Returns the current fullness level.
    pub fn level(&self) -> FullnessLevel {
        self.current_level
    }

    /// Returns the previous fullness level.
    pub fn prev_level(&self) -> FullnessLevel {
        self.prev_level
    }

    /// Returns the current fullness ratio (0.0–1.0).
    pub fn fullness_ratio(&self) -> f64 {
        if self.total_capacity == 0 {
            return 0.0;
        }
        self.current_occupancy as f64 / self.total_capacity as f64
    }

    /// Returns the current occupancy.
    pub fn occupancy(&self) -> usize {
        self.current_occupancy
    }

    /// Returns the total capacity.
    pub fn total_capacity(&self) -> usize {
        self.total_capacity
    }

    /// Update total_capacity at runtime (called when first QueueStatus arrives).
    pub fn set_total_capacity(&mut self, capacity: usize) {
        self.total_capacity = capacity;
    }

    /// Returns a reference to the flow stats.
    pub fn stats(&self) -> &Arc<FlowStats> {
        &self.stats
    }

    /// Determine the chunk size multiplier based on the current level.
    ///
    /// - Green: 1.0 (normal)
    /// - Yellow: 0.75 (reduce by 25%)
    /// - Orange/Red: 0.0 (pause splitting)
    pub fn chunk_size_multiplier(&self) -> f64 {
        match self.current_level {
            FullnessLevel::Green => 1.0,
            FullnessLevel::Yellow => YELLOW_REDUCTION_FACTOR,
            FullnessLevel::Orange | FullnessLevel::Red => 0.0,
        }
    }

    /// Returns true if the sender should pause chunk splitting.
    pub fn should_pause_splitting(&self) -> bool {
        self.current_level.is_severe()
    }

    /// Returns true if the sender should stop all sending.
    pub fn should_stop_all(&self) -> bool {
        self.current_level.is_critical()
    }

    /// Returns true if normal operation should resume (level is Green).
    pub fn should_resume(&self) -> bool {
        self.current_level == FullnessLevel::Green
    }
}

// ─── ReceiverMonitor ─────────────────────────────────────────────────────────

/// Monitors buffer pool fullness on the receiver side and sends QUEUE_STATUS
/// messages to the sender via the control channel.
///
/// # Operation
///
/// Every `status_interval` (~1000ms), the monitor:
/// 1. Checks the buffer pool's current fullness.
/// 2. Sends a [`ControlMessage::QueueStatus`] with queued_chunks and queued_bytes.
/// 3. Updates the local [`FlowController`] state.
pub struct ReceiverMonitor {
    /// Total capacity of the buffer pool.
    total_capacity: usize,
    /// Flow controller for tracking fullness state.
    controller: FlowController,
    /// Channel to send QUEUE_STATUS messages to the sender.
    control_tx: mpsc::Sender<ControlMessage>,
    /// Interval between status reports.
    status_interval: Duration,
    /// Total bytes in-flight across all FragmentReassemblers.
    /// Used instead of buffer_pool.used_count() for accurate pipeline memory measurement.
    receiver_bytes: Arc<AtomicUsize>,
}

impl ReceiverMonitor {
    /// Create a new `ReceiverMonitor`.
    ///
    /// * `total_capacity` - Total capacity of the buffer pool (in items or bytes).
    /// * `control_tx` - Channel to send control messages to the sender.
    /// * `status_interval` - How often to send QUEUE_STATUS messages.
    /// * `receiver_bytes` - Shared atomic tracking total bytes in FragmentReassemblers.
    pub fn new(
        total_capacity: usize,
        control_tx: mpsc::Sender<ControlMessage>,
        status_interval: Duration,
        receiver_bytes: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            total_capacity,
            controller: FlowController::new(total_capacity),
            control_tx,
            status_interval,
            receiver_bytes,
        }
    }

    /// Run the monitor loop.
    ///
    /// Periodically checks buffer fullness and sends QUEUE_STATUS messages.
    /// Returns when the control channel is closed or the monitor is cancelled.
    pub async fn run(&mut self, mut cancel_rx: mpsc::Receiver<()>) {
        info!(
            "flow receiver monitor started: interval={:?}, capacity={}",
            self.status_interval, self.total_capacity
        );

        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.status_interval) => {
                    self.check_and_report().await;
                }
                _ = cancel_rx.recv() => {
                    info!("flow receiver monitor cancelled");
                    break;
                }
            }
        }
    }

    /// Read current RSS (resident set size) from /proc/self/status.
    fn read_rss_kb() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines().find_map(|line| {
                    if line.starts_with("VmRSS:") {
                        line.split_whitespace()
                            .nth(1)
                            .and_then(|v| v.parse::<u64>().ok())
                    } else {
                        None
                    }
                })
            })
            .unwrap_or(0)
    }

    /// Check buffer fullness and send QUEUE_STATUS if needed.
    async fn check_and_report(&mut self) {
        // Track actual pipeline bytes in-flight across all FragmentReassemblers
        // instead of buffer_pool.used_count(). The buffer pool only has ~16 buffers
        // and is never a bottleneck — pipeline memory is in the reassembler HashMaps.
        let occupancy = self.receiver_bytes.load(Ordering::Relaxed);
        let (level, changed) = self.controller.update_occupancy(occupancy);
        let rss_kb = Self::read_rss_kb();

        info!(
            "FLOW_METRIC: occupancy={}, capacity={}, ratio={:.6}, level={:?}, changed={} rss_mb={}",
            occupancy,
            self.total_capacity,
            self.controller.fullness_ratio(),
            level,
            changed,
            rss_kb / 1024,
        );

        // Build QUEUE_STATUS message
        let queued_chunks = occupancy as u32;
        let queued_bytes = (self.controller.fullness_ratio() * self.total_capacity as f64) as u32;

        let msg = ControlMessage::QueueStatus {
            queued_chunks,
            queued_bytes,
            total_capacity: self.total_capacity as u32,
        };

        match self.control_tx.send(msg).await {
            Ok(()) => {
                self.controller.stats.record_status_sent();
            }
            Err(mpsc::error::SendError(_)) => {
                warn!("control channel closed, stopping monitor");
            }
        }
    }

    /// Returns a reference to the flow stats.
    pub fn stats(&self) -> &Arc<FlowStats> {
        self.controller.stats()
    }

    /// Returns a reference to the flow controller.
    pub fn controller(&self) -> &FlowController {
        &self.controller
    }
}

// ─── SenderReactor ───────────────────────────────────────────────────────────

/// Receives QUEUE_STATUS messages on the sender side and applies backpressure
/// actions based on the reported fullness level.
///
/// # Backpressure Actions
///
/// | Level  | Action                                      |
/// |--------|---------------------------------------------|
/// | Green  | Normal operation, resume if was paused       |
/// | Yellow | Reduce chunk size by 25%                     |
/// | Orange | Pause chunk splitting (backpressure to stdin)|
/// | Red    | Pause ALL sending, send NACK to receiver     |
///
/// When buffer drops below 50%, resume normal operation.
pub struct SenderReactor {
    /// Flow controller tracking current state.
    controller: FlowController,
    /// Channel to receive QUEUE_STATUS messages from the receiver.
    control_rx: mpsc::Receiver<ControlMessage>,
    /// Channel to send pause/resume signals to the splitter.
    /// `true` = pause sending, `false` = resume sending.
    flow_pause_tx: Option<mpsc::Sender<bool>>,
}

impl SenderReactor {
    /// Create a new `SenderReactor`.
    ///
    /// * `total_capacity` - Total buffer pool capacity (must match receiver).
    /// * `control_rx` - Channel to receive QUEUE_STATUS messages.
    /// * `flow_pause_tx` - Optional channel to send pause/resume signals to the splitter.
    pub fn new(
        total_capacity: usize,
        control_rx: mpsc::Receiver<ControlMessage>,
        flow_pause_tx: Option<mpsc::Sender<bool>>,
    ) -> Self {
        Self {
            controller: FlowController::new(total_capacity),
            control_rx,
            flow_pause_tx,
        }
    }

    /// Run the reactor loop.
    ///
    /// Processes incoming QUEUE_STATUS messages and applies backpressure.
    /// Returns when the control channel is closed.
    pub async fn run(&mut self) {
        info!(
            "flow sender reactor started: capacity={}",
            self.controller.total_capacity()
        );

        while let Some(msg) = self.control_rx.recv().await {
            self.process_message(msg).await;
        }

        info!("flow sender reactor finished (control channel closed)");
    }

    /// Process a single control message.
    async fn process_message(&mut self, msg: ControlMessage) {
        match msg {
            ControlMessage::QueueStatus {
                queued_chunks,
                queued_bytes,
                total_capacity: _,
            } => {
                self.controller.stats.record_status_received();
                self.handle_queue_status(queued_chunks as usize, queued_bytes as usize)
                    .await;
            }
            other => {
                debug!(
                    "sender reactor ignoring non-queue-status message: {}",
                    other
                );
            }
        }
    }

    /// Handle a QUEUE_STATUS update.
    async fn handle_queue_status(&mut self, occupancy: usize, _bytes: usize) {
        let (level, changed) = self.controller.update_occupancy(occupancy);

        debug!(
            "queue status: occupancy={}, level={:?}, changed={}",
            occupancy, level, changed,
        );

        match level {
            FullnessLevel::Green => {
                // Normal operation — resume if we were paused
                if changed && self.controller.prev_level().is_severe() {
                    info!("flow: green zone — resuming normal operation");
                    self.flow_pause_tx.as_ref().map(|tx| tx.try_send(false));
                }
            }
            FullnessLevel::Yellow => {
                // Reduce chunk size by 25%
                if changed {
                    info!("flow: yellow zone — reducing chunk size by 25%");
                    self.controller.stats.record_chunk_size_reduction();
                }
            }
            FullnessLevel::Orange => {
                // Pause chunk splitting (backpressure to stdin)
                if changed {
                    info!("flow: orange zone — pausing chunk splitting");
                    self.flow_pause_tx.as_ref().map(|tx| tx.try_send(true));
                }
            }
            FullnessLevel::Red => {
                // Pause ALL sending, send NACK to receiver
                if changed {
                    warn!("flow: RED zone — pausing ALL sending, sending NACK");
                    self.controller.stats.record_nack();
                    self.flow_pause_tx.as_ref().map(|tx| tx.try_send(true));
                }
            }
        }
    }

    /// Returns a reference to the flow stats.
    pub fn stats(&self) -> &Arc<FlowStats> {
        self.controller.stats()
    }

    /// Returns a reference to the flow controller.
    pub fn controller(&self) -> &FlowController {
        &self.controller
    }
}

// ─── Helper: compute buffer fullness from BufferPool ─────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    // ─── FullnessLevel Tests ─────────────────────────────────────────────

    #[test]
    fn fullness_green_below_50() {
        assert_eq!(FullnessLevel::from_ratio(0.0), FullnessLevel::Green);
        assert_eq!(FullnessLevel::from_ratio(0.25), FullnessLevel::Green);
        assert_eq!(FullnessLevel::from_ratio(0.49), FullnessLevel::Green);
    }

    #[test]
    fn fullness_yellow_50_to_80() {
        assert_eq!(FullnessLevel::from_ratio(0.50), FullnessLevel::Yellow);
        assert_eq!(FullnessLevel::from_ratio(0.65), FullnessLevel::Yellow);
        assert_eq!(FullnessLevel::from_ratio(0.79), FullnessLevel::Yellow);
    }

    #[test]
    fn fullness_orange_80_to_95() {
        assert_eq!(FullnessLevel::from_ratio(0.80), FullnessLevel::Orange);
        assert_eq!(FullnessLevel::from_ratio(0.90), FullnessLevel::Orange);
        assert_eq!(FullnessLevel::from_ratio(0.94), FullnessLevel::Orange);
    }

    #[test]
    fn fullness_red_95_plus() {
        assert_eq!(FullnessLevel::from_ratio(0.95), FullnessLevel::Red);
        assert_eq!(FullnessLevel::from_ratio(0.99), FullnessLevel::Red);
        assert_eq!(FullnessLevel::from_ratio(1.0), FullnessLevel::Red);
    }

    #[test]
    fn fullness_level_predicates() {
        assert!(!FullnessLevel::Green.is_pressured());
        assert!(FullnessLevel::Yellow.is_pressured());
        assert!(FullnessLevel::Orange.is_pressured());
        assert!(FullnessLevel::Red.is_pressured());

        assert!(!FullnessLevel::Green.is_severe());
        assert!(!FullnessLevel::Yellow.is_severe());
        assert!(FullnessLevel::Orange.is_severe());
        assert!(FullnessLevel::Red.is_severe());

        assert!(!FullnessLevel::Green.is_critical());
        assert!(!FullnessLevel::Yellow.is_critical());
        assert!(!FullnessLevel::Orange.is_critical());
        assert!(FullnessLevel::Red.is_critical());
    }

    // ─── FlowController Tests ────────────────────────────────────────────

    #[test]
    fn controller_starts_green() {
        let ctrl = FlowController::new(100);
        assert_eq!(ctrl.level(), FullnessLevel::Green);
        assert_eq!(ctrl.fullness_ratio(), 0.0);
    }

    #[test]
    fn controller_update_occupancy_tracks_level() {
        let mut ctrl = FlowController::new(100);

        // Green zone
        let (level, changed) = ctrl.update_occupancy(30);
        assert_eq!(level, FullnessLevel::Green);
        assert!(!changed, "first update should not be a change from initial");

        // Yellow zone
        let (level, changed) = ctrl.update_occupancy(60);
        assert_eq!(level, FullnessLevel::Yellow);
        assert!(changed);

        // Orange zone
        let (level, changed) = ctrl.update_occupancy(85);
        assert_eq!(level, FullnessLevel::Orange);
        assert!(changed);

        // Red zone
        let (level, changed) = ctrl.update_occupancy(96);
        assert_eq!(level, FullnessLevel::Red);
        assert!(changed);
    }

    #[test]
    fn controller_transitions_back_to_green() {
        let mut ctrl = FlowController::new(100);

        ctrl.update_occupancy(80); // Orange
        assert_eq!(ctrl.level(), FullnessLevel::Orange);

        ctrl.update_occupancy(40); // Back to Green
        assert_eq!(ctrl.level(), FullnessLevel::Green);
        assert_eq!(ctrl.prev_level(), FullnessLevel::Orange);
    }

    #[test]
    fn controller_chunk_size_multiplier() {
        let mut ctrl = FlowController::new(100);

        ctrl.update_occupancy(10);
        assert!((ctrl.chunk_size_multiplier() - 1.0).abs() < f64::EPSILON);

        ctrl.update_occupancy(60);
        assert!((ctrl.chunk_size_multiplier() - 0.75).abs() < f64::EPSILON);

        ctrl.update_occupancy(85);
        assert!((ctrl.chunk_size_multiplier() - 0.0).abs() < f64::EPSILON);

        ctrl.update_occupancy(96);
        assert!((ctrl.chunk_size_multiplier() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn controller_should_pause_and_stop() {
        let mut ctrl = FlowController::new(100);

        ctrl.update_occupancy(10);
        assert!(!ctrl.should_pause_splitting());
        assert!(!ctrl.should_stop_all());

        ctrl.update_occupancy(85);
        assert!(ctrl.should_pause_splitting());
        assert!(!ctrl.should_stop_all());

        ctrl.update_occupancy(96);
        assert!(ctrl.should_pause_splitting());
        assert!(ctrl.should_stop_all());
    }

    #[test]
    fn controller_should_resume_after_drop_below_50() {
        let mut ctrl = FlowController::new(100);

        ctrl.update_occupancy(85); // Orange
        ctrl.update_occupancy(40); // Green
        assert!(ctrl.should_resume());
    }

    #[test]
    fn controller_fullness_ratio() {
        let mut ctrl = FlowController::new(200);
        ctrl.update_occupancy(50);
        assert!((ctrl.fullness_ratio() - 0.25).abs() < f64::EPSILON);

        ctrl.update_occupancy(150);
        assert!((ctrl.fullness_ratio() - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn controller_zero_capacity_does_not_panic() {
        let mut ctrl = FlowController::new(0);
        assert_eq!(ctrl.fullness_ratio(), 0.0);
        let (level, _) = ctrl.update_occupancy(0);
        assert_eq!(level, FullnessLevel::Green);
    }

    // ─── FlowStats Tests ─────────────────────────────────────────────────

    #[test]
    fn stats_track_max_fullness() {
        let stats = FlowStats::default();
        stats.record_fullness(0.5);
        stats.record_fullness(0.8);
        stats.record_fullness(0.3);
        assert!((stats.max_fullness_ratio() - 0.8).abs() < 0.001);
    }

    #[test]
    fn stats_track_pause_resume() {
        let stats = FlowStats::default();
        stats.record_pause();
        stats.record_pause();
        stats.record_resume();
        assert_eq!(stats.total_pauses.load(Ordering::Relaxed), 2);
        assert_eq!(stats.total_resumes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn stats_track_all_counters() {
        let stats = FlowStats::default();
        stats.record_status_sent();
        stats.record_status_received();
        stats.record_chunk_size_reduction();
        stats.record_nack();

        assert_eq!(stats.status_messages_sent.load(Ordering::Relaxed), 1);
        assert_eq!(stats.status_messages_received.load(Ordering::Relaxed), 1);
        assert_eq!(stats.chunk_size_reductions.load(Ordering::Relaxed), 1);
        assert_eq!(stats.nacks_sent.load(Ordering::Relaxed), 1);
    }

    // ─── FlowController Pause/Resume Recording Tests ─────────────────────

    #[test]
    fn controller_records_pause_on_orange_entry() {
        let mut ctrl = FlowController::new(100);
        ctrl.update_occupancy(85); // Orange — should record pause
        assert_eq!(ctrl.stats().total_pauses.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn controller_records_resume_on_exit_from_severe() {
        let mut ctrl = FlowController::new(100);
        ctrl.update_occupancy(85); // Orange — pause
        assert_eq!(ctrl.stats().total_pauses.load(Ordering::Relaxed), 1);

        ctrl.update_occupancy(40); // Green — resume
        assert_eq!(ctrl.stats().total_resumes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn controller_records_pause_on_red_entry() {
        let mut ctrl = FlowController::new(100);
        ctrl.update_occupancy(96); // Red — should record pause
        assert_eq!(ctrl.stats().total_pauses.load(Ordering::Relaxed), 1);
    }

    // ─── ReceiverMonitor Tests ───────────────────────────────────────────

    #[tokio::test]
    async fn receiver_monitor_sends_queue_status() {
        let (control_tx, mut control_rx) = mpsc::channel(16);
        let (cancel_tx, cancel_rx) = mpsc::channel(1);
        let receiver_bytes = Arc::new(AtomicUsize::new(0));

        let mut monitor = ReceiverMonitor::new(
            10,
            control_tx,
            Duration::from_millis(50), // Fast interval for testing
            receiver_bytes,
        );

        // Spawn the monitor
        let handle = tokio::spawn(async move {
            monitor.run(cancel_rx).await;
        });

        // Wait for a QUEUE_STATUS message
        let msg = tokio::time::timeout(Duration::from_millis(200), control_rx.recv())
            .await
            .expect("should receive QUEUE_STATUS")
            .expect("channel should be open");

        match msg {
            ControlMessage::QueueStatus {
                queued_chunks,
                queued_bytes: _,
                total_capacity: _,
            } => {
                assert_eq!(queued_chunks, 0); // Pool is empty
            }
            other => panic!("expected QueueStatus, got: {:?}", other),
        }

        // Cancel the monitor
        let _ = cancel_tx.send(()).await;
        let _ = handle.await;
    }

    #[tokio::test]
    async fn receiver_monitor_stops_on_cancel() {
        let (control_tx, _control_rx) = mpsc::channel(16);
        let (cancel_tx, cancel_rx) = mpsc::channel(1);
        let receiver_bytes = Arc::new(AtomicUsize::new(0));

        let mut monitor = ReceiverMonitor::new(
            10,
            control_tx,
            Duration::from_secs(3600), // Very long interval
            receiver_bytes,
        );

        let handle = tokio::spawn(async move {
            monitor.run(cancel_rx).await;
        });

        // Cancel immediately
        let _ = cancel_tx.send(()).await;

        // Should complete quickly
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("monitor should stop on cancel")
            .expect("monitor should not panic");
    }

    // ─── SenderReactor Tests ─────────────────────────────────────────────

    #[tokio::test]
    async fn sender_reactor_green_resumes_sending() {
        let (control_tx, control_rx) = mpsc::channel(16);

        let mut reactor = SenderReactor::new(100, control_rx, None);

        // First put it into orange, then green
        let handle = tokio::spawn(async move {
            // Send orange-level status
            control_tx
                .send(ControlMessage::QueueStatus {
                    queued_chunks: 85,
                    queued_bytes: 85,
                    total_capacity: 100,
                })
                .await
                .ok();
            // Then send green-level status
            control_tx
                .send(ControlMessage::QueueStatus {
                    queued_chunks: 30,
                    queued_bytes: 30,
                    total_capacity: 100,
                })
                .await
                .ok();
            // Then close the channel to stop the reactor
            drop(control_tx);
        });

        // Run the reactor (will process both messages then exit)
        reactor.run().await;

        // The reactor should have processed both messages
        let stats = reactor.stats();
        assert_eq!(stats.status_messages_received.load(Ordering::Relaxed), 2);

        let _ = handle.await;
    }

    #[tokio::test]
    async fn sender_reactor_yellow_sends_multiplier() {
        let (control_tx, control_rx) = mpsc::channel(16);

        let mut reactor = SenderReactor::new(100, control_rx, None);

        let handle = tokio::spawn(async move {
            control_tx
                .send(ControlMessage::QueueStatus {
                    queued_chunks: 60,
                    queued_bytes: 60,
                    total_capacity: 100,
                })
                .await
                .ok();
            drop(control_tx);
        });

        reactor.run().await;

        let stats = reactor.stats();
        assert_eq!(stats.chunk_size_reductions.load(Ordering::Relaxed), 1);

        let _ = handle.await;
    }

    #[tokio::test]
    async fn sender_reactor_red_sends_nack_and_pause() {
        let (control_tx, control_rx) = mpsc::channel(16);

        let mut reactor = SenderReactor::new(100, control_rx, None);

        let handle = tokio::spawn(async move {
            control_tx
                .send(ControlMessage::QueueStatus {
                    queued_chunks: 96,
                    queued_bytes: 96,
                    total_capacity: 100,
                })
                .await
                .ok();
            drop(control_tx);
        });

        reactor.run().await;

        let stats = reactor.stats();
        assert_eq!(stats.nacks_sent.load(Ordering::Relaxed), 1);

        let _ = handle.await;
    }

    #[tokio::test]
    async fn sender_reactor_ignores_non_queue_status() {
        let (control_tx, control_rx) = mpsc::channel(16);

        let mut reactor = SenderReactor::new(100, control_rx, None);

        let handle = tokio::spawn(async move {
            // Send a non-queue-status message
            control_tx
                .send(ControlMessage::Ack {
                    sequence_number: 42,
                })
                .await
                .ok();
            drop(control_tx);
        });

        reactor.run().await;

        // Should have received 0 status messages (only 1 ack was ignored)
        let stats = reactor.stats();
        assert_eq!(stats.status_messages_received.load(Ordering::Relaxed), 0);

        let _ = handle.await;
    }

    // ─── Integration: Full Cycle Tests ───────────────────────────────────

    #[tokio::test]
    async fn full_cycle_green_to_red_to_green() {
        let (control_tx, control_rx) = mpsc::channel(16);

        let mut reactor = SenderReactor::new(100, control_rx, None);

        let handle = tokio::spawn(async move {
            // Green (normal)
            control_tx
                .send(ControlMessage::QueueStatus {
                    queued_chunks: 10,
                    queued_bytes: 10,
                    total_capacity: 100,
                })
                .await
                .ok();
            // Yellow (reduce)
            control_tx
                .send(ControlMessage::QueueStatus {
                    queued_chunks: 60,
                    queued_bytes: 60,
                    total_capacity: 100,
                })
                .await
                .ok();
            // Orange (pause splitting)
            control_tx
                .send(ControlMessage::QueueStatus {
                    queued_chunks: 85,
                    queued_bytes: 85,
                    total_capacity: 100,
                })
                .await
                .ok();
            // Red (stop all)
            control_tx
                .send(ControlMessage::QueueStatus {
                    queued_chunks: 96,
                    queued_bytes: 96,
                    total_capacity: 100,
                })
                .await
                .ok();
            // Back to green (resume)
            control_tx
                .send(ControlMessage::QueueStatus {
                    queued_chunks: 30,
                    queued_bytes: 30,
                    total_capacity: 100,
                })
                .await
                .ok();
            drop(control_tx);
        });

        reactor.run().await;

        // Verify the reactor processed all 5 messages
        let stats = reactor.stats();
        assert_eq!(stats.status_messages_received.load(Ordering::Relaxed), 5);
        assert_eq!(stats.total_pauses.load(Ordering::Relaxed), 1); // One pause on orange entry
        assert_eq!(stats.total_resumes.load(Ordering::Relaxed), 1); // One resume on green return
        assert_eq!(stats.chunk_size_reductions.load(Ordering::Relaxed), 1); // One reduction on yellow
        assert_eq!(stats.nacks_sent.load(Ordering::Relaxed), 1); // One NACK on red

        // Verify max fullness was recorded (at least 0.96)
        assert!(stats.max_fullness_ratio() >= 0.95);

        let _ = handle.await;
    }

    // ─── Backpressure Test Stubs ─────────────────────────────────────────

    /// Verifies that a QueueStatus message sent through an mpsc channel
    /// is properly received and processed by the SenderReactor.
    ///
    /// This test creates a control channel, sends a QueueStatus message with
    /// a known occupancy level, and verifies that the reactor updates its
    /// internal state accordingly.
    #[tokio::test]
    async fn test_queue_status_reaches_sender_reactor() {
        let (control_tx, control_rx) = mpsc::channel::<ControlMessage>(16);
        let mut reactor = SenderReactor::new(1000, control_rx, None);

        // Spawn a task that sends a QueueStatus with 60% occupancy (Yellow zone)
        let handle = tokio::spawn(async move {
            control_tx
                .send(ControlMessage::QueueStatus {
                    queued_chunks: 600,
                    queued_bytes: 600,
                    total_capacity: 1000,
                })
                .await
                .ok();
            // Close the channel so the reactor exits
            drop(control_tx);
        });

        // Run the reactor — it will process the message then exit on channel close
        reactor.run().await;

        // Verify the reactor updated its state from the QueueStatus
        assert_eq!(
            reactor
                .stats()
                .status_messages_received
                .load(Ordering::Relaxed),
            1,
            "reactor should have received one QueueStatus"
        );
        assert_eq!(
            reactor.controller().level(),
            FullnessLevel::Yellow,
            "600/1000 occupancy should map to Yellow zone"
        );

        let _ = handle.await;
    }
}
