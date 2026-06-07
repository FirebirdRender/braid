/// Adaptive channel count algorithm.
///
/// # Overview
///
/// The `ChannelCountAdaptor` manages the number of parallel UDP data channels
/// using a ramp-up algorithm with throughput validation:
///
/// - **Ramp-up**: Starts at `DEFAULT_INITIAL_CHANNELS` (4), adds one channel
///   every 10–20 seconds (configurable interval).
/// - **Throughput check**: Before/after adding a channel, throughput is measured.
///   - Increase >5%: continue adding channels.
///   - Increase <5% (plateau): stop adding channels.
///   - Throughput DECREASES: remove the last channel added.
/// - **Timeout**: If no throughput data arrives within 30 seconds, adaptation
///   is paused (no further channel additions).
/// - **User override**: `--no-auto` disables adaptation, uses fixed count.
///
/// # Clamping
///
/// The channel count is always clamped to [`MIN_CHANNELS`, `MAX_CHANNELS`]:
/// - `MIN_CHANNELS = 1`
/// - `MAX_CHANNELS = 256`
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Minimum number of channels.
pub const MIN_CHANNELS: usize = 1;

/// Maximum number of channels.
pub const MAX_CHANNELS: usize = 256;

/// Default initial channel count.
pub const DEFAULT_INITIAL_CHANNELS: usize = 4;

/// Default minimum ramp-up interval (10 seconds).
pub const DEFAULT_MIN_INTERVAL: Duration = Duration::from_secs(10);

/// Default maximum ramp-up interval (20 seconds).
pub const DEFAULT_MAX_INTERVAL: Duration = Duration::from_secs(20);

/// Throughput improvement threshold (5%).
const THROUGHPUT_THRESHOLD: f64 = 0.05;

/// EWMA smoothing factor for throughput (alpha = 0.3).
const EWMA_ALPHA: f64 = 0.3;

/// Timeout duration — if no throughput data arrives within this window,
/// adaptation is paused.
const THROUGHPUT_TIMEOUT: Duration = Duration::from_secs(30);

/// Number of throughput samples to collect before/after a channel change
/// to compute a stable throughput estimate.
const THROUGHPUT_WINDOW: usize = 10;

/// A single throughput sample: bytes transferred over a measured duration.
#[derive(Debug, Clone, Copy)]
pub struct ThroughputSample {
    /// Number of bytes transferred in this sample.
    pub bytes: u64,
    /// Duration of the sample in nanoseconds.
    pub duration_ns: u64,
}

/// The outcome of a channel count evaluation tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickAction {
    /// No action needed — waiting for the next interval.
    None,
    /// Add one channel (throughput improved or first ramp-up step).
    AddChannel,
    /// Remove one channel (throughput degraded after last addition).
    RemoveChannel,
    /// Plateau detected — stop adding channels.
    Plateau,
}

/// Adaptive channel count controller.
///
/// # Usage
///
/// ```ignore
/// let mut adaptor = ChannelCountAdaptor::new(4, 10, 20);
///
/// // Call tick() periodically (e.g., every second):
/// let action = adaptor.tick();
///
/// // Record throughput samples as data flows:
/// adaptor.record_throughput(ThroughputSample { bytes: 65536, duration_ns: 1_000_000 });
///
/// // When adding a channel, call before/after:
/// adaptor.notify_channel_added();
/// adaptor.notify_channel_removed();
/// ```
#[derive(Debug)]
pub struct ChannelCountAdaptor {
    /// Current number of channels.
    current_channels: usize,
    /// Minimum allowed channels.
    min_channels: usize,
    /// Maximum allowed channels.
    max_channels: usize,
    /// Minimum ramp-up interval.
    min_interval: Duration,
    /// Maximum ramp-up interval (actual interval is randomized in [min, max]).
    max_interval: Duration,
    /// Timestamp of the last channel count change.
    last_change: Option<Instant>,
    /// Throughput samples collected since the last channel change.
    /// Used to measure baseline throughput before adding a channel.
    pre_change_samples: Vec<ThroughputSample>,
    /// Throughput samples collected after the last channel change.
    /// Used to measure post-change throughput.
    post_change_samples: Vec<ThroughputSample>,
    /// EWMA throughput before the last channel change (bytes per nanosecond).
    pre_change_ewma: f64,
    /// EWMA throughput after the last channel change (bytes per nanosecond).
    post_change_ewma: f64,
    /// Whether we have collected enough post-change samples for comparison.
    post_change_ready: bool,
    /// Whether we are in a plateau state (stop adding).
    plateau: bool,
    /// Timestamp of the last throughput sample received.
    last_throughput_time: Option<Instant>,
    /// Whether the adaptor is disabled (user override via --no-auto).
    disabled: AtomicBool,
    /// Fixed channel count to use when disabled.
    fixed_channel_count: usize,
}

impl ChannelCountAdaptor {
    /// Create a new `ChannelCountAdaptor`.
    ///
    /// * `initial_channels` - Starting channel count (clamped to [min, max]).
    /// * `min_channels` - Minimum allowed channels.
    /// * `max_channels` - Maximum allowed channels.
    /// * `min_interval_secs` - Minimum ramp-up interval in seconds.
    /// * `max_interval_secs` - Maximum ramp-up interval in seconds.
    pub fn new(
        initial_channels: usize,
        min_channels: usize,
        max_channels: usize,
        min_interval_secs: u64,
        max_interval_secs: u64,
    ) -> Self {
        assert!(min_channels > 0, "min_channels must be positive");
        assert!(
            max_channels >= min_channels,
            "max_channels must be >= min_channels"
        );
        assert!(
            max_interval_secs >= min_interval_secs,
            "max_interval_secs must be >= min_interval_secs"
        );

        let clamped = initial_channels.clamp(min_channels, max_channels);

        Self {
            current_channels: clamped,
            min_channels,
            max_channels,
            min_interval: Duration::from_secs(min_interval_secs),
            max_interval: Duration::from_secs(max_interval_secs),
            last_change: None,
            pre_change_samples: Vec::with_capacity(THROUGHPUT_WINDOW),
            post_change_samples: Vec::with_capacity(THROUGHPUT_WINDOW),
            pre_change_ewma: 0.0,
            post_change_ewma: 0.0,
            post_change_ready: false,
            plateau: false,
            last_throughput_time: None,
            disabled: AtomicBool::new(false),
            fixed_channel_count: clamped,
        }
    }

    /// Create a new `ChannelCountAdaptor` with default parameters.
    ///
    /// Defaults: initial = `DEFAULT_INITIAL_CHANNELS` (4), min = `MIN_CHANNELS`,
    /// max = `MAX_CHANNELS`, interval = 10–20 seconds.
    pub fn new_default() -> Self {
        Self::new(
            DEFAULT_INITIAL_CHANNELS,
            MIN_CHANNELS,
            MAX_CHANNELS,
            DEFAULT_MIN_INTERVAL.as_secs(),
            DEFAULT_MAX_INTERVAL.as_secs(),
        )
    }

    /// Disable adaptive behavior and use a fixed channel count.
    ///
    /// This is called when the user provides `--no-auto`.
    pub fn set_fixed(&mut self, fixed_count: usize) {
        self.fixed_channel_count = fixed_count.clamp(self.min_channels, self.max_channels);
        self.disabled.store(true, Ordering::Relaxed);
    }

    /// Returns `true` if the adaptor is disabled (fixed channel count mode).
    pub fn is_disabled(&self) -> bool {
        self.disabled.load(Ordering::Relaxed)
    }

    /// Returns the current channel count.
    ///
    /// If the adaptor is disabled, returns the fixed channel count.
    pub fn current_channel_count(&self) -> usize {
        if self.is_disabled() {
            return self.fixed_channel_count;
        }
        self.current_channels
    }

    /// Returns the minimum channel count.
    pub fn min_channels(&self) -> usize {
        self.min_channels
    }

    /// Returns the maximum channel count.
    pub fn max_channels(&self) -> usize {
        self.max_channels
    }

    /// Returns `true` if the adaptor has detected a plateau (stop adding).
    pub fn is_plateau(&self) -> bool {
        self.plateau
    }

    /// Returns the number of pre-change samples collected.
    pub fn pre_change_sample_count(&self) -> usize {
        self.pre_change_samples.len()
    }

    /// Returns the number of post-change samples collected.
    pub fn post_change_sample_count(&self) -> usize {
        self.post_change_samples.len()
    }

    /// Record a throughput sample.
    ///
    /// Samples are accumulated into either the pre-change or post-change
    /// window depending on the adaptor's state. When enough post-change
    /// samples have been collected, the adaptor evaluates whether to
    /// continue adding channels, stop (plateau), or remove a channel.
    pub fn record_throughput(&mut self, sample: ThroughputSample) {
        if self.is_disabled() {
            return;
        }

        self.last_throughput_time = Some(Instant::now());

        if self.post_change_ready {
            // We already evaluated the last channel change; accumulate
            // samples for the next evaluation cycle.
            self.pre_change_samples.push(sample);
            if self.pre_change_samples.len() > THROUGHPUT_WINDOW {
                self.pre_change_samples.remove(0);
            }
        } else if self.last_change.is_some() {
            // We added/removed a channel and are collecting post-change samples.
            self.post_change_samples.push(sample);
            if self.post_change_samples.len() > THROUGHPUT_WINDOW {
                self.post_change_samples.remove(0);
            }
        } else {
            // No channel change yet; accumulate baseline samples.
            self.pre_change_samples.push(sample);
            if self.pre_change_samples.len() > THROUGHPUT_WINDOW {
                self.pre_change_samples.remove(0);
            }
        }
    }

    /// Compute the average throughput from a set of samples.
    ///
    /// Returns bytes per nanosecond, or 0.0 if no valid samples.
    fn compute_throughput(samples: &[ThroughputSample]) -> f64 {
        if samples.is_empty() {
            return 0.0;
        }
        let total_bytes: u64 = samples.iter().map(|s| s.bytes).sum();
        let total_duration_ns: u64 = samples.iter().map(|s| s.duration_ns).sum();
        if total_duration_ns == 0 {
            return 0.0;
        }
        total_bytes as f64 / total_duration_ns as f64
    }

    /// Apply EWMA smoothing to a throughput value.
    fn smooth_ewma(prev: f64, current: f64) -> f64 {
        if prev == 0.0 {
            current
        } else {
            prev * (1.0 - EWMA_ALPHA) + current * EWMA_ALPHA
        }
    }

    /// Evaluate the effect of the last channel change.
    ///
    /// Compares pre-change EWMA throughput with post-change EWMA throughput
    /// and returns the appropriate `TickAction`.
    fn evaluate_change(&mut self) -> TickAction {
        if !self.post_change_ready {
            return TickAction::None;
        }

        let pre = self.pre_change_ewma;
        let post = self.post_change_ewma;

        if pre == 0.0 {
            // No baseline — can't evaluate. Continue adding.
            self.post_change_ready = false;
            self.plateau = false;
            return TickAction::AddChannel;
        }

        let change = (post - pre) / pre;

        if change > THROUGHPUT_THRESHOLD {
            // Throughput improved by >5%: continue adding.
            self.post_change_ready = false;
            self.plateau = false;
            TickAction::AddChannel
        } else if change < -THROUGHPUT_THRESHOLD {
            // Throughput decreased by >5%: remove the last channel.
            self.post_change_ready = false;
            self.plateau = false;
            TickAction::RemoveChannel
        } else {
            // Throughput change within [-5%, +5%]: plateau.
            self.plateau = true;
            self.post_change_ready = false;
            TickAction::Plateau
        }
    }

    /// Called when a channel has been added externally.
    ///
    /// This resets the post-change sample window and starts collecting
    /// throughput data to evaluate the effect of the new channel.
    pub fn notify_channel_added(&mut self) {
        if self.is_disabled() {
            return;
        }
        self.current_channels =
            (self.current_channels + 1).clamp(self.min_channels, self.max_channels);
        self.last_change = Some(Instant::now());
        self.post_change_samples.clear();
        self.post_change_ewma = 0.0;
        self.post_change_ready = false;
        self.plateau = false;

        // Compute EWMA from pre-change samples as baseline.
        let raw = Self::compute_throughput(&self.pre_change_samples);
        self.pre_change_ewma = Self::smooth_ewma(self.pre_change_ewma, raw);
    }

    /// Called when a channel has been removed externally.
    ///
    /// This resets the post-change sample window and starts collecting
    /// throughput data to evaluate whether the removal helped.
    pub fn notify_channel_removed(&mut self) {
        if self.is_disabled() {
            return;
        }
        if self.current_channels > self.min_channels {
            self.current_channels -= 1;
        }
        self.last_change = Some(Instant::now());
        self.post_change_samples.clear();
        self.post_change_ewma = 0.0;
        self.post_change_ready = false;
        self.plateau = false;

        // Compute EWMA from pre-change samples as baseline.
        let raw = Self::compute_throughput(&self.pre_change_samples);
        self.pre_change_ewma = Self::smooth_ewma(self.pre_change_ewma, raw);
    }

    /// Advance the adaptor state by one tick.
    ///
    /// This should be called periodically (e.g., every second) to drive
    /// the ramp-up logic. Returns a `TickAction` indicating what the
    /// caller should do.
    ///
    /// The ramp-up logic:
    /// 1. If disabled, return `None`.
    /// 2. If plateau detected, return `None`.
    /// 3. If at max channels, return `None`.
    /// 4. If no throughput data for 30s, return `None` (timeout).
    /// 5. If enough post-change samples collected, evaluate the change.
    /// 6. If enough time has passed since last change, signal `AddChannel`.
    pub fn tick(&mut self) -> TickAction {
        if self.is_disabled() {
            return TickAction::None;
        }

        // Check for throughput timeout.
        if let Some(last_time) = self.last_throughput_time {
            if last_time.elapsed() > THROUGHPUT_TIMEOUT {
                // No throughput data for 30s — pause adaptation.
                return TickAction::None;
            }
        }

        // If plateau detected, stop adding.
        if self.plateau {
            return TickAction::None;
        }

        // If at max channels, can't add more.
        if self.current_channels >= self.max_channels {
            return TickAction::None;
        }

        // If we have enough post-change samples, evaluate the last change.
        if self.post_change_samples.len() >= THROUGHPUT_WINDOW {
            let raw = Self::compute_throughput(&self.post_change_samples);
            self.post_change_ewma = Self::smooth_ewma(self.post_change_ewma, raw);
            self.post_change_ready = true;
            return self.evaluate_change();
        }

        // Check if enough time has passed since the last change.
        let elapsed = self
            .last_change
            .map(|t| t.elapsed())
            .unwrap_or(Duration::ZERO);

        // We need at least min_interval to have passed.
        if elapsed < self.min_interval {
            return TickAction::None;
        }

        // Randomize the actual interval between min and max.
        // Use a simple hash of the current channels for deterministic jitter.
        let interval_range = self.max_interval.as_secs_f64() - self.min_interval.as_secs_f64();
        let jitter = if interval_range > 0.0 {
            // Deterministic jitter based on current channel count.
            let hash = (self.current_channels as u64).wrapping_mul(0x9e3779b97f4a7c15);
            (hash % 1_000_000) as f64 / 1_000_000.0 * interval_range
        } else {
            0.0
        };
        let target_interval = Duration::from_secs_f64(self.min_interval.as_secs_f64() + jitter);

        if elapsed >= target_interval {
            // Time to try adding a channel.
            // First, compute the baseline throughput from pre-change samples.
            if !self.pre_change_samples.is_empty() {
                let raw = Self::compute_throughput(&self.pre_change_samples);
                self.pre_change_ewma = Self::smooth_ewma(self.pre_change_ewma, raw);
            }

            // Clear pre-change samples to start fresh for the new cycle.
            self.pre_change_samples.clear();

            TickAction::AddChannel
        } else {
            TickAction::None
        }
    }

    /// Reset the adaptor to its initial state.
    ///
    /// Clears all samples, resets channel count to default, clears plateau.
    pub fn reset(&mut self) {
        self.current_channels =
            DEFAULT_INITIAL_CHANNELS.clamp(self.min_channels, self.max_channels);
        self.last_change = None;
        self.pre_change_samples.clear();
        self.post_change_samples.clear();
        self.pre_change_ewma = 0.0;
        self.post_change_ewma = 0.0;
        self.post_change_ready = false;
        self.plateau = false;
        self.last_throughput_time = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── Helper: fill pre-change window with uniform throughput samples ──────

    fn fill_pre_change(adaptor: &mut ChannelCountAdaptor, bytes: u64, duration_ns: u64) {
        for _ in 0..THROUGHPUT_WINDOW {
            adaptor.record_throughput(ThroughputSample { bytes, duration_ns });
        }
    }

    // ─── Construction ───────────────────────────────────────────────────────

    #[test]
    fn starts_at_default_channels() {
        let a = ChannelCountAdaptor::new_default();
        assert_eq!(a.current_channel_count(), DEFAULT_INITIAL_CHANNELS);
        assert_eq!(a.min_channels(), MIN_CHANNELS);
        assert_eq!(a.max_channels(), MAX_CHANNELS);
        assert!(!a.is_plateau());
        assert!(!a.is_disabled());
    }

    #[test]
    fn clamps_initial_channel_count() {
        let a = ChannelCountAdaptor::new(0, MIN_CHANNELS, MAX_CHANNELS, 10, 20);
        assert_eq!(a.current_channel_count(), MIN_CHANNELS);

        let a = ChannelCountAdaptor::new(999, MIN_CHANNELS, MAX_CHANNELS, 10, 20);
        assert_eq!(a.current_channel_count(), MAX_CHANNELS);
    }

    #[test]
    fn custom_bounds() {
        let a = ChannelCountAdaptor::new(8, 2, 16, 5, 10);
        assert_eq!(a.min_channels(), 2);
        assert_eq!(a.max_channels(), 16);
        assert_eq!(a.current_channel_count(), 8);
    }

    #[test]
    #[should_panic(expected = "min_channels must be positive")]
    fn rejects_zero_min_channels() {
        ChannelCountAdaptor::new(4, 0, 256, 10, 20);
    }

    #[test]
    #[should_panic(expected = "max_channels must be >= min_channels")]
    fn rejects_max_less_than_min() {
        ChannelCountAdaptor::new(4, 10, 5, 10, 20);
    }

    #[test]
    #[should_panic(expected = "max_interval_secs must be >= min_interval_secs")]
    fn rejects_max_interval_less_than_min() {
        ChannelCountAdaptor::new(4, 1, 256, 20, 10);
    }

    // ─── Tick returns AddChannel after interval ─────────────────────────────

    #[test]
    fn tick_returns_add_channel_after_interval() {
        let mut a = ChannelCountAdaptor::new(4, 1, 256, 0, 0); // zero interval = immediate

        // Fill pre-change samples so there's a baseline.
        fill_pre_change(&mut a, 65536, 1_000_000);

        // Tick should return AddChannel since interval has passed.
        let action = a.tick();
        assert_eq!(
            action,
            TickAction::AddChannel,
            "expected AddChannel after interval"
        );
    }

    #[test]
    fn tick_returns_none_before_interval() {
        let mut a = ChannelCountAdaptor::new(4, 1, 256, 60, 60); // 60s interval

        fill_pre_change(&mut a, 65536, 1_000_000);

        // Tick immediately should return None (interval hasn't passed).
        let action = a.tick();
        assert_eq!(action, TickAction::None, "expected None before interval");
    }

    // ─── Throughput improvement → continue adding ───────────────────────────

    #[test]
    fn improved_throughput_returns_add_channel() {
        let mut a = ChannelCountAdaptor::new(4, 1, 256, 0, 0);

        // Fill pre-change with low throughput.
        fill_pre_change(&mut a, 65536, 1_000_000);

        // Simulate adding a channel and filling post-change with higher throughput.
        a.notify_channel_added();
        for _ in 0..THROUGHPUT_WINDOW {
            a.record_throughput(ThroughputSample {
                bytes: 65536,
                duration_ns: 100_000, // 10x faster
            });
        }

        let action = a.tick();
        assert_eq!(
            action,
            TickAction::AddChannel,
            "improved throughput should continue adding channels"
        );
    }

    // ─── Throughput degradation → remove channel ────────────────────────────

    #[test]
    fn degraded_throughput_returns_remove_channel() {
        let mut a = ChannelCountAdaptor::new(4, 1, 256, 0, 0);

        // Fill pre-change with high throughput.
        fill_pre_change(&mut a, 65536, 100_000);

        // Simulate adding a channel and filling post-change with lower throughput.
        a.notify_channel_added();
        for _ in 0..THROUGHPUT_WINDOW {
            a.record_throughput(ThroughputSample {
                bytes: 65536,
                duration_ns: 1_000_000, // 10x slower
            });
        }

        let action = a.tick();
        assert_eq!(
            action,
            TickAction::RemoveChannel,
            "degraded throughput should remove channel"
        );
    }

    // ─── Plateau detection ──────────────────────────────────────────────────

    #[test]
    fn plateau_detected_when_throughput_stable() {
        let mut a = ChannelCountAdaptor::new(4, 1, 256, 0, 0);

        // Fill pre-change with some throughput.
        fill_pre_change(&mut a, 65536, 1_000_000);

        // Simulate adding a channel and filling post-change with same throughput.
        a.notify_channel_added();
        for _ in 0..THROUGHPUT_WINDOW {
            a.record_throughput(ThroughputSample {
                bytes: 65536,
                duration_ns: 1_000_000, // same
            });
        }

        let action = a.tick();
        assert_eq!(
            action,
            TickAction::Plateau,
            "stable throughput should plateau"
        );

        // Once plateaued, subsequent ticks should return None.
        let action2 = a.tick();
        assert_eq!(action2, TickAction::None, "plateau should persist");
    }

    // ─── Disabled mode ──────────────────────────────────────────────────────

    #[test]
    fn disabled_mode_returns_fixed_count() {
        let mut a = ChannelCountAdaptor::new_default();
        a.set_fixed(8);

        assert!(a.is_disabled());
        assert_eq!(a.current_channel_count(), 8);

        // tick should be a no-op when disabled.
        let action = a.tick();
        assert_eq!(action, TickAction::None);

        // record_throughput should be a no-op.
        a.record_throughput(ThroughputSample {
            bytes: 999999,
            duration_ns: 1,
        });
        assert_eq!(a.current_channel_count(), 8);
    }

    #[test]
    fn disabled_mode_clamps_fixed_count() {
        let mut a = ChannelCountAdaptor::new_default();
        a.set_fixed(0);
        assert_eq!(a.current_channel_count(), MIN_CHANNELS);

        a.set_fixed(999);
        assert_eq!(a.current_channel_count(), MAX_CHANNELS);
    }

    // ─── Max channels cap ───────────────────────────────────────────────────

    #[test]
    fn tick_returns_none_at_max_channels() {
        let mut a = ChannelCountAdaptor::new(MAX_CHANNELS, MIN_CHANNELS, MAX_CHANNELS, 0, 0);

        fill_pre_change(&mut a, 65536, 1_000_000);

        let action = a.tick();
        assert_eq!(
            action,
            TickAction::None,
            "should not add beyond max channels"
        );
    }

    // ─── Throughput timeout ─────────────────────────────────────────────────

    #[test]
    fn tick_returns_none_after_throughput_timeout() {
        let mut a = ChannelCountAdaptor::new(4, 1, 256, 0, 0);

        // Record some throughput initially.
        fill_pre_change(&mut a, 65536, 1_000_000);

        // Manually set last_throughput_time far in the past to simulate timeout.
        a.last_throughput_time = Some(Instant::now() - THROUGHPUT_TIMEOUT - Duration::from_secs(1));

        let action = a.tick();
        assert_eq!(
            action,
            TickAction::None,
            "should pause after throughput timeout"
        );
    }

    // ─── notify_channel_added / notify_channel_removed ──────────────────────

    #[test]
    fn notify_channel_added_increments_count() {
        let mut a = ChannelCountAdaptor::new(4, 1, 256, 0, 0);
        assert_eq!(a.current_channel_count(), 4);

        a.notify_channel_added();
        assert_eq!(a.current_channel_count(), 5);
    }

    #[test]
    fn notify_channel_added_capped_at_max() {
        let mut a = ChannelCountAdaptor::new(MAX_CHANNELS, MIN_CHANNELS, MAX_CHANNELS, 0, 0);
        a.notify_channel_added();
        assert_eq!(a.current_channel_count(), MAX_CHANNELS);
    }

    #[test]
    fn notify_channel_removed_decrements_count() {
        let mut a = ChannelCountAdaptor::new(4, 1, 256, 0, 0);
        a.notify_channel_removed();
        assert_eq!(a.current_channel_count(), 3);
    }

    #[test]
    fn notify_channel_removed_capped_at_min() {
        let mut a = ChannelCountAdaptor::new(MIN_CHANNELS, MIN_CHANNELS, MAX_CHANNELS, 0, 0);
        a.notify_channel_removed();
        assert_eq!(a.current_channel_count(), MIN_CHANNELS);
    }

    // ─── Reset ──────────────────────────────────────────────────────────────

    #[test]
    fn reset_restores_initial_state() {
        let mut a = ChannelCountAdaptor::new(8, 1, 256, 0, 0);

        fill_pre_change(&mut a, 65536, 100_000);
        a.notify_channel_added(); // 8→9
                                  // Fill post-change window directly (don't use fill_post_change helper
                                  // which calls notify_channel_added again).
        for _ in 0..THROUGHPUT_WINDOW {
            a.record_throughput(ThroughputSample {
                bytes: 65536,
                duration_ns: 1_000_000,
            });
        }
        assert_eq!(a.current_channel_count(), 9);
        assert!(a.post_change_sample_count() > 0);

        a.reset();
        assert_eq!(a.current_channel_count(), DEFAULT_INITIAL_CHANNELS);
        assert_eq!(a.pre_change_sample_count(), 0);
        assert_eq!(a.post_change_sample_count(), 0);
        assert!(!a.is_plateau());
    }

    // ─── Multiple ramp-up cycles ────────────────────────────────────────────

    #[test]
    fn multiple_ramp_up_cycles() {
        let mut a = ChannelCountAdaptor::new(4, 1, 256, 0, 0);

        // Cycle 1: add channel, throughput improves → add another.
        fill_pre_change(&mut a, 65536, 1_000_000);
        a.notify_channel_added();
        for _ in 0..THROUGHPUT_WINDOW {
            a.record_throughput(ThroughputSample {
                bytes: 65536,
                duration_ns: 100_000,
            });
        }
        assert_eq!(a.tick(), TickAction::AddChannel);

        // Cycle 2: add channel, throughput degrades → remove.
        a.notify_channel_added();
        for _ in 0..THROUGHPUT_WINDOW {
            a.record_throughput(ThroughputSample {
                bytes: 65536,
                duration_ns: 10_000_000,
            });
        }
        assert_eq!(a.tick(), TickAction::RemoveChannel);
    }

    // ─── Zero-duration samples ──────────────────────────────────────────────

    #[test]
    fn zero_duration_samples_do_not_crash() {
        let mut a = ChannelCountAdaptor::new(4, 1, 256, 0, 0);

        // Fill with zero-duration samples.
        for _ in 0..THROUGHPUT_WINDOW {
            a.record_throughput(ThroughputSample {
                bytes: 65536,
                duration_ns: 0,
            });
        }

        // Should not panic.
        let action = a.tick();
        // With zero-duration throughput, pre_change_ewma stays 0,
        // so evaluate_change returns AddChannel (no baseline).
        assert_eq!(action, TickAction::AddChannel);
    }

    // ─── EWMA smoothing ─────────────────────────────────────────────────────

    #[test]
    fn ewma_smooths_throughput_measurements() {
        let mut a = ChannelCountAdaptor::new(4, 1, 256, 0, 0);

        // Fill pre-change with consistent throughput.
        fill_pre_change(&mut a, 65536, 1_000_000);

        // Add channel and fill post-change with same throughput.
        a.notify_channel_added();
        for _ in 0..THROUGHPUT_WINDOW {
            a.record_throughput(ThroughputSample {
                bytes: 65536,
                duration_ns: 1_000_000,
            });
        }

        // Same throughput → plateau (within threshold).
        let action = a.tick();
        assert_eq!(action, TickAction::Plateau);
    }

    // ─── Tick with no samples yet ───────────────────────────────────────────

    #[test]
    fn tick_with_no_samples_returns_add_channel() {
        let mut a = ChannelCountAdaptor::new(4, 1, 256, 0, 0);

        // No samples recorded yet, but interval has passed (0s).
        // Should return AddChannel since there's no baseline to compare.
        let action = a.tick();
        assert_eq!(action, TickAction::AddChannel);
    }
}
