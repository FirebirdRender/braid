/// Adaptive chunk size algorithm using AIMD (Additive Increase Multiplicative Decrease).
///
/// # Overview
///
/// The `ChunkSizeAdaptor` tracks throughput over a sliding window of recently
/// completed chunks and adjusts the chunk size using an AIMD control law:
///
/// - **Throughput increase** (current EWMA > previous EWMA): grow chunk size by 25%
///   (`chunk *= 1.25`), capped at `max_chunk`.
/// - **Throughput decrease** (current EWMA <= previous EWMA): shrink chunk size by 50%
///   (`chunk *= 0.5`), capped at `min_chunk`.
/// - **CRC error**: immediate 50% shrink, capped at `min_chunk`.
///
/// Throughput is smoothed with an EWMA filter: `ewma = prev_ewma * 0.7 + current * 0.3`.
///
/// # Clamping
///
/// The chunk size is always clamped to [`MIN_CHUNK`, `MAX_CHUNK`]:
/// - `MIN_CHUNK = 1024`
/// - `MAX_CHUNK = 1048576` (1 MiB)
///
/// # User Override
///
/// When a fixed chunk size is provided via `--chunk-size`, the adaptor is
/// disabled and always returns the fixed value.
use std::sync::atomic::{AtomicBool, Ordering};

/// Minimum chunk size in bytes.
pub const MIN_CHUNK: usize = 1024;

/// Maximum chunk size in bytes (capped at u16::MAX = 65535 for protocol compatibility).
pub const MAX_CHUNK: usize = 65535;

/// Default throughput tracking window size (number of chunks).
pub const DEFAULT_WINDOW_SIZE: usize = 100;

/// Default initial chunk size (capped at u16::MAX = 65535 for protocol compatibility).
pub const DEFAULT_INITIAL_CHUNK: usize = 65535;

/// EWMA smoothing factor for throughput (alpha = 0.3, so new = 0.3 * current + 0.7 * prev).
const EWMA_ALPHA: f64 = 0.3;

/// Growth factor on throughput improvement (25%).
const GROWTH_FACTOR: f64 = 1.25;

/// Shrink factor on throughput degradation or CRC error (50%).
const SHRINK_FACTOR: f64 = 0.5;

/// A single throughput sample: bytes transferred over a measured duration.
#[derive(Debug, Clone, Copy)]
pub struct ThroughputSample {
    /// Number of bytes transferred in this sample.
    pub bytes: u64,
    /// Duration of the sample in nanoseconds.
    pub duration_ns: u64,
}

/// Adaptive chunk size controller using AIMD.
///
/// # Usage
///
/// ```ignore
/// let mut adaptor = ChunkSizeAdaptor::new(1024, 1048576, 100);
///
/// // Record throughput samples as chunks complete:
/// adaptor.record_throughput(ThroughputSample { bytes: 1024, duration_ns: 1_000_000 });
///
/// // On CRC error:
/// adaptor.record_crc_error();
///
/// // Get current chunk size:
/// let chunk_size = adaptor.current_chunk_size();
/// ```
#[derive(Debug)]
pub struct ChunkSizeAdaptor {
    /// Current chunk size in bytes.
    current_chunk_size: usize,
    /// Minimum allowed chunk size.
    min_chunk: usize,
    /// Maximum allowed chunk size.
    max_chunk: usize,
    /// Number of throughput samples to track in the sliding window.
    window_size: usize,
    /// Sliding window of throughput samples.
    samples: Vec<ThroughputSample>,
    /// Previous EWMA throughput value (bytes per nanosecond).
    prev_ewma: f64,
    /// Whether the adaptor is disabled (user override via --chunk-size).
    disabled: AtomicBool,
    /// Fixed chunk size to use when disabled.
    fixed_chunk_size: usize,
}

impl ChunkSizeAdaptor {
    /// Create a new `ChunkSizeAdaptor`.
    ///
    /// * `initial_chunk_size` - Starting chunk size (clamped to [min, max]).
    /// * `min_chunk` - Minimum allowed chunk size.
    /// * `max_chunk` - Maximum allowed chunk size.
    /// * `window_size` - Number of throughput samples to track.
    pub fn new(
        initial_chunk_size: usize,
        min_chunk: usize,
        max_chunk: usize,
        window_size: usize,
    ) -> Self {
        assert!(min_chunk > 0, "min_chunk must be positive");
        assert!(max_chunk >= min_chunk, "max_chunk must be >= min_chunk");
        assert!(window_size > 0, "window_size must be positive");

        let clamped = initial_chunk_size.clamp(min_chunk, max_chunk);

        Self {
            current_chunk_size: clamped,
            min_chunk,
            max_chunk,
            window_size,
            samples: Vec::with_capacity(window_size),
            prev_ewma: 0.0,
            disabled: AtomicBool::new(false),
            fixed_chunk_size: clamped,
        }
    }

    /// Create a new `ChunkSizeAdaptor` with default parameters.
    ///
    /// Defaults: initial = `MIN_CHUNK` (1024), min = `MIN_CHUNK`, max = `MAX_CHUNK`,
    /// window = `DEFAULT_WINDOW_SIZE` (100).
    pub fn new_default() -> Self {
        Self::new(
            DEFAULT_INITIAL_CHUNK,
            MIN_CHUNK,
            MAX_CHUNK,
            DEFAULT_WINDOW_SIZE,
        )
    }

    /// Disable adaptive behavior and use a fixed chunk size.
    ///
    /// This is called when the user provides `--chunk-size`.
    pub fn set_fixed(&mut self, fixed_size: usize) {
        self.fixed_chunk_size = fixed_size.clamp(self.min_chunk, self.max_chunk);
        self.disabled.store(true, Ordering::Relaxed);
    }

    /// Returns `true` if the adaptor is disabled (fixed chunk size mode).
    pub fn is_disabled(&self) -> bool {
        self.disabled.load(Ordering::Relaxed)
    }

    /// Returns the current chunk size.
    ///
    /// If the adaptor is disabled, returns the fixed chunk size.
    pub fn current_chunk_size(&self) -> usize {
        if self.is_disabled() {
            return self.fixed_chunk_size;
        }
        self.current_chunk_size
    }

    /// Returns the minimum chunk size.
    pub fn min_chunk(&self) -> usize {
        self.min_chunk
    }

    /// Returns the maximum chunk size.
    pub fn max_chunk(&self) -> usize {
        self.max_chunk
    }

    /// Returns the number of samples currently in the window.
    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }

    /// Returns the configured window size.
    pub fn window_size(&self) -> usize {
        self.window_size
    }

    /// Record a throughput sample from a completed chunk.
    ///
    /// This triggers the AIMD adjustment when the window is full: the adaptor
    /// computes the EWMA of the current window, compares it to the previous
    /// EWMA, and grows or shrinks the chunk size accordingly.
    pub fn record_throughput(&mut self, sample: ThroughputSample) {
        if self.is_disabled() {
            return;
        }

        // Add sample to the sliding window.
        if self.samples.len() >= self.window_size {
            self.samples.remove(0);
        }
        self.samples.push(sample);

        // Only run AIMD when the window is full.
        if self.samples.len() < self.window_size {
            return;
        }

        // Compute average throughput for the current window.
        let total_bytes: u64 = self.samples.iter().map(|s| s.bytes).sum();
        let total_duration_ns: u64 = self.samples.iter().map(|s| s.duration_ns).sum();

        if total_duration_ns == 0 {
            return; // Avoid division by zero.
        }

        let current_throughput = total_bytes as f64 / total_duration_ns as f64;

        // Apply EWMA smoothing.
        let ewma = if self.prev_ewma == 0.0 {
            // First measurement: initialize EWMA directly.
            current_throughput
        } else {
            self.prev_ewma * (1.0 - EWMA_ALPHA) + current_throughput * EWMA_ALPHA
        };

        // Compare with previous EWMA to decide growth or shrink.
        if ewma > self.prev_ewma {
            // Throughput improved: grow chunk size by 25%.
            let new_size = (self.current_chunk_size as f64 * GROWTH_FACTOR) as usize;
            self.current_chunk_size = new_size.clamp(self.min_chunk, self.max_chunk);
        } else {
            // Throughput degraded or stayed same: shrink chunk size by 50%.
            let new_size = (self.current_chunk_size as f64 * SHRINK_FACTOR) as usize;
            self.current_chunk_size = new_size.clamp(self.min_chunk, self.max_chunk);
        }

        self.prev_ewma = ewma;
    }

    /// Record a CRC error, triggering an immediate 50% shrink.
    ///
    /// This is called when the receiver reports a CRC mismatch for a chunk.
    pub fn record_crc_error(&mut self) {
        if self.is_disabled() {
            return;
        }

        let new_size = (self.current_chunk_size as f64 * SHRINK_FACTOR) as usize;
        self.current_chunk_size = new_size.clamp(self.min_chunk, self.max_chunk);
    }

    /// Reset the adaptor to its initial state.
    ///
    /// Clears all throughput samples and resets the EWMA.
    pub fn reset(&mut self) {
        self.current_chunk_size = DEFAULT_INITIAL_CHUNK.clamp(self.min_chunk, self.max_chunk);
        self.samples.clear();
        self.prev_ewma = 0.0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── Helper: fill window with uniform throughput samples ────────────────

    /// Fill the window with samples of uniform throughput to trigger an AIMD
    /// evaluation. Returns the adaptor with a full window.
    fn fill_window(adaptor: &mut ChunkSizeAdaptor, bytes: u64, duration_ns: u64) {
        for _ in 0..adaptor.window_size {
            adaptor.record_throughput(ThroughputSample { bytes, duration_ns });
        }
    }

    // ─── Construction ───────────────────────────────────────────────────────

    #[test]
    fn starts_at_default_chunk() {
        let a = ChunkSizeAdaptor::new_default();
        assert_eq!(a.current_chunk_size(), DEFAULT_INITIAL_CHUNK);
        assert_eq!(a.min_chunk(), MIN_CHUNK);
        assert_eq!(a.max_chunk(), MAX_CHUNK);
        assert_eq!(a.window_size(), DEFAULT_WINDOW_SIZE);
    }

    #[test]
    fn clamps_initial_chunk_size() {
        let a = ChunkSizeAdaptor::new(0, MIN_CHUNK, MAX_CHUNK, 10);
        assert_eq!(a.current_chunk_size(), MIN_CHUNK);

        let a = ChunkSizeAdaptor::new(2_000_000, MIN_CHUNK, MAX_CHUNK, 10);
        assert_eq!(a.current_chunk_size(), MAX_CHUNK);
    }

    #[test]
    fn custom_bounds() {
        let a = ChunkSizeAdaptor::new(4096, 2048, 8192, 10);
        assert_eq!(a.min_chunk(), 2048);
        assert_eq!(a.max_chunk(), 8192);
        assert_eq!(a.current_chunk_size(), 4096);
    }

    #[test]
    #[should_panic(expected = "min_chunk must be positive")]
    fn rejects_zero_min_chunk() {
        ChunkSizeAdaptor::new(1024, 0, 1048576, 10);
    }

    #[test]
    #[should_panic(expected = "max_chunk must be >= min_chunk")]
    fn rejects_max_less_than_min() {
        ChunkSizeAdaptor::new(1024, 2048, 1024, 10);
    }

    #[test]
    #[should_panic(expected = "window_size must be positive")]
    fn rejects_zero_window() {
        ChunkSizeAdaptor::new(1024, 1024, 1048576, 0);
    }

    // ─── Growth on throughput improvement ───────────────────────────────────

    #[test]
    fn grows_on_throughput_improvement() {
        let mut a = ChunkSizeAdaptor::new(MIN_CHUNK, MIN_CHUNK, MAX_CHUNK, 3);

        // Fill window with low throughput (slow).
        fill_window(&mut a, 1024, 1_000_000);

        // Now fill window with higher throughput (faster).
        // prev_ewma was initialized from the first window's throughput.
        // Second window has higher throughput → should grow.
        let before = a.current_chunk_size();
        fill_window(&mut a, 1024, 100_000); // 10x faster

        let after = a.current_chunk_size();
        assert!(
            after > before,
            "expected growth from {} to >{}",
            before,
            before
        );
    }

    #[test]
    fn growth_is_25_percent_single_step() {
        let mut a = ChunkSizeAdaptor::new(4096, MIN_CHUNK, MAX_CHUNK, 3);

        // Fill window with low throughput to set baseline EWMA.
        fill_window(&mut a, 4096, 1_000_000);

        let before = a.current_chunk_size();

        // Now add one more sample with higher throughput.
        // The window is full, so this triggers an AIMD evaluation.
        // EWMA comparison: prev_ewma (low) vs new_ewma (higher) → growth.
        a.record_throughput(ThroughputSample {
            bytes: 4096,
            duration_ns: 100_000,
        });

        let expected_growth = (before as f64 * GROWTH_FACTOR) as usize;
        assert_eq!(
            a.current_chunk_size(),
            expected_growth,
            "expected growth from {} to {}",
            before,
            expected_growth
        );
    }

    // ─── Shrink on throughput degradation ───────────────────────────────────

    #[test]
    fn shrinks_on_throughput_degradation() {
        let mut a = ChunkSizeAdaptor::new(MAX_CHUNK, MIN_CHUNK, MAX_CHUNK, 3);

        // Fill window with high throughput.
        fill_window(&mut a, 1048576, 100_000);

        // Fill window with lower throughput.
        let before = a.current_chunk_size();
        fill_window(&mut a, 1048576, 1_000_000); // 10x slower

        let after = a.current_chunk_size();
        assert!(
            after < before,
            "expected shrink from {} to <{}",
            before,
            before
        );
    }

    #[test]
    fn shrink_is_50_percent_single_step() {
        let mut a = ChunkSizeAdaptor::new(65536, MIN_CHUNK, MAX_CHUNK, 3);

        // Fill window with high throughput to set baseline EWMA.
        fill_window(&mut a, 65536, 100_000);

        let before = a.current_chunk_size();

        // Add one sample with lower throughput → triggers AIMD shrink.
        a.record_throughput(ThroughputSample {
            bytes: 65536,
            duration_ns: 1_000_000,
        });

        let expected_shrink = (before as f64 * SHRINK_FACTOR) as usize;
        assert_eq!(
            a.current_chunk_size(),
            expected_shrink,
            "expected shrink from {} to {}",
            before,
            expected_shrink
        );
    }

    // ─── Clamping ───────────────────────────────────────────────────────────

    #[test]
    fn growth_capped_at_max() {
        let mut a = ChunkSizeAdaptor::new(MAX_CHUNK, MIN_CHUNK, MAX_CHUNK, 3);

        // Fill window with low throughput.
        fill_window(&mut a, MAX_CHUNK as u64, 1_000_000);

        // Fill window with higher throughput — growth should be capped.
        fill_window(&mut a, MAX_CHUNK as u64, 100_000);

        assert_eq!(a.current_chunk_size(), MAX_CHUNK);
    }

    #[test]
    fn shrink_capped_at_min() {
        let mut a = ChunkSizeAdaptor::new(MIN_CHUNK, MIN_CHUNK, MAX_CHUNK, 3);

        // Fill window with high throughput.
        fill_window(&mut a, MIN_CHUNK as u64, 100_000);

        // Fill window with lower throughput — shrink should be capped.
        fill_window(&mut a, MIN_CHUNK as u64, 1_000_000);

        assert_eq!(a.current_chunk_size(), MIN_CHUNK);
    }

    // ─── CRC error immediate shrink ─────────────────────────────────────────

    #[test]
    fn crc_error_triggers_immediate_shrink() {
        let mut a = ChunkSizeAdaptor::new(65536, MIN_CHUNK, MAX_CHUNK, 5);

        let before = a.current_chunk_size();
        a.record_crc_error();

        let expected = (before as f64 * SHRINK_FACTOR) as usize;
        assert_eq!(a.current_chunk_size(), expected);
    }

    #[test]
    fn crc_error_shrink_capped_at_min() {
        let mut a = ChunkSizeAdaptor::new(MIN_CHUNK, MIN_CHUNK, MAX_CHUNK, 5);

        a.record_crc_error();
        assert_eq!(a.current_chunk_size(), MIN_CHUNK);
    }

    // ─── EWMA smoothing ─────────────────────────────────────────────────────

    #[test]
    fn ewma_smooths_throughput_measurements() {
        let mut a = ChunkSizeAdaptor::new(MIN_CHUNK, MIN_CHUNK, MAX_CHUNK, 3);

        // First window: initialize EWMA.
        fill_window(&mut a, 1024, 1_000_000);
        let after_first = a.current_chunk_size();

        // Second window: same throughput → no improvement → shrink.
        fill_window(&mut a, 1024, 1_000_000);
        let after_second = a.current_chunk_size();

        // Same throughput should cause shrink (not improvement).
        assert!(
            after_second <= after_first,
            "same throughput should not cause growth"
        );
    }

    // ─── Disabled mode (user override) ──────────────────────────────────────

    #[test]
    fn disabled_mode_returns_fixed_size() {
        let mut a = ChunkSizeAdaptor::new_default();
        a.set_fixed(8192);

        assert!(a.is_disabled());
        assert_eq!(a.current_chunk_size(), 8192);

        // record_throughput should be a no-op when disabled.
        a.record_throughput(ThroughputSample {
            bytes: 999999,
            duration_ns: 1,
        });
        assert_eq!(a.current_chunk_size(), 8192);

        // CRC error should be a no-op when disabled.
        a.record_crc_error();
        assert_eq!(a.current_chunk_size(), 8192);
    }

    #[test]
    fn disabled_mode_clamps_fixed_size() {
        let mut a = ChunkSizeAdaptor::new_default();
        a.set_fixed(0);
        assert_eq!(a.current_chunk_size(), MIN_CHUNK);

        a.set_fixed(9_999_999);
        assert_eq!(a.current_chunk_size(), MAX_CHUNK);
    }

    // ─── Reset ──────────────────────────────────────────────────────────────

    #[test]
    fn reset_restores_initial_state() {
        let mut a = ChunkSizeAdaptor::new(65536, MIN_CHUNK, MAX_CHUNK, 5);

        fill_window(&mut a, 65536, 100_000);
        fill_window(&mut a, 65536, 1_000_000);
        assert!(a.current_chunk_size() < 65536);
        assert_eq!(a.sample_count(), 5);

        a.reset();
        assert_eq!(a.current_chunk_size(), DEFAULT_INITIAL_CHUNK);
        assert_eq!(a.sample_count(), 0);
    }

    // ─── Window size tracking ───────────────────────────────────────────────

    #[test]
    fn window_fills_and_ages_out() {
        let mut a = ChunkSizeAdaptor::new(MIN_CHUNK, MIN_CHUNK, MAX_CHUNK, 3);

        assert_eq!(a.sample_count(), 0);

        // Add 2 samples: window not full yet.
        a.record_throughput(ThroughputSample {
            bytes: 1024,
            duration_ns: 1000,
        });
        a.record_throughput(ThroughputSample {
            bytes: 1024,
            duration_ns: 1000,
        });
        assert_eq!(a.sample_count(), 2);

        // Add 3rd sample: window is full.
        a.record_throughput(ThroughputSample {
            bytes: 1024,
            duration_ns: 1000,
        });
        assert_eq!(a.sample_count(), 3);

        // Add 4th sample: oldest is evicted, still 3 samples.
        a.record_throughput(ThroughputSample {
            bytes: 1024,
            duration_ns: 1000,
        });
        assert_eq!(a.sample_count(), 3);
    }

    // ─── Throughput sample with zero duration ───────────────────────────────

    #[test]
    fn zero_duration_does_not_crash() {
        let mut a = ChunkSizeAdaptor::new(MIN_CHUNK, MIN_CHUNK, MAX_CHUNK, 3);

        // Fill window with zero-duration samples.
        for _ in 0..3 {
            a.record_throughput(ThroughputSample {
                bytes: 1024,
                duration_ns: 0,
            });
        }

        // Should not panic, chunk size should remain unchanged.
        assert_eq!(a.current_chunk_size(), MIN_CHUNK);
    }

    // ─── Configurable window size ───────────────────────────────────────────

    #[test]
    fn custom_window_size() {
        let mut a = ChunkSizeAdaptor::new(MIN_CHUNK, MIN_CHUNK, MAX_CHUNK, 10);
        assert_eq!(a.window_size(), 10);

        // Fill window with 10 samples.
        fill_window(&mut a, 1024, 1_000_000);
        assert_eq!(a.sample_count(), 10);
    }

    // ─── Multiple AIMD cycles ───────────────────────────────────────────────

    #[test]
    fn multiple_aimd_cycles() {
        let mut a = ChunkSizeAdaptor::new(4096, MIN_CHUNK, MAX_CHUNK, 3);

        // Cycle 1: improve → grow.
        fill_window(&mut a, 4096, 1_000_000);
        fill_window(&mut a, 4096, 100_000);
        assert!(a.current_chunk_size() > 4096);

        // Cycle 2: degrade → shrink.
        let mid = a.current_chunk_size();
        fill_window(&mut a, 4096, 1_000_000);
        assert!(a.current_chunk_size() < mid);
    }
}
