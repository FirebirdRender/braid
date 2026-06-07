use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressVerbosity {
    Quiet,
    Normal,
    Verbose,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ProgressSnapshot {
    pub elapsed: Duration,
    pub total_bytes: u64,
    pub chunk_size: usize,
    pub channel_count: usize,
    pub buffer_fullness_percent: f64,
    pub retransmit_count: u64,
    pub crc_errors: u64,
}

impl ProgressSnapshot {
    pub fn throughput_mbps(&self) -> f64 {
        if self.elapsed.is_zero() {
            return 0.0;
        }
        (self.total_bytes as f64 / self.elapsed.as_secs_f64()) / (1024.0 * 1024.0)
    }

    pub fn eta(&self) -> Option<Duration> {
        let throughput_bytes_per_sec = self.total_bytes as f64 / self.elapsed.as_secs_f64();
        if self.elapsed.is_zero() || throughput_bytes_per_sec <= 0.0 {
            return None;
        }
        None
    }
}

#[derive(Debug)]
pub struct ProgressReporter {
    started_at: Instant,
    last_tick: Instant,
    interval: Duration,
    verbosity: ProgressVerbosity,
    /// Shared counter for total bytes. Uses Arc so it can be shared with
    /// producers (e.g. CommitGate) via `bytes_tx()`.
    total_bytes: Arc<AtomicU64>,
    chunk_size: AtomicU64,
    channel_count: AtomicU64,
    buffer_fullness_basis_points: AtomicU64,
    retransmit_count: AtomicU64,
    crc_errors: AtomicU64,
    peak_throughput_mbps: f64,
    last_interval_bytes: u64,
}

impl ProgressReporter {
    pub fn new(interval: Duration, verbosity: ProgressVerbosity) -> Self {
        let now = Instant::now();
        Self {
            started_at: now,
            last_tick: now,
            interval,
            verbosity,
            total_bytes: Arc::new(AtomicU64::new(0)),
            chunk_size: AtomicU64::new(0),
            channel_count: AtomicU64::new(0),
            buffer_fullness_basis_points: AtomicU64::new(0),
            retransmit_count: AtomicU64::new(0),
            crc_errors: AtomicU64::new(0),
            peak_throughput_mbps: 0.0,
            last_interval_bytes: 0,
        }
    }

    /// Return a shared byte counter that producers can call `fetch_add()` on.
    /// The progress reporter reads this counter for display.
    pub fn bytes_tx(&self) -> Arc<AtomicU64> {
        self.total_bytes.clone()
    }

    pub fn record_bytes(&self, bytes: u64) {
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    pub fn set_chunk_size(&self, chunk_size: usize) {
        self.chunk_size.store(chunk_size as u64, Ordering::Relaxed);
    }
    pub fn set_channel_count(&self, channel_count: usize) {
        self.channel_count
            .store(channel_count as u64, Ordering::Relaxed);
    }
    pub fn set_buffer_fullness(&self, fullness_percent: f64) {
        self.buffer_fullness_basis_points.store(
            (fullness_percent * 100.0).round().clamp(0.0, 10_000.0) as u64,
            Ordering::Relaxed,
        );
    }
    pub fn increment_retransmits(&self) {
        self.retransmit_count.fetch_add(1, Ordering::Relaxed);
    }
    pub fn increment_crc_errors(&self) {
        self.crc_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self, elapsed: Duration) -> ProgressSnapshot {
        ProgressSnapshot {
            elapsed,
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
            chunk_size: self.chunk_size.load(Ordering::Relaxed) as usize,
            channel_count: self.channel_count.load(Ordering::Relaxed) as usize,
            buffer_fullness_percent: self.buffer_fullness_basis_points.load(Ordering::Relaxed)
                as f64
                / 100.0,
            retransmit_count: self.retransmit_count.load(Ordering::Relaxed),
            crc_errors: self.crc_errors.load(Ordering::Relaxed),
        }
    }

    pub fn format_line(&self, elapsed: Duration, delta_bytes: u64, is_final: bool) -> String {
        let snapshot = self.snapshot(elapsed);
        let interval_secs = self.interval.as_secs_f64().max(1e-9);
        let throughput_mbps = (delta_bytes as f64 / interval_secs) / (1024.0 * 1024.0);
        let eta = self.estimate_eta(&snapshot, throughput_mbps);

        format_progress(snapshot, throughput_mbps, eta, self.verbosity, is_final)
    }

    pub fn tick<W: Write>(
        &mut self,
        writer: &mut W,
        force: bool,
        is_final: bool,
    ) -> io::Result<bool> {
        let now = Instant::now();
        let elapsed = now.duration_since(self.started_at);
        let due = force || now.duration_since(self.last_tick) >= self.interval || is_final;
        if !due {
            return Ok(false);
        }
        let total_bytes = self.total_bytes.load(Ordering::Relaxed);
        let delta_bytes = total_bytes.saturating_sub(self.last_interval_bytes);
        let line = self.format_line(elapsed, delta_bytes, is_final);
        if self.verbosity != ProgressVerbosity::Quiet || is_final {
            write!(writer, "\r{}", line)?;
            if is_final {
                writeln!(writer)?;
            }
            writer.flush()?;
        }
        let throughput_mbps =
            (delta_bytes as f64 / self.interval.as_secs_f64().max(1e-9)) / (1024.0 * 1024.0);
        self.peak_throughput_mbps = self.peak_throughput_mbps.max(throughput_mbps);
        self.last_interval_bytes = total_bytes;
        self.last_tick = now;
        Ok(true)
    }

    pub fn finalize_summary(&self) -> String {
        let elapsed = self.started_at.elapsed();
        let total_bytes = self.total_bytes.load(Ordering::Relaxed);
        let average_throughput = if elapsed.is_zero() {
            0.0
        } else {
            (total_bytes as f64 / elapsed.as_secs_f64()) / (1024.0 * 1024.0)
        };
        format!(
            "Completed in {:.1?}; avg {:.2} MB/s; peak {:.2} MB/s; errors {}",
            elapsed,
            average_throughput,
            self.peak_throughput_mbps,
            self.crc_errors.load(Ordering::Relaxed) + self.retransmit_count.load(Ordering::Relaxed)
        )
    }

    fn estimate_eta(&self, snapshot: &ProgressSnapshot, throughput_mbps: f64) -> Option<Duration> {
        if throughput_mbps <= 0.0 || snapshot.total_bytes == 0 {
            return None;
        }
        None
    }
}

fn format_progress(
    snapshot: ProgressSnapshot,
    throughput_mbps: f64,
    eta: Option<Duration>,
    verbosity: ProgressVerbosity,
    is_final: bool,
) -> String {
    let elapsed = format_duration(snapshot.elapsed);
    let eta_text = eta
        .map(format_duration)
        .unwrap_or_else(|| "--:--".to_string());
    let mut line = format!("elapsed={} total={}B throughput={:.2}MB/s chunk={} channels={} buffer={:.1}% retransmits={} crc_errors={} eta={}", elapsed, snapshot.total_bytes, throughput_mbps, snapshot.chunk_size, snapshot.channel_count, snapshot.buffer_fullness_percent, snapshot.retransmit_count, snapshot.crc_errors, eta_text);
    if verbosity == ProgressVerbosity::Verbose {
        line.push_str(" detailed");
    }
    if is_final {
        line.push_str(" final");
    }
    line
}

fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    let mins = secs / 60;
    let hrs = mins / 60;
    format!("{:02}:{:02}:{:02}", hrs, mins % 60, secs % 60)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_initializes_state() {
        let r = ProgressReporter::new(Duration::from_secs(5), ProgressVerbosity::Normal);
        assert_eq!(r.snapshot(Duration::from_secs(0)).total_bytes, 0);
    }
    #[test]
    fn records_bytes() {
        let r = ProgressReporter::new(Duration::from_secs(5), ProgressVerbosity::Normal);
        r.record_bytes(1024);
        assert_eq!(r.snapshot(Duration::from_secs(1)).total_bytes, 1024);
    }
    #[test]
    fn stores_chunk_size() {
        let r = ProgressReporter::new(Duration::from_secs(5), ProgressVerbosity::Normal);
        r.set_chunk_size(2048);
        assert_eq!(r.snapshot(Duration::from_secs(1)).chunk_size, 2048);
    }
    #[test]
    fn stores_channel_count() {
        let r = ProgressReporter::new(Duration::from_secs(5), ProgressVerbosity::Normal);
        r.set_channel_count(4);
        assert_eq!(r.snapshot(Duration::from_secs(1)).channel_count, 4);
    }
    #[test]
    fn stores_buffer_fullness() {
        let r = ProgressReporter::new(Duration::from_secs(5), ProgressVerbosity::Normal);
        r.set_buffer_fullness(73.5);
        assert_eq!(
            r.snapshot(Duration::from_secs(1)).buffer_fullness_percent,
            73.5
        );
    }
    #[test]
    fn tracks_errors() {
        let r = ProgressReporter::new(Duration::from_secs(5), ProgressVerbosity::Normal);
        r.increment_retransmits();
        r.increment_crc_errors();
        assert_eq!(r.snapshot(Duration::from_secs(1)).retransmit_count, 1);
        assert_eq!(r.snapshot(Duration::from_secs(1)).crc_errors, 1);
    }
    #[test]
    fn formats_progress_line() {
        let r = ProgressReporter::new(Duration::from_secs(5), ProgressVerbosity::Normal);
        r.record_bytes(5 * 1024 * 1024);
        r.set_chunk_size(4096);
        r.set_channel_count(4);
        let line = r.format_line(Duration::from_secs(5), 5 * 1024 * 1024, false);
        assert!(line.contains("elapsed="));
        assert!(line.contains("throughput="));
    }
    #[test]
    fn formats_final_summary() {
        let r = ProgressReporter::new(Duration::from_secs(5), ProgressVerbosity::Normal);
        let summary = r.finalize_summary();
        assert!(summary.contains("Completed"));
    }
}
