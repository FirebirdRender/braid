use std::time::{Duration, Instant};

/// Per-channel health tracking for connection resilience.
///
/// Monitors failure counts, consecutive failures, and last activity
/// for each UDP channel. Provides methods to determine if a channel
/// should be considered dead and whether the channel set as a whole
/// has failed beyond recovery.
#[derive(Debug, Clone)]
pub struct ChannelHealth {
    /// Per-channel failure counters.
    channels: Vec<ChannelState>,
    /// Maximum consecutive failures before a channel is marked dead.
    max_consecutive_failures: u32,
    /// Duration without activity after which a channel is considered stale.
    activity_timeout: Duration,
}

/// Health state for a single channel.
#[derive(Debug, Clone)]
struct ChannelState {
    /// Whether this channel is currently alive.
    alive: bool,
    /// Consecutive send failures on this channel.
    consecutive_failures: u32,
    /// Total failures on this channel.
    total_failures: u64,
    /// Timestamp of the last successful send.
    last_success: Option<Instant>,
    /// Timestamp of the last failure.
    last_failure: Option<Instant>,
}

impl Default for ChannelState {
    fn default() -> Self {
        Self {
            alive: true,
            consecutive_failures: 0,
            total_failures: 0,
            last_success: None,
            last_failure: None,
        }
    }
}

impl ChannelHealth {
    /// Create a new `ChannelHealth` tracker for `channel_count` channels.
    ///
    /// * `channel_count` - Number of UDP channels to track.
    /// * `max_consecutive_failures` - Max consecutive failures before a channel is dead.
    /// * `activity_timeout` - Duration of inactivity before a channel is stale.
    pub fn new(
        channel_count: usize,
        max_consecutive_failures: u32,
        activity_timeout: Duration,
    ) -> Self {
        Self {
            channels: vec![ChannelState::default(); channel_count],
            max_consecutive_failures,
            activity_timeout,
        }
    }

    /// Record a successful send on the given channel.
    pub fn record_success(&mut self, channel_index: usize) {
        if channel_index < self.channels.len() {
            let state = &mut self.channels[channel_index];
            state.alive = true;
            state.consecutive_failures = 0;
            state.last_success = Some(Instant::now());
        }
    }

    /// Record a send failure on the given channel.
    ///
    /// Returns `true` if the channel was just marked dead by this failure.
    pub fn record_failure(&mut self, channel_index: usize) -> bool {
        if channel_index < self.channels.len() {
            let state = &mut self.channels[channel_index];
            state.consecutive_failures += 1;
            state.total_failures += 1;
            state.last_failure = Some(Instant::now());
            if state.consecutive_failures >= self.max_consecutive_failures && state.alive {
                state.alive = false;
                return true;
            }
        }
        false
    }

    /// Check whether a specific channel is alive.
    pub fn is_alive(&self, channel_index: usize) -> bool {
        self.channels
            .get(channel_index)
            .map(|s| s.alive)
            .unwrap_or(false)
    }

    /// Check whether a channel has been inactive beyond the timeout.
    pub fn is_stale(&self, channel_index: usize) -> bool {
        self.channels.get(channel_index).is_some_and(|s| {
            if let Some(last) = s.last_success {
                last.elapsed() > self.activity_timeout
            } else {
                // No success yet — only stale if we've had failures
                s.last_failure.is_some()
            }
        })
    }

    /// Number of alive channels.
    pub fn alive_count(&self) -> usize {
        self.channels.iter().filter(|s| s.alive).count()
    }

    /// Total channel count.
    pub fn total_count(&self) -> usize {
        self.channels.len()
    }

    /// Whether all channels are dead.
    pub fn all_dead(&self) -> bool {
        self.alive_count() == 0
    }

    /// Get consecutive failures for a specific channel.
    pub fn consecutive_failures(&self, channel_index: usize) -> u32 {
        self.channels
            .get(channel_index)
            .map(|s| s.consecutive_failures)
            .unwrap_or(0)
    }

    /// Get total failures for a specific channel.
    pub fn total_failures(&self, channel_index: usize) -> u64 {
        self.channels
            .get(channel_index)
            .map(|s| s.total_failures)
            .unwrap_or(0)
    }

    /// Reset all channels to alive state.
    pub fn reset_all(&mut self) {
        for state in &mut self.channels {
            state.alive = true;
            state.consecutive_failures = 0;
        }
    }
}

impl Default for ChannelHealth {
    fn default() -> Self {
        Self::new(1, 3, Duration::from_secs(30))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_state_all_alive() {
        let health = ChannelHealth::new(4, 3, Duration::from_secs(30));
        assert_eq!(health.total_count(), 4);
        assert_eq!(health.alive_count(), 4);
        assert!(!health.all_dead());
    }

    #[test]
    fn record_failure_marks_channel_dead() {
        let mut health = ChannelHealth::new(2, 2, Duration::from_secs(30));
        assert!(!health.record_failure(0));
        assert!(health.is_alive(0));
        assert!(health.record_failure(0));
        assert!(!health.is_alive(0));
        assert_eq!(health.alive_count(), 1);
    }

    #[test]
    fn record_success_resets_failures() {
        let mut health = ChannelHealth::new(1, 2, Duration::from_secs(30));
        health.record_failure(0);
        health.record_success(0);
        assert!(health.is_alive(0));
        assert_eq!(health.consecutive_failures(0), 0);
    }

    #[test]
    fn all_dead_detected() {
        let mut health = ChannelHealth::new(2, 1, Duration::from_secs(30));
        assert!(!health.all_dead());
        health.record_failure(0);
        health.record_failure(1);
        assert!(health.all_dead());
    }

    #[test]
    fn total_failures_tracked() {
        let mut health = ChannelHealth::new(1, 3, Duration::from_secs(30));
        health.record_failure(0);
        health.record_failure(0);
        health.record_success(0);
        health.record_failure(0);
        assert_eq!(health.total_failures(0), 3);
    }

    #[test]
    fn out_of_range_graceful() {
        let health = ChannelHealth::new(2, 3, Duration::from_secs(30));
        assert!(!health.is_alive(5));
        assert_eq!(health.consecutive_failures(5), 0);
        assert_eq!(health.total_failures(5), 0);
    }

    #[test]
    fn reset_all_restores_alive() {
        let mut health = ChannelHealth::new(4, 1, Duration::from_secs(30));
        health.record_failure(0);
        health.record_failure(1);
        health.record_failure(2);
        assert_eq!(health.alive_count(), 1);
        health.reset_all();
        assert_eq!(health.alive_count(), 4);
    }

    #[test]
    fn default_has_one_channel() {
        let health: ChannelHealth = Default::default();
        assert_eq!(health.total_count(), 1);
        assert_eq!(health.max_consecutive_failures, 3);
    }
}