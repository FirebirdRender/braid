use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::watch;
use tracing::{error, info};

use crate::error::BraidError;

/// Manages graceful shutdown for the Braid application.
///
/// # Architecture
///
/// `ShutdownManager` provides a two-level notification mechanism:
///
/// 1. **Atomic flag** (`is_shutting_down`) — lock-free check for hot paths
///    (e.g., per-fragment dispatch loops, CRC verification gates).
/// 2. **Watch channel** (`watch::Receiver<bool>`) — awaitable notification
///    for async tasks that need to block until shutdown is signalled
///    (e.g., sender splitter, receiver commit gate).
///
/// # Shutdown sequence
///
/// When a signal (SIGINT/SIGTERM) is received:
/// 1. Set the atomic flag (`is_shutting_down = true`) — all components
///    that poll this flag will stop accepting new work.
/// 2. Notify the watch channel — async tasks waiting on `wait_for_shutdown()`
///    will wake up and begin their cleanup.
/// 3. Components flush and EOS: sender flushes remaining fragments and
///    sends EOS markers; receiver flushes stdout and closes sockets.
/// 4. Exit with code 0 (complete) or 1 (interrupted).
///
/// # Error handling
///
/// - `ShutdownManager::check()` returns `Err(BraidError::Shutdown)` when
///   the shutdown flag is set, allowing `?` propagation in hot paths.
/// - Fatal errors (CRC mismatch at commit gate, all channels failed) should
///   trigger `ShutdownManager::initiate()` directly rather than waiting for
///   a signal.
pub struct ShutdownManager {
    /// Atomic flag set to `true` when shutdown is requested.
    flag: Arc<AtomicBool>,
    /// Watch channel sender — notifies all receivers on shutdown.
    tx: watch::Sender<bool>,
    /// Watch channel receiver — the primary consumer (cloned for tasks).
    #[allow(dead_code)]
    rx: watch::Receiver<bool>,
}

impl ShutdownManager {
    /// Create a new `ShutdownManager` in the running state.
    ///
    /// The initial flag is `false` (not shutting down) and the watch
    /// channel holds `false`.
    pub fn new() -> Self {
        let flag = Arc::new(AtomicBool::new(false));
        let (tx, rx) = watch::channel(false);
        Self { flag, tx, rx }
    }

    /// Returns `true` if shutdown has been requested.
    pub fn is_shutting_down(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }

    /// Returns `Err(BraidError::Shutdown)` if shutdown is requested.
    ///
    /// Convenience for `?` propagation in hot paths:
    ///
    /// ```ignore
    /// fn process_fragment(&self) -> Result<(), BraidError> {
    ///     shutdown.check()?;
    ///     // ... process fragment ...
    /// }
    /// ```
    pub fn check(&self) -> std::result::Result<(), BraidError> {
        if self.is_shutting_down() {
            Err(BraidError::Shutdown)
        } else {
            Ok(())
        }
    }

    /// Initiate the shutdown sequence.
    ///
    /// Sets the atomic flag and notifies the watch channel. Idempotent —
    /// subsequent calls are no-ops.
    pub fn initiate(&self) {
        if self.flag.swap(true, Ordering::AcqRel) {
            // Already shutting down — no-op.
            return;
        }
        info!("shutdown initiated");
        let _ = self.tx.send(true);
    }

    /// Return a clone of the watch channel receiver.
    ///
    /// Tasks can `await` this receiver to block until shutdown is signalled:
    ///
    /// ```ignore
    /// shutdown_rx.wait_for(|v| *v).await;
    /// // cleanup...
    /// ```
    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.tx.subscribe()
    }

    /// Return a clone of the atomic flag for lock-free checks.
    ///
    /// Hot-path components (e.g., fragment dispatch, CRC verification)
    /// should use this for zero-overhead polling:
    ///
    /// ```ignore
    /// if shutdown_flag.load(Ordering::Acquire) {
    ///     return Err(BraidError::Shutdown);
    /// }
    /// ```
    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.flag)
    }

    /// Register OS signal handlers (SIGINT and SIGTERM) that trigger
    /// shutdown when received.
    ///
    /// Returns a future that resolves when either signal is received.
    /// This should be spawned as a background task:
    ///
    /// ```ignore
    /// let manager = ShutdownManager::new();
    /// let handle = manager.clone();
    /// tokio::spawn(async move {
    ///     handle.await_signals().await;
    /// });
    /// ```
    pub async fn await_signals(&self) {
        // We need a separate future for each signal because tokio::signal
        // consumes the handler. Use a stream to handle both.
        let sigint = tokio::signal::unix::SignalKind::interrupt();
        let sigterm = tokio::signal::unix::SignalKind::terminate();

        let mut sigint_stream = match tokio::signal::unix::signal(sigint) {
            Ok(stream) => stream,
            Err(e) => {
                error!("failed to register SIGINT handler: {e}");
                return;
            }
        };

        let mut sigterm_stream = match tokio::signal::unix::signal(sigterm) {
            Ok(stream) => stream,
            Err(e) => {
                error!("failed to register SIGTERM handler: {e}");
                return;
            }
        };

        tokio::select! {
            _ = sigint_stream.recv() => {
                info!("received SIGINT, initiating graceful shutdown");
                self.initiate();
            }
            _ = sigterm_stream.recv() => {
                info!("received SIGTERM, initiating graceful shutdown");
                self.initiate();
            }
        }
    }
}

impl Default for ShutdownManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for ShutdownManager {
    fn clone(&self) -> Self {
        Self {
            flag: Arc::clone(&self.flag),
            tx: self.tx.clone(),
            rx: self.tx.subscribe(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    // ─── Construction ────────────────────────────────────────────────────────

    #[test]
    fn new_returns_not_shutting_down() {
        let mgr = ShutdownManager::new();
        assert!(!mgr.is_shutting_down());
        assert!(mgr.check().is_ok());
    }

    #[test]
    fn default_returns_not_shutting_down() {
        let mgr = ShutdownManager::default();
        assert!(!mgr.is_shutting_down());
    }

    // ─── Initiate ────────────────────────────────────────────────────────────

    #[test]
    fn initiate_sets_flag() {
        let mgr = ShutdownManager::new();
        mgr.initiate();
        assert!(mgr.is_shutting_down());
    }

    #[test]
    fn initiate_causes_check_to_return_error() {
        let mgr = ShutdownManager::new();
        mgr.initiate();
        let result = mgr.check();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), BraidError::Shutdown));
    }

    #[test]
    fn initiate_is_idempotent() {
        let mgr = ShutdownManager::new();
        mgr.initiate();
        mgr.initiate(); // Second call should be a no-op
        assert!(mgr.is_shutting_down());
    }

    // ─── Clone ───────────────────────────────────────────────────────────────

    #[test]
    fn clone_reflects_shutdown_state() {
        let mgr1 = ShutdownManager::new();
        let mgr2 = mgr1.clone();

        assert!(!mgr2.is_shutting_down());

        mgr1.initiate();
        assert!(mgr2.is_shutting_down());
    }

    #[test]
    fn cloned_check_returns_error_after_initiate() {
        let mgr1 = ShutdownManager::new();
        let mgr2 = mgr1.clone();

        mgr1.initiate();

        let result = mgr2.check();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), BraidError::Shutdown));
    }

    // ─── Subscribe (watch channel) ───────────────────────────────────────────

    #[tokio::test]
    async fn subscribe_receives_shutdown_notification() {
        let mgr = ShutdownManager::new();
        let mut rx = mgr.subscribe();

        // Spawn a task that waits for the notification using changed()
        let handle = tokio::spawn(async move {
            // changed() resolves when the watch value changes
            loop {
                if *rx.borrow() {
                    break;
                }
                if rx.changed().await.is_err() {
                    break;
                }
            }
        });

        // Trigger shutdown after a short delay
        tokio::time::sleep(Duration::from_millis(10)).await;
        mgr.initiate();

        // The task should complete
        let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
        assert!(result.is_ok(), "watch receiver should be notified");
    }

    #[tokio::test]
    async fn subscribe_clone_receives_notification() {
        let mgr = ShutdownManager::new();
        let rx1 = mgr.subscribe();
        let rx2 = mgr.subscribe();

        let handle1 = tokio::spawn(async move {
            let mut rx = rx1;
            loop {
                if *rx.borrow() {
                    break;
                }
                if rx.changed().await.is_err() {
                    break;
                }
            }
        });
        let handle2 = tokio::spawn(async move {
            let mut rx = rx2;
            loop {
                if *rx.borrow() {
                    break;
                }
                if rx.changed().await.is_err() {
                    break;
                }
            }
        });

        mgr.initiate();

        let r1 = tokio::time::timeout(Duration::from_secs(1), handle1).await;
        let r2 = tokio::time::timeout(Duration::from_secs(1), handle2).await;
        assert!(r1.is_ok(), "first subscriber should be notified");
        assert!(r2.is_ok(), "second subscriber should be notified");
    }

    // ─── Shutdown flag (Arc<AtomicBool>) ─────────────────────────────────────

    #[test]
    fn shutdown_flag_reflects_state() {
        let mgr = ShutdownManager::new();
        let flag = mgr.shutdown_flag();

        assert!(!flag.load(Ordering::Acquire));

        mgr.initiate();
        assert!(flag.load(Ordering::Acquire));
    }

    #[test]
    fn shutdown_flag_is_independent_clone() {
        let mgr = ShutdownManager::new();
        let flag1 = mgr.shutdown_flag();
        let flag2 = mgr.shutdown_flag();

        mgr.initiate();
        assert!(flag1.load(Ordering::Acquire));
        assert!(flag2.load(Ordering::Acquire));
    }

    // ─── Await signals (integration-style) ───────────────────────────────────

    #[tokio::test]
    async fn await_signals_handles_sigterm() {
        let mgr = ShutdownManager::new();
        let mgr_clone = mgr.clone();

        // Spawn the signal handler
        let handle = tokio::spawn(async move {
            mgr_clone.await_signals().await;
        });

        // Give it time to register handlers
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send SIGTERM to our own process
        let pid = std::process::id() as libc::pid_t;
        unsafe {
            libc::kill(pid, libc::SIGTERM);
        }

        // Wait for the handler to process the signal
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(mgr.is_shutting_down(), "SIGTERM should trigger shutdown");

        handle.abort();
    }

    #[tokio::test]
    async fn await_signals_handles_sigint() {
        let mgr = ShutdownManager::new();
        let mgr_clone = mgr.clone();

        let handle = tokio::spawn(async move {
            mgr_clone.await_signals().await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let pid = std::process::id() as libc::pid_t;
        unsafe {
            libc::kill(pid, libc::SIGINT);
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(mgr.is_shutting_down(), "SIGINT should trigger shutdown");

        handle.abort();
    }

    // ─── Thread safety ──────────────────────────────────────────────────────

    #[test]
    fn shutdown_manager_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<ShutdownManager>();
    }

    #[test]
    fn shutdown_manager_is_sync() {
        fn assert_sync<T: Sync>() {}
        assert_sync::<ShutdownManager>();
    }

    // ─── Concurrent initiate ─────────────────────────────────────────────────

    #[tokio::test]
    async fn concurrent_initiate_is_safe() {
        let mgr = Arc::new(ShutdownManager::new());
        let mut handles = Vec::new();

        for _ in 0..10 {
            let m = Arc::clone(&mgr);
            handles.push(tokio::spawn(async move {
                m.initiate();
            }));
        }

        for h in handles {
            h.await.expect("concurrent initiate should not panic");
        }

        assert!(mgr.is_shutting_down());
    }
}
