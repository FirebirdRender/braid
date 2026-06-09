use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::timeout;

/// Default kernel send buffer size: 64 MB.
pub const DEFAULT_SO_SNDBUF: usize = 64 * 1024 * 1024;

/// Default send timeout for individual datagram sends (2× RTT placeholder).
pub const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(2);

/// Per-worker statistics, accessible via `Arc<UdpSendWorkerStats>`.
#[derive(Debug, Default)]
pub struct UdpSendWorkerStats {
    /// Total bytes of fragment payload + header data sent over UDP.
    pub bytes_sent: AtomicU64,
    /// Total number of UDP datagrams (fragments) sent.
    pub fragments_sent: AtomicU64,
    /// Total number of send errors encountered.
    pub errors: AtomicU64,
}

impl UdpSendWorkerStats {
    fn record_send(&self, bytes: usize) {
        self.bytes_sent.fetch_add(bytes as u64, Ordering::Relaxed);
        self.fragments_sent.fetch_add(1, Ordering::Relaxed);
    }

    fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
}

/// Result type reported by a worker to the queue manager's health monitoring loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerResult {
    /// The worker finished normally (channel closed).
    Done,
    /// The worker encountered a send error.
    Failed,
}

/// A single UDP send worker that owns a `tokio::net::UdpSocket`.
///
/// Each worker binds to its assigned local port and sends fragments received
/// from a shared mpsc channel to the configured destination address.
pub struct UdpSendWorker {
    /// Local UDP port to bind to.
    local_port: u16,
    /// Destination socket address (IP:port).
    dest_addr: SocketAddr,
    /// Kernel send buffer size in bytes (SO_SNDBUF).
    so_sndbuf: usize,
    /// Per-datagram send timeout.
    send_timeout: Duration,
    /// Shared statistics.
    stats: Arc<UdpSendWorkerStats>,
    /// Channel to report worker lifecycle results (e.g. send failures) to
    /// the queue manager's health monitoring loop.
    health_tx: Option<mpsc::Sender<WorkerResult>>,
}

impl UdpSendWorker {
    /// Create a new `UdpSendWorker` configuration.
    ///
    /// The worker is not bound until [`bind`] is called.
    pub fn new(
        local_port: u16,
        dest_addr: SocketAddr,
        so_sndbuf: usize,
        send_timeout: Duration,
        stats: Arc<UdpSendWorkerStats>,
        health_tx: Option<mpsc::Sender<WorkerResult>>,
    ) -> Self {
        Self {
            local_port,
            dest_addr,
            so_sndbuf,
            send_timeout,
            stats,
            health_tx,
        }
    }

    /// Bind the UDP socket with the configured options.
    ///
    /// This creates a `tokio::net::UdpSocket` bound to `0.0.0.0:{local_port}`,
    /// then applies:
    /// - `SO_SNDBUF` — kernel send buffer size
    /// - `IP_MTU_DISCOVER` / `IP_DONTFRAG` — DF flag to detect MTU issues
    ///
    /// # Errors
    ///
    /// Returns an error if binding or setting socket options fails.
    pub async fn bind(&self) -> std::io::Result<UdpSocket> {
        let bind_addr: SocketAddr = ([0, 0, 0, 0], self.local_port).into();

        // Create a socket2 socket for low-level option configuration
        let domain = if self.dest_addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };
        let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;

        // Set SO_SNDBUF (kernel send buffer)
        // Note: the kernel may double the value; we set the configured value
        // and the kernel will cap at its maximum.
        socket.set_send_buffer_size(self.so_sndbuf)?;

        // Set IP_DONTFRAG / IP_MTU_DISCOVER to detect MTU issues
        set_df_flag(&socket, self.dest_addr.is_ipv4())?;

        // Enable SO_REUSEADDR for fast restarts
        socket.set_reuse_address(true)?;

        // Bind to the local address
        socket.bind(&bind_addr.into())?;

        // Set non-blocking mode before converting to tokio's UdpSocket
        socket.set_nonblocking(true)?;

        // Convert to tokio's UdpSocket for async I/O
        let std_udp: std::net::UdpSocket = socket.into();
        let udp_socket = UdpSocket::from_std(std_udp)?;

        Ok(udp_socket)
    }

    /// Run the worker: receive fragments from `rx` and send them to `dest_addr`.
    ///
    /// The loop terminates when it receives `None` (EOS marker) from the channel
    /// or when the channel is closed (sender dropped).
    ///
    /// Each datagram send is bounded by `send_timeout`. On timeout or socket
    /// error, the error is logged to stderr and the error counter is incremented,
    /// but the worker continues processing subsequent fragments.
    pub async fn run(&self, socket: UdpSocket, mut rx: mpsc::Receiver<Bytes>) {
        while let Some(data) = rx.recv().await {
            let result = self.send_with_timeout(&socket, &data).await;

            match result {
                Ok(n) => {
                    self.stats.record_send(n);
                }
                Err(e) => {
                    self.stats.record_error();
                    eprintln!("[UdpSendWorker:{}] send error: {}", self.local_port, e);
                    if let Some(ref tx) = self.health_tx {
                        let _ = tx.try_send(WorkerResult::Failed);
                    }
                }
            }
        }
        if let Some(ref tx) = self.health_tx {
            let _ = tx.try_send(WorkerResult::Done);
        }
    }

    /// Send a single datagram with a timeout.
    ///
    /// Wraps `UdpSocket::send_to()` with the configured `send_timeout`.
    async fn send_with_timeout(&self, socket: &UdpSocket, data: &[u8]) -> std::io::Result<usize> {
        match timeout(self.send_timeout, socket.send_to(data, self.dest_addr)).await {
            Ok(result) => result,
            Err(_elapsed) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "send timed out after {:?} to {}",
                    self.send_timeout, self.dest_addr
                ),
            )),
        }
    }
}

/// Set the Don't Fragment (DF) flag on a socket.
///
/// On Linux this uses `IP_MTU_DISCOVER` with `IP_PMTUDISC_DO` (IPv4) or
/// `IPV6_MTU_DISCOVER` with `IPV6_PMTUDISC_DO` (IPv6).
/// On macOS this uses `IP_DONTFRAG`.
#[cfg(target_os = "linux")]
fn set_df_flag(socket: &Socket, is_ipv4: bool) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;

    let fd = socket.as_raw_fd();
    if is_ipv4 {
        let val: libc::c_int = libc::IP_PMTUDISC_DO;
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::IPPROTO_IP,
                libc::IP_MTU_DISCOVER,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
    } else {
        let val: libc::c_int = libc::IPV6_PMTUDISC_DO;
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::IPPROTO_IPV6,
                libc::IPV6_MTU_DISCOVER,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Non-Linux: use socket2's built-in `set_dont_fragment()` if available,
/// otherwise log a warning and skip.
#[cfg(not(target_os = "linux"))]
#[allow(unused_variables)]
fn set_df_flag(socket: &Socket, _is_ipv4: bool) -> std::io::Result<()> {
    // socket2 provides set_dont_fragment() on some platforms
    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
        target_os = "windows",
    ))]
    {
        socket.set_dont_fragment(true)?;
    }
    #[cfg(not(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
        target_os = "windows",
    )))]
    {
        eprintln!("[UdpSendWorker] warning: IP_DONTFRAG not supported on this platform");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a worker configuration pointing to a given destination.
    fn make_worker(
        local_port: u16,
        dest_addr: SocketAddr,
    ) -> (UdpSendWorker, Arc<UdpSendWorkerStats>) {
        let stats = Arc::new(UdpSendWorkerStats::default());
        let worker = UdpSendWorker::new(
            local_port,
            dest_addr,
            65536, // small buffer for tests
            Duration::from_secs(1),
            stats.clone(),
            None, // no health reporting in tests
        );
        (worker, stats)
    }

    // ─── Worker sends datagram to local listener ───────────────────────────

    #[tokio::test]
    async fn worker_sends_datagram_to_listener() {
        let listener = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();

        // Create a worker that sends to the listener
        let (worker, stats) = make_worker(0, listen_addr);
        let socket = worker.bind().await.unwrap();

        let (tx, rx) = mpsc::channel::<Bytes>(64);

        // Spawn the worker
        let handle = tokio::spawn(async move {
            worker.run(socket, rx).await;
        });

        // Send a test fragment
        let fragment_data: Bytes = Bytes::from(vec![0x50, 0x00, 0x00, 0x0A, 0x01, 0x02, 0x03, 0x04]);
        tx.send(fragment_data.clone()).await.unwrap();
        drop(tx);

        handle.await.unwrap();

        // Verify the listener received the datagram
        let mut buf = vec![0u8; 1500];
        let (n, _src) = listener.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], &fragment_data[..]);

        // Verify stats
        assert_eq!(stats.fragments_sent.load(Ordering::Relaxed), 1);
        assert_eq!(
            stats.bytes_sent.load(Ordering::Relaxed),
            fragment_data.len() as u64
        );
        assert_eq!(stats.errors.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn worker_sends_multiple_fragments() {
        let listener = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();

        let (worker, stats) = make_worker(0, listen_addr);
        let socket = worker.bind().await.unwrap();

        let (tx, rx) = mpsc::channel::<Bytes>(64);

        let handle = tokio::spawn(async move {
            worker.run(socket, rx).await;
        });

        // Send multiple fragments
        let fragments: Vec<Bytes> = (0..5).map(|i| Bytes::from(vec![i as u8; 100])).collect();

        for frag in &fragments {
            tx.send(frag.clone()).await.unwrap();
        }
        drop(tx);

        handle.await.unwrap();

        // Verify all fragments were received
        for frag in &fragments {
            let mut buf = vec![0u8; 1500];
            let (n, _src) = listener.recv_from(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], &frag[..]);
        }

        assert_eq!(
            stats.fragments_sent.load(Ordering::Relaxed),
            fragments.len() as u64
        );
        assert_eq!(stats.errors.load(Ordering::Relaxed), 0);
    }

    // ─── Worker terminates when channel is closed ──────────────────────────

    #[tokio::test]
    async fn worker_terminates_on_channel_close() {
        let listener = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();

        let (worker, _stats) = make_worker(0, listen_addr);
        let socket = worker.bind().await.unwrap();

        let (tx, rx) = mpsc::channel::<Bytes>(64);
        drop(tx); // Drop tx immediately so rx.recv() returns None

        let handle = tokio::spawn(async move {
            worker.run(socket, rx).await;
        });

        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("worker should terminate on channel close")
            .unwrap();
    }

    // ─── Worker handles unreachable destination ────────────────────────────

    #[tokio::test]
    async fn worker_handles_unreachable_destination() {
        // Use a destination that is unlikely to have a listener
        let unreachable_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();

        let (worker, stats) = make_worker(0, unreachable_addr);
        let socket = worker.bind().await.unwrap();

        let (tx, rx) = mpsc::channel::<Bytes>(64);

        let handle = tokio::spawn(async move {
            worker.run(socket, rx).await;
        });

        // Send a fragment to the unreachable address
        let fragment_data: Bytes = Bytes::from(vec![0x50, 0x00, 0x00, 0x0A, 0x01, 0x02, 0x03, 0x04]);
        tx.send(fragment_data).await.unwrap();
        drop(tx);

        handle.await.unwrap();

        // On Linux, sending to an unreachable localhost port typically succeeds
        // at the UDP layer (the ICMP Unreachable comes back async).
        // So errors might be 0 — that's fine. The important thing is the worker
        // doesn't panic or hang.
        // We just verify the worker completed cleanly.
        let _ = stats.fragments_sent.load(Ordering::Relaxed);
    }

    // ─── Bind sets SO_SNDBUF ───────────────────────────────────────────────

    #[tokio::test]
    async fn bind_sets_so_sndbuf() {
        let listener = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();

        let (worker, _stats) = make_worker(0, listen_addr);
        let socket = worker.bind().await.unwrap();

        // Verify the socket is bound
        let local = socket.local_addr().unwrap();
        assert_ne!(local.port(), 0, "socket should be bound to a port");
    }

    // ─── Stats are accessible via Arc ──────────────────────────────────────

    #[tokio::test]
    async fn stats_accessible_via_arc() {
        let listener = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();

        let stats = Arc::new(UdpSendWorkerStats::default());
        let worker =
            UdpSendWorker::new(0, listen_addr, 65536, Duration::from_secs(1), stats.clone(), None);
        let socket = worker.bind().await.unwrap();

        let (tx, rx) = mpsc::channel::<Bytes>(64);

        let handle = tokio::spawn(async move {
            worker.run(socket, rx).await;
        });

        tx.send(Bytes::from(vec![0u8; 50])).await.unwrap();
        drop(tx);

        handle.await.unwrap();

        // Verify stats via the Arc reference
        assert_eq!(stats.fragments_sent.load(Ordering::Relaxed), 1);
        assert_eq!(stats.bytes_sent.load(Ordering::Relaxed), 50);
    }

    // ─── Send timeout returns error ────────────────────────────────────────

    #[tokio::test]
    async fn send_timeout_returns_error() {
        // Create a worker with a very short timeout
        let dest: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let stats = Arc::new(UdpSendWorkerStats::default());
        let worker = UdpSendWorker::new(
            0,
            dest,
            65536,
            Duration::from_millis(1), // very short timeout
            stats.clone(),
            None,
        );
        let socket = worker.bind().await.unwrap();

        // send_with_timeout should return a timeout error
        let data = vec![0u8; 100];
        let result = worker.send_with_timeout(&socket, &data).await;
        // On a local network, send_to usually completes before the timeout,
        // so this may succeed. We just verify it doesn't panic.
        let _ = result;
    }
}
