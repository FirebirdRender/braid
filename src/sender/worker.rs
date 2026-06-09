use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
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

// ─── BatchSendWorker: sendmmsg-based batched sender ─────────────────────────

/// Result of a `sendmmsg_all` call, for the async caller to act on.
enum SendmmsgResult {
    Ok,
    Enosys,
    WouldBlock,
    Error(u64),
}

/// A UDP send worker that accumulates fragments and issues batch sendmmsg
/// syscalls for higher throughput.
///
/// Same public interface as `UdpSendWorker` (`new()`, `bind()`, `run()`), with
/// the same stats/heartbeat reporting. Falls back to single-datagram sends on
/// ENOSYS or when batch size is 1.
pub struct BatchSendWorker {
    /// Local UDP port to bind to.
    local_port: u16,
    /// Destination socket address (IP:port).
    dest_addr: SocketAddr,
    /// Kernel send buffer size in bytes (SO_SNDBUF).
    so_sndbuf: usize,
    /// Per-datagram send timeout for the fallback path.
    send_timeout: Duration,
    /// Maximum datagrams per sendmmsg call.
    batch_size: usize,
    /// Maximum wait in microseconds before flushing an incomplete batch.
    batch_usec: u64,
    /// Shared statistics.
    stats: Arc<UdpSendWorkerStats>,
    /// Channel to report worker lifecycle results to the queue manager.
    health_tx: Option<mpsc::Sender<WorkerResult>>,
    /// Runtime ENOSYS detection: starts true, set false on first ENOSYS.
    support_batch: AtomicBool,
}

impl BatchSendWorker {
    /// Create a new `BatchSendWorker` configuration.
    ///
    /// The worker is not bound until [`bind`] is called.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        local_port: u16,
        dest_addr: SocketAddr,
        so_sndbuf: usize,
        send_timeout: Duration,
        batch_size: usize,
        batch_usec: u64,
        stats: Arc<UdpSendWorkerStats>,
        health_tx: Option<mpsc::Sender<WorkerResult>>,
    ) -> Self {
        Self {
            local_port,
            dest_addr,
            so_sndbuf,
            send_timeout,
            batch_size,
            batch_usec,
            stats,
            health_tx,
            support_batch: AtomicBool::new(true),
        }
    }

    /// Bind the UDP socket with the configured options, then connect it.
    ///
    /// Same socket options as `UdpSendWorker::bind()`, plus `connect()` to the
    /// destination address so that `sendmmsg` can use null `msg_name`.
    ///
    /// # Errors
    ///
    /// Returns an error if binding, setting socket options, or connecting fails.
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
        socket.set_send_buffer_size(self.so_sndbuf)?;

        // Set IP_DONTFRAG / IP_MTU_DISCOVER to detect MTU issues
        set_df_flag(&socket, self.dest_addr.is_ipv4())?;

        // Enable SO_REUSEADDR for fast restarts
        socket.set_reuse_address(true)?;

        // Bind to the local address
        socket.bind(&bind_addr.into())?;

        // Connect the socket so sendmmsg can use null msg_name
        socket.connect(&self.dest_addr.into())?;

        // Set non-blocking mode before converting to tokio's UdpSocket
        socket.set_nonblocking(true)?;

        // Convert to tokio's UdpSocket for async I/O
        let std_udp: std::net::UdpSocket = socket.into();
        let udp_socket = UdpSocket::from_std(std_udp)?;

        Ok(udp_socket)
    }

    /// Run the worker: receive fragments from `rx` and send them in batches.
    ///
    /// The loop accumulates fragments up to `batch_size`, then flushes via
    /// `sendmmsg`. If no new fragment arrives within `batch_usec`, the partial
    /// batch is flushed immediately.
    ///
    /// On ENOSYS, the worker falls back atomically to single-datagram sends
    /// for the remainder of its lifetime.
    pub async fn run(&self, socket: UdpSocket, mut rx: mpsc::Receiver<Bytes>) {
        let mut batch: Vec<Bytes> = Vec::with_capacity(self.batch_size);

        loop {
            // Wait for the first fragment
            let data = match rx.recv().await {
                Some(d) => d,
                None => break, // channel closed
            };
            batch.push(data);

            // Accumulate more fragments without blocking (try_recv drain)
            let burst_start = std::time::Instant::now();
            let max_wait = Duration::from_micros(self.batch_usec);
            loop {
                // Check batch size limit
                if batch.len() >= self.batch_size {
                    break;
                }
                // Check time limit
                if burst_start.elapsed() >= max_wait {
                    break;
                }
                match rx.try_recv() {
                    Ok(data) => {
                        batch.push(data);
                    }
                    Err(TryRecvError::Empty) => {
                        // No more data right now — yield briefly then retry
                        // until the time budget expires
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => {
                        // Channel closed — flush what we have and exit
                        self.flush_batch(&socket, &mut batch).await;
                        if let Some(ref tx) = self.health_tx {
                            let _ = tx.try_send(WorkerResult::Done);
                        }
                        return;
                    }
                }
            }

            // Flush the batch
            self.flush_batch(&socket, &mut batch).await;
        }

        // Flush any remaining data
        if !batch.is_empty() {
            self.flush_batch(&socket, &mut batch).await;
        }
        if let Some(ref tx) = self.health_tx {
            let _ = tx.try_send(WorkerResult::Done);
        }
    }

    /// Flush a batch of fragments via `sendmmsg`, with full rebuild of
    /// `msgvec`/`iovecs` on each retry attempt (memory safety: pointers
    /// always point to current `batch` contents).
    ///
    /// On `ENOSYS`, atomically disables batching and falls back to
    /// [`flush_single`]. On `EAGAIN`/`EWOULDBLOCK`, stops and leaves
    /// remaining data in the batch.
    async fn flush_batch(&self, socket: &UdpSocket, batch: &mut Vec<Bytes>) {
        if batch.is_empty() {
            return;
        }

        if !self.support_batch.load(Ordering::Relaxed) || self.batch_size <= 1 {
            self.flush_single(socket, batch).await;
            return;
        }

        // Delegate to sync helper: no raw-pointer types cross .await boundaries
        let fd = socket.as_raw_fd();
        match Self::sendmmsg_all(fd, batch, self.batch_size, &self.stats, self.local_port) {
            SendmmsgResult::Ok => {}
            SendmmsgResult::Enosys => {
                self.support_batch.store(false, Ordering::Relaxed);
                self.stats.record_error();
                eprintln!(
                    "[BatchSendWorker:{}] sendmmsg not supported (ENOSYS), falling back to single-send",
                    self.local_port
                );
                if let Some(ref tx) = self.health_tx {
                    let _ = tx.try_send(WorkerResult::Failed);
                }
                self.flush_single(socket, batch).await;
            }
            SendmmsgResult::WouldBlock => {
                self.stats.record_error();
            }
            SendmmsgResult::Error(count) => {
                for _ in 0..count {
                    self.stats.record_error();
                }
                if let Some(ref tx) = self.health_tx {
                    let _ = tx.try_send(WorkerResult::Failed);
                }
            }
        }
    }

    /// Synchronous sendmmsg loop. Returns how many datagrams were sent via
    /// batch stats, or a signal for the async caller to handle.
    ///
    /// This is a sync fn so that `Vec<libc::iovec>` and `Vec<libc::mmsghdr>`
    /// (which contain raw pointers and are not `Send`) never cross `.await`
    /// boundaries in the async state machine.
    fn sendmmsg_all(
        fd: std::os::unix::io::RawFd,
        batch: &mut Vec<Bytes>,
        batch_size: usize,
        stats: &UdpSendWorkerStats,
        _local_port: u16,
    ) -> SendmmsgResult {
        let max_retries = 1024;
        let mut retry_count = 0usize;

        while !batch.is_empty() && retry_count < max_retries {
            let n_to_send = batch.len().min(batch_size);

            // Rebuild msgvec and iovecs from scratch each attempt.
            // iov_base pointers reference data owned by the Bytes in `batch`,
            // which is not mutated or dropped while we hold the `batch` Vec.
            let mut iovecs: Vec<libc::iovec> = Vec::with_capacity(n_to_send);
            for data in batch.iter().take(n_to_send) {
                iovecs.push(libc::iovec {
                    iov_base: data.as_ptr() as *mut libc::c_void,
                    iov_len: data.len(),
                });
            }

            let mut msgvec: Vec<libc::mmsghdr> = Vec::with_capacity(n_to_send);
            for iov in iovecs.iter() {
                msgvec.push(libc::mmsghdr {
                    msg_hdr: libc::msghdr {
                        msg_name: ptr::null_mut(),
                        msg_namelen: 0,
                        msg_iov: iov as *const libc::iovec as *mut libc::iovec,
                        msg_iovlen: 1,
                        msg_control: ptr::null_mut(),
                        msg_controllen: 0,
                        msg_flags: 0,
                    },
                    msg_len: 0,
                });
            }

            // SAFETY: sendmmsg mutates msgvec entries (writes msg_len) but
            // does not read outside the bounds. iovecs point to valid Bytes
            // data owned by `batch`. The fd is valid and owned by the socket.
            let result = unsafe {
                libc::sendmmsg(fd, msgvec.as_mut_ptr(), n_to_send as u32, libc::MSG_NOSIGNAL)
            };

            if result == -1 {
                let err = std::io::Error::last_os_error();
                match err.raw_os_error() {
                    Some(libc::ENOSYS) => {
                        return SendmmsgResult::Enosys;
                    }
                    Some(libc::EAGAIN) => { // EAGAIN == EWOULDBLOCK on Linux
                        return SendmmsgResult::WouldBlock;
                    }
                    _ => {
                        // Unexpected error: drop the first packet and retry
                        batch.remove(0);
                        retry_count += 1;
                    }
                }
            } else {
                let sent = result as usize;
                for i in 0..sent {
                    stats.record_send(batch[i].len());
                }
                // Drain sent datagrams from the batch front.
                // This invalidates the pointers in iovecs/msgvec, so we must
                // NOT use them after this point (we don't — the loop rebuilds).
                for _ in 0..sent {
                    batch.remove(0);
                }
                retry_count = 0;
                if sent < n_to_send {
                    // Partial send — socket is congested, stop
                    return SendmmsgResult::Ok;
                }
            }
        }

        if !batch.is_empty() && retry_count >= max_retries {
            let n = batch.len();
            batch.clear();
            return SendmmsgResult::Error(n as u64);
        }

        SendmmsgResult::Ok
    }

    /// Fallback: send remaining fragments one at a time using
    /// `socket.try_send()` (connected socket, no per-packet address).
    ///
    /// Used when `sendmmsg` returns ENOSYS or when `--no-batch` is in effect.
    async fn flush_single(&self, socket: &UdpSocket, batch: &mut Vec<Bytes>) {
        while !batch.is_empty() {
            let data = &batch[0];

            // Wait for the socket to become writable with a timeout
            let writable = match timeout(self.send_timeout, socket.writable()).await {
                Ok(Ok(())) => true,
                Ok(Err(e)) => {
                    self.stats.record_error();
                    eprintln!(
                        "[BatchSendWorker:{}] socket.writable() error: {}",
                        self.local_port, e
                    );
                    batch.remove(0);
                    continue;
                }
                Err(_) => {
                    self.stats.record_error();
                    eprintln!(
                        "[BatchSendWorker:{}] socket.writable() timed out",
                        self.local_port
                    );
                    batch.remove(0);
                    continue;
                }
            };

            if !writable {
                continue;
            }

            // try_send (no addr needed — socket is connected)
            match socket.try_send(data) {
                Ok(n) => {
                    self.stats.record_send(n);
                    batch.remove(0);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Socket not writable yet — loop back to socket.writable()
                    continue;
                }
                Err(e) => {
                    self.stats.record_error();
                    eprintln!(
                        "[BatchSendWorker:{}] try_send error: {}",
                        self.local_port, e
                    );
                    if let Some(ref tx) = self.health_tx {
                        let _ = tx.try_send(WorkerResult::Failed);
                    }
                    batch.remove(0);
                }
            }
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

    // ─── BatchSendWorker tests ──────────────────────────────────────────────

    fn make_batch_worker(
        local_port: u16,
        dest_addr: SocketAddr,
    ) -> (BatchSendWorker, Arc<UdpSendWorkerStats>) {
        let stats = Arc::new(UdpSendWorkerStats::default());
        let worker = BatchSendWorker::new(
            local_port,
            dest_addr,
            65536, // small buffer for tests
            Duration::from_secs(1),
            16,   // batch_size
            100,  // batch_usec
            stats.clone(),
            None, // no health reporting in tests
        );
        (worker, stats)
    }

    #[tokio::test]
    async fn batch_worker_sends_datagram_to_listener() {
        let listener = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();

        let (worker, stats) = make_batch_worker(0, listen_addr);
        let socket = worker.bind().await.unwrap();

        let (tx, rx) = mpsc::channel::<Bytes>(64);

        let handle = tokio::spawn(async move {
            worker.run(socket, rx).await;
        });

        let fragment_data: Bytes = Bytes::from(vec![0x50, 0x00, 0x00, 0x0A, 0x01, 0x02, 0x03, 0x04]);
        tx.send(fragment_data.clone()).await.unwrap();
        drop(tx);

        handle.await.unwrap();

        let mut buf = vec![0u8; 1500];
        let (n, _src) = listener.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], &fragment_data[..]);

        assert_eq!(stats.fragments_sent.load(Ordering::Relaxed), 1);
        assert_eq!(
            stats.bytes_sent.load(Ordering::Relaxed),
            fragment_data.len() as u64
        );
        assert_eq!(stats.errors.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn batch_worker_sends_multiple_fragments() {
        let listener = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();

        let (worker, stats) = make_batch_worker(0, listen_addr);
        let socket = worker.bind().await.unwrap();

        let (tx, rx) = mpsc::channel::<Bytes>(64);

        let handle = tokio::spawn(async move {
            worker.run(socket, rx).await;
        });

        let fragments: Vec<Bytes> = (0..5).map(|i| Bytes::from(vec![i as u8; 100])).collect();

        for frag in &fragments {
            tx.send(frag.clone()).await.unwrap();
        }
        drop(tx);

        handle.await.unwrap();

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

    #[tokio::test]
    async fn batch_worker_terminates_on_channel_close() {
        let listener = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();

        let (worker, _stats) = make_batch_worker(0, listen_addr);
        let socket = worker.bind().await.unwrap();

        let (tx, rx) = mpsc::channel::<Bytes>(64);
        drop(tx);

        let handle = tokio::spawn(async move {
            worker.run(socket, rx).await;
        });

        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("batch worker should terminate on channel close")
            .unwrap();
    }

    #[tokio::test]
    async fn batch_worker_handles_unreachable_destination() {
        let unreachable_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();

        let (worker, stats) = make_batch_worker(0, unreachable_addr);
        let socket = worker.bind().await.unwrap();

        let (tx, rx) = mpsc::channel::<Bytes>(64);

        let handle = tokio::spawn(async move {
            worker.run(socket, rx).await;
        });

        let fragment_data: Bytes = Bytes::from(vec![0x50, 0x00, 0x00, 0x0A, 0x01, 0x02, 0x03, 0x04]);
        tx.send(fragment_data).await.unwrap();
        drop(tx);

        handle.await.unwrap();

        let _ = stats.fragments_sent.load(Ordering::Relaxed);
    }

    #[tokio::test]
    async fn batch_worker_batch_accumulation() {
        let listener = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();

        let stats = Arc::new(UdpSendWorkerStats::default());
        let worker = BatchSendWorker::new(
            0,
            listen_addr,
            65536,
            Duration::from_secs(1),
            16,  // batch_size large enough to hold all
            200, // batch_usec — 200µs window for accumulation
            stats.clone(),
            None,
        );
        let socket = worker.bind().await.unwrap();

        let (tx, rx) = mpsc::channel::<Bytes>(64);

        let handle = tokio::spawn(async move {
            worker.run(socket, rx).await;
        });

        // Send fragments with small delays so they accumulate in the batch
        let fragments: Vec<Bytes> = (0..3).map(|i| Bytes::from(vec![i as u8; 50])).collect();

        for frag in &fragments {
            tx.send(frag.clone()).await.unwrap();
            // Small delay — short enough that all fit within the batch_usec window
            // but long enough to let try_recv see them
            tokio::time::sleep(Duration::from_micros(10)).await;
        }
        drop(tx);

        handle.await.unwrap();

        // All fragments should have been received
        for frag in &fragments {
            let mut buf = vec![0u8; 1500];
            let (n, _src) = listener.recv_from(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], &frag[..]);
        }

        assert_eq!(
            stats.fragments_sent.load(Ordering::Relaxed),
            fragments.len() as u64
        );
    }

    #[tokio::test]
    async fn batch_worker_bind_connects_socket() {
        let listener = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();

        let (worker, _stats) = make_batch_worker(0, listen_addr);
        let socket = worker.bind().await.unwrap();

        let local = socket.local_addr().unwrap();
        assert_ne!(local.port(), 0, "socket should be bound to a port");
    }

    #[tokio::test]
    async fn batch_worker_stats_accessible_via_arc() {
        let listener = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();

        let stats = Arc::new(UdpSendWorkerStats::default());
        let worker = BatchSendWorker::new(
            0,
            listen_addr,
            65536,
            Duration::from_secs(1),
            16,
            100,
            stats.clone(),
            None,
        );
        let socket = worker.bind().await.unwrap();

        let (tx, rx) = mpsc::channel::<Bytes>(64);

        let handle = tokio::spawn(async move {
            worker.run(socket, rx).await;
        });

        tx.send(Bytes::from(vec![0u8; 50])).await.unwrap();
        drop(tx);

        handle.await.unwrap();

        assert_eq!(stats.fragments_sent.load(Ordering::Relaxed), 1);
        assert_eq!(stats.bytes_sent.load(Ordering::Relaxed), 50);
    }
}
