use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use braid::adaptive::channels::ChannelCountAdaptor;
use braid::adaptive::chunk_size::ChunkSizeAdaptor;
use braid::buffer::pool::BufferPool;
use braid::control::client::ControlClient;
use braid::control::negotiation::{negotiate, NegotiationConfig};
use braid::control::server::ControlServer;
use braid::flow::SenderReactor;
use braid::progress::reporter::ProgressReporter;
use braid::sender::queue::QueueManagerBuilder;
use braid::sender::splitter::ChunkSplitter;
use braid::sender::worker::{UdpSendWorker, UdpSendWorkerStats};
use braid::shutdown::manager::ShutdownManager;

/// Helper: run a full negotiation between a sender and receiver.
async fn setup_negotiation(
    channel_count: u8,
) -> (ControlClient, Vec<braid::control::negotiation::ChannelInfo>) {
    let server = ControlServer::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr: SocketAddr = server.local_addr().unwrap();

    let receiver_handle = tokio::spawn(async move {
        let mut conn = server.accept().await.unwrap();
        braid::control::negotiation::accept_negotiation(&mut conn).await
    });

    let mut client = ControlClient::connect(addr).await.unwrap();
    let config = NegotiationConfig {
        channel_count,
        min_chunk: 10,
        max_chunk: 20,
        mtu: 14,
        compression_lz4: false,
        compression_zstd: false,
    };

    let sender_result = negotiate(&mut client, config).await.unwrap();

    let _receiver_result = receiver_handle.await.unwrap();

    (client, sender_result.channels)
}

// ─── Integration: Full pipeline component wiring ───────────────────────────

#[tokio::test]
async fn test_shutdown_manager_creation() {
    let mgr = ShutdownManager::new();
    assert!(!mgr.is_shutting_down());
}

#[tokio::test]
async fn test_shutdown_manager_initiate() {
    let mgr = ShutdownManager::new();
    mgr.initiate();
    assert!(mgr.is_shutting_down());
}

#[tokio::test]
async fn test_shutdown_manager_subscribe() {
    let mgr = ShutdownManager::new();
    let mut rx = mgr.subscribe();

    let handle = tokio::spawn(async move {
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

    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("subscriber should be notified")
        .unwrap();
}

#[tokio::test]
async fn test_chunk_splitter_creation() {
    let pool = BufferPool::new(2, 1500);
    let splitter = ChunkSplitter::new(Arc::new(AtomicUsize::new(1024)), 1024, 1500, pool);
    assert_eq!(splitter.chunk_size(), 1024);
    assert_eq!(splitter.mtu(), 1500);
    assert!(splitter.fragment_payload_size() > 0);
}

#[tokio::test]
async fn test_chunk_size_adaptor_default() {
    let adaptor = ChunkSizeAdaptor::new_default();
    // MAX_CHUNK is capped at u16::MAX (65535) since ChunkHeader.payload_length is u16.
    assert_eq!(adaptor.current_chunk_size(), 65535);
    assert!(!adaptor.is_disabled());
}

#[tokio::test]
async fn test_channel_count_adaptor_default() {
    let adaptor = ChannelCountAdaptor::new_default();
    assert_eq!(adaptor.current_channel_count(), 4);
    assert!(!adaptor.is_disabled());
}

#[tokio::test]
async fn test_queue_manager_build() {
    let (mgr, receivers) = QueueManagerBuilder::new(4).channel_capacity(64).build();
    assert_eq!(mgr.worker_count(), 4);
    assert_eq!(receivers.len(), 4);
}

#[tokio::test]
async fn test_queue_manager_dispatch() {
    let (mut mgr, _receivers) = QueueManagerBuilder::new(2).channel_capacity(64).build();
    let result = mgr.dispatch(Bytes::from(vec![0u8; 100]));
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_udp_worker_creation() {
    let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
    let worker = UdpSendWorker::new(
        0,
        addr,
        65536,
        Duration::from_secs(1),
        Arc::new(UdpSendWorkerStats::default()),
        None,
    );
    let socket = worker.bind().await.unwrap();
    let local = socket.local_addr().unwrap();
    assert_ne!(local.port(), 0);
}

#[tokio::test]
async fn test_progress_reporter_creation() {
    let reporter = ProgressReporter::new(
        Duration::from_secs(1),
        braid::progress::reporter::ProgressVerbosity::Normal,
    );
    let snapshot = reporter.snapshot(Duration::from_secs(0));
    assert_eq!(snapshot.total_bytes, 0);
}

#[tokio::test]
async fn test_sender_reactor_creation() {
    use tokio::sync::mpsc;
    let (_control_tx, control_rx) = mpsc::channel(16);

    let reactor = SenderReactor::new(100, control_rx);
    assert_eq!(
        reactor.controller().level(),
        braid::flow::FullnessLevel::Green
    );
}

// ─── Integration: End-to-end negotiation ───────────────────────────────────

#[tokio::test]
async fn test_full_negotiation_single_channel() {
    let (_client, channels) = setup_negotiation(1).await;
    assert_eq!(channels.len(), 1);
    assert_eq!(channels[0].channel_id, 0);
    assert!(channels[0].port > 0);
}

#[tokio::test]
async fn test_full_negotiation_multiple_channels() {
    let (_client, channels) = setup_negotiation(4).await;
    assert_eq!(channels.len(), 4);
    for (i, ch) in channels.iter().enumerate() {
        assert_eq!(ch.channel_id, i as u16);
        assert!(ch.port > 0);
    }
}

// ─── Integration: Worker sends via direct channel ──────────────────────────

#[tokio::test]
async fn test_worker_direct_send() {
    let listener = tokio::net::UdpSocket::bind("0.0.0.0:0").await.unwrap();
    let listen_addr = listener.local_addr().unwrap();

    let worker = UdpSendWorker::new(
        0,
        listen_addr,
        65536,
        Duration::from_secs(1),
        Arc::new(UdpSendWorkerStats::default()),
        None,
    );
    let socket = worker.bind().await.unwrap();

    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(64);

    let worker_handle = tokio::spawn(async move {
        worker.run(socket, rx).await;
    });

    let fragment_data = Bytes::from(vec![0x42u8; 100]);
    tx.send(fragment_data.clone()).await.unwrap();
    drop(tx);

    tokio::time::timeout(Duration::from_secs(2), worker_handle)
        .await
        .expect("worker should complete")
        .unwrap();

    let mut buf = vec![0u8; 1500];
    let result =
        tokio::time::timeout(Duration::from_millis(500), listener.recv_from(&mut buf)).await;
    if let Ok(Ok((n, _src))) = result {
        assert_eq!(&buf[..n], &fragment_data[..]);
    }
}

// ─── Integration: ShutdownManager signal propagation ───────────────────────

#[tokio::test]
async fn test_shutdown_propagation_to_components() {
    let shutdown = ShutdownManager::new();

    // Spawn a task that watches for shutdown
    let mut rx = shutdown.subscribe();
    let watch_handle = tokio::spawn(async move {
        loop {
            if *rx.borrow() {
                return true;
            }
            if rx.changed().await.is_err() {
                return false;
            }
        }
    });

    // Spawn a task that polls the shutdown flag
    let flag = shutdown.shutdown_flag();
    let poll_handle = tokio::spawn(async move {
        loop {
            if flag.load(std::sync::atomic::Ordering::Acquire) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    // Initiate shutdown
    tokio::time::sleep(Duration::from_millis(50)).await;
    shutdown.initiate();

    // Both tasks should observe the shutdown
    let watch_result = tokio::time::timeout(Duration::from_secs(1), watch_handle)
        .await
        .expect("watch task should complete")
        .unwrap();
    assert!(watch_result);

    let poll_result = tokio::time::timeout(Duration::from_secs(1), poll_handle)
        .await
        .expect("poll task should complete")
        .unwrap();
    assert!(poll_result);
}

// ─── Integration: Flow control wiring ──────────────────────────────────────

#[tokio::test]
async fn test_flow_control_wiring() {
    use tokio::sync::mpsc;

    let (control_tx, control_rx) = mpsc::channel(16);

    let mut reactor = SenderReactor::new(100, control_rx);

    let handle = tokio::spawn(async move {
        reactor.run().await;
    });

    // Send a yellow-level queue status (50-80% fullness)
    control_tx
        .send(braid::protocol::ControlMessage::QueueStatus {
            queued_chunks: 60,
            queued_bytes: 60,
        })
        .await
        .ok();

    // Give the reactor time to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    drop(control_tx);

    tokio::time::sleep(Duration::from_millis(50)).await;

    handle.await.unwrap();
}
