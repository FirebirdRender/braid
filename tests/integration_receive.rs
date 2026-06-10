use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use braid::buffer::pool::BufferPool;
use braid::flow::ReceiverMonitor;
use braid::progress::reporter::{ProgressReporter, ProgressVerbosity};
use braid::protocol::ControlMessage;
use braid::receiver::commit::{CommitGate, CommitGateInput};
use braid::receiver::ordering::ChunkOrderer;
use braid::receiver::reassembly::FragmentReassembler;
use braid::shutdown::manager::ShutdownManager;

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
async fn test_buffer_pool_creation() {
    let pool = BufferPool::new(10, 1024);
    let guard = pool.acquire().await;

    assert_eq!(guard.buffer.len(), 1024);
}

#[tokio::test]
async fn test_fragment_reassembler_creation() {
    let (tx, _rx) = tokio::sync::mpsc::channel::<Bytes>(16);
    let reassembler = FragmentReassembler::new(tx, 1024 * 1024, 60, BufferPool::new(4, 65536), Arc::new(AtomicUsize::new(0)));
    assert_eq!(reassembler.in_flight_count(), 0);
    assert_eq!(reassembler.inflight_bytes(), 0);
}

#[tokio::test]
async fn test_chunk_orderer_creation() {
    let (tx, _rx) = tokio::sync::mpsc::channel::<CommitGateInput>(16);
    let orderer = ChunkOrderer::new(tx, 0);
    let stats = orderer.stats();
    assert_eq!(stats.chunks_received, 0);
    assert_eq!(stats.chunks_emitted, 0);
    assert_eq!(stats.chunks_waiting, 0);
}

#[tokio::test]
async fn test_commit_gate_creation() {
    let (_tx, rx) = tokio::sync::mpsc::channel::<CommitGateInput>(16);
    let gate = CommitGate::new(rx);
    let stats = gate.stats();
    assert_eq!(stats.chunks_committed, 0);
    assert_eq!(stats.bytes_written, 0);
}

#[tokio::test]
async fn test_progress_reporter_creation() {
    let reporter = ProgressReporter::new(Duration::from_secs(1), ProgressVerbosity::Normal);
    let snapshot = reporter.snapshot(Duration::from_secs(0));
    assert_eq!(snapshot.total_bytes, 0);
}

#[tokio::test]
async fn test_receiver_monitor_creation() {
    let (control_tx, _control_rx) = tokio::sync::mpsc::channel(16);
    let monitor = ReceiverMonitor::new(10, control_tx, Duration::from_secs(1), Arc::new(AtomicUsize::new(0)));
    assert_eq!(
        monitor.controller().level(),
        braid::flow::FullnessLevel::Green
    );
}

#[tokio::test]
async fn test_reassembly_to_orderer_pipeline() {
    let (reassembly_tx, mut reassembly_rx) = tokio::sync::mpsc::channel::<Bytes>(16);
    let (orderer_tx, mut orderer_rx) = tokio::sync::mpsc::channel::<CommitGateInput>(16);

    let receiver_bytes = Arc::new(AtomicUsize::new(0));
    let mut reassembler = FragmentReassembler::new(reassembly_tx, 1024 * 1024, 60, BufferPool::new(4, 65536), receiver_bytes);
    let mut orderer = ChunkOrderer::new(orderer_tx, 0);

    use braid::protocol::crc::compute_chunk_crc;
    use braid::protocol::headers::{ChunkHeader, FragmentHeader};
    use bytes::BytesMut;
    let data: &[u8] = b"hello pipeline";
    let chunk_crc = compute_chunk_crc(0, data);
    let chunk_header = ChunkHeader::new(0, data.len() as u16, 0, chunk_crc);

    let mut chunk_buf = BytesMut::with_capacity(ChunkHeader::LEN + data.len());
    chunk_buf.extend_from_slice(&chunk_header.to_bytes());
    chunk_buf.extend_from_slice(data);

    use braid::protocol::crc::compute_fragment_crc;
    let fragment_crc = compute_fragment_crc(&chunk_buf);
    let frag_header = FragmentHeader {
        chunk_id: 0,
        fragment_index: 0,
        total_fragments: 1,
        fragment_length: chunk_buf.len() as u16,
        fragment_crc,
    };
    let mut fragment = Vec::with_capacity(FragmentHeader::LEN + chunk_buf.len());
    fragment.extend_from_slice(&frag_header.to_bytes());
    fragment.extend_from_slice(&chunk_buf);

    let completed = reassembler.add_fragment(fragment.into()).await.unwrap();
    assert!(
        completed,
        "single-fragment chunk should complete immediately"
    );

    let reassembled = reassembly_rx.recv().await.unwrap();
    orderer.push_chunk(reassembled);

    let ordered = orderer_rx.recv().await.unwrap();
    assert_eq!(ordered.data, data);
    assert_eq!(ordered.sequence_number, 0);
}

#[tokio::test]
async fn test_orderer_to_commit_gate_pipeline() {
    let (orderer_tx, orderer_rx) = tokio::sync::mpsc::channel::<CommitGateInput>(16);
    let mut gate = CommitGate::new(orderer_rx);

    let gate_handle = tokio::spawn(async move {
        gate.run().await;
        gate.stats()
    });

    use braid::protocol::crc::compute_chunk_crc;
    let data = b"ordered data";
    let crc = compute_chunk_crc(42, data);
    orderer_tx
        .send(CommitGateInput {
            data: Bytes::copy_from_slice(b"ordered data"),
            sequence_number: 42,
            chunk_crc: crc,
        })
        .await
        .unwrap();

    drop(orderer_tx);

    let stats = tokio::time::timeout(Duration::from_secs(5), gate_handle)
        .await
        .expect("gate should complete")
        .expect("gate should not panic");

    assert_eq!(stats.chunks_committed, 1);
    assert_eq!(stats.bytes_written, data.len() as u64);
    assert_eq!(stats.crc_failures, 0);
}

#[tokio::test]
async fn test_full_pipeline_reassembly_to_commit() {
    let (reassembly_tx, mut reassembly_rx) = tokio::sync::mpsc::channel::<Bytes>(16);
    let (orderer_tx, orderer_rx) = tokio::sync::mpsc::channel::<CommitGateInput>(16);

    let receiver_bytes = Arc::new(AtomicUsize::new(0));
    let mut reassembler = FragmentReassembler::new(reassembly_tx, 1024 * 1024, 60, BufferPool::new(4, 65536), receiver_bytes);
    let mut orderer = ChunkOrderer::new(orderer_tx, 0);
    let mut gate = CommitGate::new(orderer_rx);

    let gate_handle = tokio::spawn(async move {
        gate.run().await;
        gate.stats()
    });

    use braid::protocol::crc::compute_chunk_crc;
    use braid::protocol::crc::compute_fragment_crc;
    use braid::protocol::headers::{ChunkHeader, FragmentHeader};
    use bytes::BytesMut;

    let chunks_data: Vec<(u64, &[u8])> = vec![(0u64, b"first chunk"), (1u64, b"second chunk")];

    for (seq, data) in &chunks_data {
        let chunk_crc = compute_chunk_crc(*seq, data);
        let chunk_header = ChunkHeader::new(0, data.len() as u16, *seq, chunk_crc);

        let mut chunk_buf = BytesMut::with_capacity(ChunkHeader::LEN + data.len());
        chunk_buf.extend_from_slice(&chunk_header.to_bytes());
        chunk_buf.extend_from_slice(data);

        let fragment_crc = compute_fragment_crc(&chunk_buf);
        let frag_header = FragmentHeader {
            chunk_id: *seq as u32,
            fragment_index: 0,
            total_fragments: 1,
            fragment_length: chunk_buf.len() as u16,
            fragment_crc,
        };
        let mut fragment = Vec::with_capacity(FragmentHeader::LEN + chunk_buf.len());
        fragment.extend_from_slice(&frag_header.to_bytes());
        fragment.extend_from_slice(&chunk_buf);

        let completed = reassembler.add_fragment(fragment.into()).await.unwrap();
        assert!(completed);

        let reassembled = reassembly_rx.recv().await.unwrap();
        orderer.push_chunk(reassembled);
    }

    drop(orderer);

    let stats = tokio::time::timeout(Duration::from_secs(5), gate_handle)
        .await
        .expect("gate should complete")
        .expect("gate should not panic");

    assert_eq!(stats.chunks_committed, 2);
    assert_eq!(
        stats.bytes_written,
        (b"first chunk".len() + b"second chunk".len()) as u64
    );
    assert_eq!(stats.crc_failures, 0);
}

#[tokio::test]
async fn test_receiver_monitor_sends_queue_status() {
    let (control_tx, mut control_rx) = tokio::sync::mpsc::channel(16);
    let (cancel_tx, cancel_rx) = tokio::sync::mpsc::channel(1);

    let mut monitor = ReceiverMonitor::new(10, control_tx, Duration::from_millis(50), Arc::new(AtomicUsize::new(0)));

    let handle = tokio::spawn(async move {
        monitor.run(cancel_rx).await;
    });

    let msg = tokio::time::timeout(Duration::from_millis(200), control_rx.recv())
        .await
        .expect("should receive QUEUE_STATUS")
        .expect("channel should be open");

    match msg {
        braid::protocol::ControlMessage::QueueStatus {
            queued_chunks,
            queued_bytes: _,
            total_capacity: _,
        } => {
            assert_eq!(queued_chunks, 0);
        }
        other => panic!("expected QueueStatus, got: {:?}", other),
    }

    let _ = cancel_tx.send(()).await;
    let _ = handle.await;
}

#[tokio::test]
async fn test_shutdown_propagation_to_components() {
    let shutdown = ShutdownManager::new();

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

    let flag = shutdown.shutdown_flag();
    let poll_handle = tokio::spawn(async move {
        loop {
            if flag.load(std::sync::atomic::Ordering::Acquire) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    shutdown.initiate();

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

// ─── Error path tests ────────────────────────────────────────────────

/// Test: stats_arc().snapshot() correctly exposes write_errors = 0 after
/// normal CommitGate completion — this is the detection mechanism used by
/// braid_receive to detect write failures.
#[tokio::test]
async fn test_commit_gate_stats_after_normal_run() {
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let mut gate = CommitGate::new(rx);
    let stats_arc = gate.stats_arc();

    let handle = tokio::spawn(async move {
        gate.run().await;
        stats_arc.snapshot()
    });

    // Send a valid chunk then close the channel
    let input = CommitGateInput {
        data: Bytes::copy_from_slice(b"hello world"),
        sequence_number: 0,
        chunk_crc: 0,
    };
    tx.send(input).await.unwrap();
    drop(tx);

    let stats = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("gate should complete")
        .unwrap();

    assert_eq!(stats.chunks_committed, 1);
    assert_eq!(stats.write_errors, 0);
    assert_eq!(stats.bytes_written, 11);
}

/// Test: FileComplete with success=false is correctly serialized and
/// deserialized — used by the invalid-filename and write-error paths.
#[test]
fn test_file_complete_failure_round_trip() {
    let msg = ControlMessage::FileComplete {
        success: false,
        expected_hash: 0,
        computed_hash: 0,
    };
    let bytes = msg.to_bytes();
    let parsed = ControlMessage::try_from(bytes.as_slice()).unwrap();
    assert_eq!(parsed, msg);
}
