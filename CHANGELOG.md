# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.0] - 2026-06-10

### Added

- **Receiver backpressure wiring**: Full end-to-end backpressure from receiver pipeline to sender splitter
- **Merge governor**: Combines local backpressure (QueueManager) with remote backpressure (ReceiverMonitor) via `local_paused || remote_paused` logic
- **Flow control pause channel**: `flow_pause_tx/rx` mpsc channel between `SenderReactor` and splitter merge governor
- **Capacity synchronization**: `total_capacity` field in `QueueStatus` protocol message, sender updates `FlowController` capacity on first status message

### Changed

- `SenderReactor` now sends pause/resume signals on Orange/Red/Green fullness level transitions
- `ReceiverMonitor` sends actual `total_capacity` in `QueueStatus` messages instead of placeholder
- Merge governor uses `tokio::select! { biased; }` for deterministic ordering between local and remote pause sources

## [0.5.0] - 2026-06-09

### Added

- **sendmmsg batch send**: `BatchSendWorker` replaces per-datagram `send_to()` with `sendmmsg()` batching (default batch size: 16). Reduces syscall overhead by up to 16× on Linux. ENOSYS fallback to single-send via atomic boolean toggle.
- **Multithreaded chunker**: New `parallel_splitter` module with `Dispatcher` + N parallel `chunker_worker` tasks. Workers run LZ4 compression, CRC, and fragmentation in parallel across available cores. Default worker count: `num_cpus / 2`.
- **Lock-free BufferPool**: Replaced `Mutex<Vec<usize>>` free-list with `crossbeam::queue::ArrayQueue` for contention-free concurrent access by parallel chunker workers.
- **CLI flags**: `--batch-size` (sendmmsg batch size, default 16), `--batch-usec` (flush timeout, default 100µs), `--no-batch` (disable sendmmsg), `--chunker-threads` (parallel workers, 0=auto).

### Changed

- Version bump: 0.4.0 → 0.5.0

### Performance

- 43% throughput improvement on loopback: 635 → 908 MiB/s (MTU 8800, 4 channels, 1 GiB data)
- Receiver peak throughput stable at ~528 MB/s (post-optimization)
- Full pipeline (mbuffer → braid → mbuffer): 715 MiB/s (6.0 Gbps)

## [0.4.0] - 2026-06-09

### Added

- **Buffer pool rewrite**: Semaphore-based `BufferPool` with `BytesMut` storage, `PoolBuffer` with auto-return on drop, `acquire_many(n)` for batch operations
- **Zero-copy pipeline**: All hot-path channel types migrated from `Vec<u8>` to `bytes::Bytes` — splitter, queue manager, UDP receive worker, reassembly, ordering, and commit gate
- **BufferPool integration**: Pool buffers used in hot paths — UDP receive worker (replaces `vec![0u8; mtu]`), fragment reassembly (replaces `Vec::with_capacity`), and chunk splitter read buffer
- **Connection resilience**: `ChannelHealth` module for per-channel failure tracking with configurable threshold
- **Connection retry**: `ControlClient::connect_with_retry()` with exponential backoff (doubles, capped at 60s)
- **Retry CLI flags**: `--retry`, `--max-retries`, `--retry-delay`, `--channel-failure-threshold` for `braid send`
- **Reconnect protocol**: Full `Reconnect` message exchange — sender detects all-channels-down, sends Reconnect to receiver, receiver opens new UDP sockets, sender resumes transfer
- **Server retry**: `ControlServer::accept_with_retry()` for re-entering accept loop after completed sessions
- **Worker health integration**: `UdpSendWorker` reports `WorkerResult` (success/failure) via health channel, `QueueManager` processes failures and marks dead workers
- **LZ4 compression**: New `compress` module with `compress_lz4()`/`decompress_lz4()` via `lz4_flex` (pure Rust, zero C dependencies)
- **Per-chunk compression flag**: `COMPRESSED_LZ4` flag in `ChunkHeader.flags` field for per-chunk compress/no-compress decision
- **Auto-disable**: Incompressible data sent uncompressed (compressed size ≥ original → `COMPRESSION_NONE`)
- **CRC on uncompressed data**: CRC computed before compression, verified after decompression — catches both network errors and decompression bugs
- **Compression CLI flags**: `--compress-lz4` and `--compress-zstd` for `braid send`
- **Compression negotiation**: Feature bits in `NegotiationConfig` for LZ4 and Zstd capability advertising
- **Worker `channel_id` tracking**: `UdpSendWorker` now tracks its channel ID for health reporting
- **QueueManager `last_chunk_id`**: Tracks last dispatched chunk ID for reconnect resume point

### Changed

- `BufferPool` internals: `Mutex<Vec<Vec<u8>>>` → `tokio::sync::Semaphore` + `Mutex<Vec<usize>>` free-list + `Vec<BytesMut>` storage
- `PoolBuffer` replaces `BufferGuard`: zero-copy split (`split_to().freeze()`) instead of `to_vec()` copy
- `ChunkSplitter::new()` now takes `pool: BufferPool` parameter
- `FragmentReassembler::new()` now takes `pool: BufferPool` parameter
- `UdpSendWorker::new()` now takes `channel_id: usize` as first parameter
- `QueueManagerBuilder::build()` returns 3-tuple `(QueueManager, Vec<WorkerReceiver>, mpsc::Receiver<WorkerResult>)` instead of 2-tuple
- Benchmarks updated for async `BufferPool::acquire()` API

### Removed

- Old `BufferGuard` and `get_buffer()` from `buffer/pool.rs`
- `Deref`/`DerefMut` impls on buffer types
- `legacy_buffers` storage in `PoolInner`

### Added

- File mode (`--mode file --input <path>`): send files with metadata, filename sanitization, overwrite detection with auto-rename, streaming CRC32C hash verification, and progress display
- `FileStart` and `FileComplete` control messages for file transfer handshake
- `BraidError::FileHashMismatch` variant for integrity failure reporting
- `FileSplitter` wrapper for file-specific splitter behavior via `AsyncRead`
- `ChunkSplitter::run` now accepts `impl AsyncRead` instead of requiring stdin
- E2E test suite for file mode: happy path (small, medium, empty file), edge cases

### Changed

- `ChunkSplitter::run` refactored to accept generic `impl AsyncRead` input

### Fixed

- Integration test hang: circular dependency deadlock in `braid_receive.rs` where `reassembly_tx` was held in scope while awaiting the orderer, preventing channel close — added `drop(reassembly_tx)` before orderer await
- `fault_injection_test.rs` compilation errors: `run_loopback_with_tc` return type, missing `test_data()` helper, duplicate function
- `test_sigint_during_receive` assertion: receiver now exits with code 0 (graceful shutdown via ShutdownManager) instead of non-zero
- All source files formatted with `cargo fmt`

## [0.2.0] - YYYY-MM-DD

### Added

- `--max-rate` flag to `braid send` for limiting send throughput (e.g., `--max-rate 125000000` for 1Gbps)
- Short flag `-r` for `--max-rate` on `braid send`
- Subcommand aliases: `braid s` for `braid send`, `braid recv` and `braid rx` for `braid receive`
- Byte-size suffix parser supporting K/M/G suffixes (case-insensitive, decimal) for `--buffer-size`, `--chunk-size`, and similar values (e.g., `64m` = 64,000,000)
- Data-rate suffix parser supporting K/M/G suffixes (case-insensitive, decimal) for `--max-rate` (e.g., `125m` = 125,000,000)

### Changed

- Added short flags across all CLI arguments: `-d` (destination), `-c` (chunk-size), `-q` (quiet), `-v` (verbose), `-b` (bind), `-s` (buffer-size), `-o` (output)

### Fixed

- ChunkReassembler and ChunkOrderer made fully async to prevent blocking the event loop during reassembly and ordering
- ChunkSplitter now supports pause/resume via an mpsc channel, enabling natural backpressure without dropping fragments
- QueueStatus messages now route over the TCP control connection instead of UDP, preventing status message loss under congestion
- ReceiverMonitor converted to async execution to avoid blocking the receive pipeline
- Dead channel cleanup: channels that stop responding are now properly detected and removed from the active set
- E2E CRC integrity test corrected to properly verify chunk and fragment CRCs end-to-end
- E2E flow control test stabilized to correctly exercise pause/resume and backpressure paths
- E2E SIGINT handling test fixed to properly verify graceful shutdown on interrupt signals

## [0.1.0] - YYYY-MM-DD

### Added

- Initial release of BRAID (Broadcast Reliable Adaptive Internet Datagram)
- Two-level UDP framing protocol: chunk layer (CRC-verified chunks) and fragment layer (MTU-sized fragments)
- Parallel UDP channels with LACP-like dispatch strategy
- Hash-sharded reassembly distributed by `chunk_id % N`
- Ordered delivery via ChunkOrderer with binary heap
- TCP control protocol for channel count and chunk size negotiation
- Adaptive chunk sizing negotiated between sender and receiver
- Zero-allocation fragment CRC via chained `crc32fast::Hasher`
- In-place header stripping using `copy_within` and `truncate`
- Direct header serialization via `write_to(&mut impl BufMut)`
- Bulk fragment dispatch (64 fragments per batch)
- Progress reporting with Quiet, Normal, and Verbose verbosity levels
- Graceful shutdown via `Eos` control message
