# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-06-08

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
