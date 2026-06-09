# BRAID

**B**roadcast **R**eliable **A**daptive **I**nternet **D**atagram — a high-throughput UDP data transfer tool.

BRAID streams data over UDP using parallel channels, adaptive MTU sizing, CRC-verified chunking, and hash-sharded reassembly. Designed for bulk data transfer where TCP's head-of-line blocking and slow-start overhead are unacceptable.

## How It Works

BRAID implements a two-level framing protocol over UDP:

```
stdin → [ChunkSplitter] → fragments → [UDP workers] → network → [Reassemblers] → [ChunkOrderer] → [CommitGate] → stdout/file
```

### Two-Level Framing

1. **Chunk layer**: stdin is split into chunks (default: adaptive, up to 65535 bytes). Each chunk gets a 16-byte `ChunkHeader` containing magic byte, flags, payload length, sequence number, and a CRC over the sequence number + payload.

2. **Fragment layer**: each chunk is split into MTU-sized fragments. Each fragment gets a 14-byte `FragmentHeader` containing chunk ID, fragment index, total fragments, fragment length, and a CRC over the fragment payload.

### Parallel Channels

BRAID uses multiple independent UDP channels for parallel data transfer. The sender and receiver negotiate the number of channels via a TCP control connection. Fragments are distributed across channels using a least-loaded dispatch strategy.

### Hash-Sharded Reassembly

On the receiver side, fragments are distributed across N parallel reassemblers by `chunk_id % N`. Each reassembler independently tracks in-flight chunks, verifies fragment CRCs (pre-verified by UDP workers), assembles complete chunks, and verifies the chunk CRC before emitting to the ordering layer.

### Ordered Delivery

The `ChunkOrderer` maintains a binary heap of received chunks and emits them in sequence number order. Out-of-order chunks are buffered until the next expected sequence number arrives.

## Installation

```bash
cargo build --release
```

The binary is at `target/release/braid`.

## Usage

### Same-Host Baseline (without BRAID)

First, establish a baseline using a direct pipe or netcat to measure the raw pipe capacity:

```bash
# Baseline: pipe through netcat (TCP)
# Terminal 1 (receiver):
nc -l 127.0.0.1 9999 > /dev/null

# Terminal 2 (sender):
dd if=/dev/zero bs=1M count=1024 | pv -b > /dev/tcp/127.0.0.1/9999

# Or use mbuffer for measurement:
# Terminal 1:
mbuffer -q -4 -l 127.0.0.1:9999 -o /dev/null

# Terminal 2:
dd if=/dev/zero bs=1M count=1024 | mbuffer -q -4 -m 256M -s 64k | nc 127.0.0.1 9999
```

### Same Host with BRAID

```bash
# Terminal 1 (receiver):
./target/release/braid receive \
    --bind 127.0.0.1:9000 \
    --buffer-size 268435456 \
    --output /dev/null

# Terminal 2 (sender):
dd if=/dev/zero bs=1M count=1024 | \
    ./target/release/braid send \
    --destination 127.0.0.1:9000 \
    --channels 4 \
    --mtu 8800
```

With throughput measurement:

```bash
# Terminal 1:
./target/release/braid receive \
    --bind 127.0.0.1:9000 \
    --buffer-size 268435456 \
    --output /dev/null

# Terminal 2 (measure with pv):
dd if=/dev/zero bs=1M count=1024 | \
    pv -b | \
    ./target/release/braid send \
    --destination 127.0.0.1:9000 \
    --channels 4 \
    --mtu 8800
```

### Between Hosts

```bash
# On the receiver host (10.0.0.2):
./target/release/braid receive \
    --bind 0.0.0.0:9000 \
    --buffer-size 268435456 \
    --output received_data.bin

# On the sender host (10.0.0.1):
dd if=/dev/zero bs=1M count=1024 | \
    ./target/release/braid send \
    --destination 10.0.0.2:9000 \
    --channels 4 \
    --mtu 1500
```

### Writing to a File

```bash
# Receiver writes to file instead of stdout:
./target/release/braid receive \
    --bind 127.0.0.1:9000 \
    --buffer-size 268435456 \
    --output /path/to/output.bin
```

### File Mode (Sender-side file transfer)

File mode enables the sender to transmit a file with metadata, filename sanitization, and integrity verification:

```bash
# Terminal 1 (receiver):
./target/release/braid receive \
    --bind 127.0.0.1:9000 \
    --buffer-size 268435456 \
    --output /path/to/output.bin

# Terminal 2 (sender, file mode):
./target/release/braid send \
    --destination 127.0.0.1:9000 \
    --mode file \
    --input /path/to/input.bin
```

In file mode, the sender:
- Reads the file and sends it with `FileStart`/`FileComplete` control messages
- Streams CRC32C hash computation during transfer
- Waits for the receiver to acknowledge file completion with hash verification
- Reports transfer verification status (crc32c hash match)

The receiver:
- Sanitizes filenames to prevent path traversal attacks
- Detects existing files and auto-renames with a numeric suffix (e.g., `file.bin.1`)
- Computes CRC32C hash of the received file
- Reports hash match/failure back to the sender

## CLI Reference

### `braid send`

```
Usage: braid send [OPTIONS] --destination <DESTINATION>

Options:
  -d, --destination <DESTINATION>          Destination address as IP:PORT
  -c, --chunk-size <CHUNK_SIZE>            Chunk size in bytes (0 = adaptive) [default: 0]
      --channels <CHANNELS>                Number of parallel channels (0 = adaptive) [default: 0]
      --mtu <MTU>                          MTU for fragment sizing [default: 1500]
      --mode <MODE>                        Input mode: pipe or file [default: pipe]
      --input <PATH>                       Input file path for file mode
  -q, --quiet                              Quiet mode: suppress progress output
  -v, --verbose                            Verbose mode: detailed progress output
  -r, --max-rate <MAX_RATE>                Maximum send rate (0 = unlimited) [default: 0]
      --compress-lz4                       Enable LZ4 compression
      --compress-zstd                      Enable Zstd compression
      --retry                              Retry connection on initial failure
      --max-retries <MAX_RETRIES>          Maximum retry attempts [default: 3]
      --retry-delay <RETRY_DELAY>          Initial retry delay in ms [default: 1000]
      --channel-failure-threshold <N>      Consecutive failures before dead [default: 3]
      --batch-size <N>                     sendmmsg batch size [default: 16]
      --batch-usec <USEC>                  Max time (µs) to wait before flushing batch [default: 100]
      --no-batch                           Disable sendmmsg batching (use single-send per datagram)
      --chunker-threads <N>                Number of parallel chunker workers (0 = auto: num_cpus/2) [default: 0]
  -h, --help                               Print help
```

### `braid receive`

```
Usage: braid receive [OPTIONS] --bind <BIND> --buffer-size <BUFFER_SIZE>

Options:
      --bind <BIND>                Bind address as IP:PORT
      --buffer-size <BUFFER_SIZE>  Maximum receive buffer size in bytes
      --output <OUTPUT>            Path to output file (default: stdout)
      --mtu <MTU>                  MTU for receive buffer sizing [default: 1500]
      --quiet                      Quiet mode: suppress progress output
      --verbose                    Verbose mode: detailed progress output
  -h, --help                       Print help
```

## MTU Tuning

BRAID fragments chunks to fit within the configured MTU. The fragment payload size is `MTU - 14` (FragmentHeader overhead). Larger MTUs reduce per-byte overhead and improve throughput:

| MTU | Fragment Payload | Typical Use Case |
|-----|-----------------|------------------|
| 1500 | 1486 | Standard Ethernet (default) |
| 9000 | 8986 | Jumbo frames (datacenter) |
| 65535 | 65521 | Loopback (max UDP payload) |

On loopback, larger MTUs (8800–65535) give the best throughput. On real networks, match your path MTU — use `tracepath` or `ping -M do -s <size>` to discover it.

## Performance

BRAID achieves ~455 MiB/s on loopback at MTU 1500 with 4 parallel channels (Criterion benchmark: `pipeline/full/65535`). Performance scales with MTU size and channel count — at MTU 8800, throughput reaches **908 MiB/s (7.6 Gbps)** with sendmmsg batching and the multithreaded chunker enabled.

Key optimizations:
- **sendmmsg batch send**: batches up to 16 datagrams per syscall (default), or configurable via `--batch-size`. Reduces syscall overhead by up to 16×. Falls back to single-send on kernels without sendmmsg support.
- **Multithreaded chunker**: LZ4 compression, CRC computation, and fragmentation run in parallel across N worker tasks (default: `num_cpus / 2`). Configurable via `--chunker-threads`.
- **Lock-free buffer pool**: `crossbeam::ArrayQueue` eliminates `Mutex` contention when multiple chunker workers access the buffer pool simultaneously.
- **Zero-copy buffer pool**: pre-allocated `BytesMut` pool with semaphore-based acquire/release eliminates hot-path allocations
- **Zero-allocation fragment CRC**: chained `crc32fast::Hasher` avoids intermediate Vec
- **Hash-sharded reassembly**: N parallel reassemblers distributed by `chunk_id % N`
- **In-place header stripping**: `copy_within` + `truncate` instead of `to_vec()` copy
- **Direct header serialization**: `write_to(&mut impl BufMut)` avoids `to_bytes()` allocation
- **Bulk dispatch**: fragments batched per channel message (64 per batch)
- **Adaptive chunk sizing**: chunk size negotiates with receiver for optimal fit
- **LZ4 compression**: optional chunk-level compression with auto-disable for incompressible data

## Architecture

```
src/
├── bin/
│   ├── braid.rs           # CLI entry point (clap)
│   ├── braid_send.rs      # Send orchestrator
│   └── braid_receive.rs   # Receive orchestrator
├── protocol/
│   ├── headers.rs         # FragmentHeader, ChunkHeader (14B + 16B)
│   ├── crc.rs             # CRC32C computation
│   └── control.rs         # TCP control protocol messages
├── sender/
│   ├── splitter.rs        # ChunkSplitter: stdin → fragments (single-threaded)
│   ├── parallel_splitter.rs # Dispatcher + chunker_worker (multithreaded)
│   ├── queue.rs           # QueueManager: LACP-like dispatch
│   ├── worker.rs          # UdpSendWorker + BatchSendWorker (sendmmsg)
│   └── health.rs          # ChannelHealth: per-channel failure tracking
├── receiver/
│   ├── reassembly.rs      # FragmentReassembler: fragments → chunks
│   ├── ordering.rs        # ChunkOrderer: sequence number ordering
│   └── commit.rs          # CommitGate: output writer
├── file_mode/
│   ├── hash.rs            # Streaming CRC32C hash computation
│   ├── sanitize.rs        # Filename sanitization (path traversal prevention)
│   ├── output.rs          # Overwrite detection with auto-rename
│   ├── sender.rs          # FileModeSender: file metadata + hash
│   └── receiver.rs        # FileModeReceiver: file output + hash verification
├── control/
│   ├── client.rs          # Control protocol client
│   ├── server.rs          # Control protocol server
│   └── negotiation.rs     # Channel/chunk size negotiation
├── buffer/                # Buffer pool and ring buffer
├── compress/              # LZ4 compression (compress_lz4, decompress_lz4)
├── flow/                  # Flow control (reactor, monitor)
├── progress/              # Progress reporting
├── shutdown/              # Graceful shutdown
├── adaptive/              # Adaptive sizing
└── error/                 # Error types
```

## v0.5.0 Features

### sendmmsg Batch Send

v0.5.0 replaces per-datagram `send_to()` with `sendmmsg()` batching:

- **`--batch-size`**: Number of datagrams per `sendmmsg` call (default: 16). Reduces syscall overhead by up to 16× on Linux.
- **`--batch-usec`**: Maximum time to wait before flushing an incomplete batch (default: 100µs).
- **`--no-batch`**: Disable batching and revert to single-send per datagram (for compatibility).
- **ENOSYS fallback**: Automatically detects kernels without sendmmsg support and falls back to single-send via atomic boolean toggle.
- **Memory safety**: `msgvec`/`iovecs` arrays rebuilt from scratch after each partial send — no dangling pointer risk.
- **EAGAIN handling**: On socket congestion, yields via `socket.writable()` before retrying (up to 1024 retries).

### Multithreaded Chunker

v0.5.0 replaces the single-threaded chunker pipeline with a dispatcher + N parallel workers:

- **`--chunker-threads`**: Number of parallel worker tasks (default: `num_cpus / 2`, minimum 1).
- **Dispatcher**: Reads stdin/file, round-robins raw chunks to workers via dedicated mpsc channels.
- **Workers**: Each worker performs LZ4 compression, CRC computation, and fragmentation independently.
- **Wire format identical**: Receiver has no knowledge of parallelization — fragments are identical to the single-threaded path.
- **Lock-free BufferPool**: `crossbeam::ArrayQueue` replaces `Mutex<Vec<usize>>` for contention-free concurrent buffer access.

### Performance Benchmarks

Pre-optimization (single-threaded chunker, no sendmmsg):

| Data Volume | Wire Time | Throughput | Receiver Peak |
|-------------|-----------|------------|---------------|
| 1 GiB | 1.610s | 635 MiB/s (5.3 Gbps) | 507.73 MB/s |

Post-optimization (batch=16, chunker-threads=auto):

| Data Volume | Wire Time | Throughput | Receiver Peak |
|-------------|-----------|------------|---------------|
| 1 GiB | 1.126s | 908 MiB/s (7.6 Gbps) | 527.55 MB/s |

Full pipeline (mbuffer → braid → mbuffer): 715 MiB/s (6.0 Gbps)

**Improvement: +43% throughput** over the single-threaded baseline.

### Fragment Wire Format

```
Offset  Size  Field
0       4     chunk_id (u32 BE)
4       2     fragment_index (u16 BE)
6       2     total_fragments (u16 BE)
8       2     fragment_length (u16 BE)
10      4     fragment_crc (u32 BE)
14      N     fragment payload (ChunkHeader + chunk data)
```

### Chunk Wire Format

```
Offset  Size  Field
0       1     magic (0x50)
1       1     flags
2       2     payload_length (u16 BE)
4       8     sequence_number (u64 BE)
12      4     chunk_crc (u32 BE)
16      N     chunk payload data
```

### Control Protocol

BRAID uses a TCP control connection for negotiation and status. The sender connects to the receiver's control port, sends a `Hello`, and negotiates channel count and chunk size. Once negotiation completes, data flows over UDP channels. On completion, an `Eos` message signals clean shutdown.

## Testing

```bash
# Run all tests
cargo test

# Run benchmarks
cargo bench

# Run with logging
RUST_LOG=debug cargo run -- send --destination 127.0.0.1:9000 < input.bin
RUST_LOG=debug cargo run -- receive --bind 127.0.0.1:9000 --buffer-size 65536
```

## License

MIT
