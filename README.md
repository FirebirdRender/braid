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
      --destination <DESTINATION>  Destination address as IP:PORT
      --chunk-size <CHUNK_SIZE>    Chunk size in bytes (0 = adaptive) [default: 0]
      --channels <CHANNELS>        Number of parallel channels (0 = adaptive) [default: 0]
      --mtu <MTU>                  MTU for fragment sizing [default: 1500]
      --quiet                      Quiet mode: suppress progress output
      --verbose                    Verbose mode: detailed progress output
  -h, --help                       Print help
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

BRAID achieves ~455 MiB/s on loopback at MTU 1500 with 4 parallel channels (Criterion benchmark: `pipeline/full/65535`). Performance scales with MTU size and channel count — at MTU 8800, e2e throughput reaches ~669 MB/s (6.6 Gbps).

Key optimizations:
- **Zero-allocation fragment CRC**: chained `crc32fast::Hasher` avoids intermediate Vec
- **Hash-sharded reassembly**: N parallel reassemblers distributed by `chunk_id % N`
- **In-place header stripping**: `copy_within` + `truncate` instead of `to_vec()` copy
- **Direct header serialization**: `write_to(&mut impl BufMut)` avoids `to_bytes()` allocation
- **Bulk dispatch**: fragments batched per channel message (64 per batch)
- **Adaptive chunk sizing**: chunk size negotiates with receiver for optimal fit

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
│   ├── splitter.rs        # ChunkSplitter: stdin → fragments
│   ├── queue.rs           # QueueManager: LACP-like dispatch
│   └── worker.rs          # UdpSendWorker: per-channel UDP sender
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
├── flow/                  # Flow control (reactor, monitor)
├── progress/              # Progress reporting
├── shutdown/              # Graceful shutdown
├── adaptive/              # Adaptive sizing
└── error/                 # Error types
```

## Protocol Details

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
