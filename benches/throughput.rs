//! BRAID throughput benchmarks.
//!
//! Measures raw throughput (MB/s) for each component in the pipeline:
//!   1. CRC throughput — compute_fragment_crc and compute_chunk_crc
//!   2. Fragment serialization — encode/decode FragmentHeader and ChunkHeader
//!   3. Chunk splitter — split input into chunks and fragments
//!   4. Fragment reassembly — reassemble fragments back into chunks
//!   5. Buffer pool — acquire/release throughput
//!   6. Ring buffer — push/pop throughput (single-threaded)
//!   7. Full pipeline — end-to-end (optional, #[ignore])
//!
//! Run: cargo bench
//! Compare: iperf3 -c 127.0.0.1 -t 30 -p 5201

use std::hint::black_box;
use std::time::Duration;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use braid::buffer::{BufferPool, RingBuffer};
use braid::protocol::crc::{compute_chunk_crc, compute_fragment_crc};
use braid::protocol::headers::{ChunkHeader, FragmentHeader};
use braid::receiver::reassembly::FragmentReassembler;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Generate a buffer filled with a deterministic byte pattern.
fn gen_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i & 0xFF) as u8).collect()
}

// ---------------------------------------------------------------------------
// 1. CRC throughput
// ---------------------------------------------------------------------------

fn bench_crc_fragment(c: &mut Criterion) {
    let sizes = [64, 256, 1024, 4096, 16384, 65536];

    let mut group = c.benchmark_group("crc/fragment");
    for &size in &sizes {
        let data = gen_data(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| {
                let crc = compute_fragment_crc(black_box(data));
                black_box(crc);
            });
        });
    }
    group.finish();
}

fn bench_crc_chunk(c: &mut Criterion) {
    let sizes = [64, 256, 1024, 4096, 16384, 65536];

    let mut group = c.benchmark_group("crc/chunk");
    for &size in &sizes {
        let data = gen_data(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| {
                let crc = compute_chunk_crc(black_box(42), black_box(data));
                black_box(crc);
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 2. Fragment / Chunk header serialization
// ---------------------------------------------------------------------------

fn bench_fragment_header_encode(c: &mut Criterion) {
    let header = FragmentHeader {
        chunk_id: 0xDEADBEEF,
        fragment_index: 1234,
        total_fragments: 5678,
        fragment_length: 1400,
        fragment_crc: 0xCAFEBABE,
    };

    let mut group = c.benchmark_group("serialize/fragment_header_encode");
    group.throughput(Throughput::Bytes(FragmentHeader::LEN as u64));
    group.bench_function("to_bytes", |b| {
        b.iter(|| {
            let bytes = black_box(header).to_bytes();
            black_box(bytes);
        });
    });
    group.finish();
}

fn bench_fragment_header_decode(c: &mut Criterion) {
    let header = FragmentHeader {
        chunk_id: 0xDEADBEEF,
        fragment_index: 1234,
        total_fragments: 5678,
        fragment_length: 1400,
        fragment_crc: 0xCAFEBABE,
    };
    let bytes = header.to_bytes();

    let mut group = c.benchmark_group("serialize/fragment_header_decode");
    group.throughput(Throughput::Bytes(FragmentHeader::LEN as u64));
    group.bench_function("try_from", |b| {
        b.iter(|| {
            let parsed = FragmentHeader::try_from(black_box(&bytes[..])).unwrap();
            black_box(parsed);
        });
    });
    group.finish();
}

fn bench_chunk_header_encode(c: &mut Criterion) {
    let header = ChunkHeader::new(0, 65535, 0xDEADBEEF_CAFEBABE, 0x12345678);

    let mut group = c.benchmark_group("serialize/chunk_header_encode");
    group.throughput(Throughput::Bytes(ChunkHeader::LEN as u64));
    group.bench_function("to_bytes", |b| {
        b.iter(|| {
            let bytes = black_box(header).to_bytes();
            black_box(bytes);
        });
    });
    group.finish();
}

fn bench_chunk_header_decode(c: &mut Criterion) {
    let header = ChunkHeader::new(0, 65535, 0xDEADBEEF_CAFEBABE, 0x12345678);
    let bytes = header.to_bytes();

    let mut group = c.benchmark_group("serialize/chunk_header_decode");
    group.throughput(Throughput::Bytes(ChunkHeader::LEN as u64));
    group.bench_function("try_from", |b| {
        b.iter(|| {
            let parsed = ChunkHeader::try_from(black_box(&bytes[..])).unwrap();
            black_box(parsed);
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Chunk splitter throughput
// ---------------------------------------------------------------------------

/// Benchmark the core chunk->fragment splitting logic (without I/O).
/// NOTE: ChunkHeader.payload_length is u16 (max 65535), so sizes > 65535 wrap to 0.
fn bench_splitter_fragment_construction(c: &mut Criterion) {
    let chunk_sizes = [4096, 16384, 65535];

    let mut group = c.benchmark_group("splitter/fragment_construction");
    for &chunk_size in &chunk_sizes {
        let payload = gen_data(chunk_size);
        let mtu = 1500;
        let frag_payload_size = mtu - FragmentHeader::LEN;

        // Build chunk header + payload
        let chunk_crc = compute_chunk_crc(0, &payload);
        let chunk_header = ChunkHeader::new(0, payload.len() as u16, 0, chunk_crc);
        let mut chunk_buf = Vec::with_capacity(ChunkHeader::LEN + payload.len());
        chunk_buf.extend_from_slice(&chunk_header.to_bytes());
        chunk_buf.extend_from_slice(&payload);

        let total_fragments = (chunk_buf.len() + frag_payload_size - 1) / frag_payload_size;

        group.throughput(Throughput::Bytes(chunk_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(chunk_size),
            &(chunk_buf, frag_payload_size, total_fragments),
            |b, (buf, fps, total)| {
                b.iter(|| {
                    let mut fragments = Vec::with_capacity(*total);
                    for fi in 0..*total {
                        let start = fi * fps;
                        let end = std::cmp::min(start + fps, buf.len());
                        let fp = &buf[start..end];
                        let fcrc = compute_fragment_crc(fp);
                        let fh = FragmentHeader {
                            chunk_id: 0,
                            fragment_index: fi as u16,
                            total_fragments: *total as u16,
                            fragment_length: fp.len() as u16,
                            fragment_crc: fcrc,
                        };
                        let mut frag = Vec::with_capacity(FragmentHeader::LEN + fp.len());
                        frag.extend_from_slice(&fh.to_bytes());
                        frag.extend_from_slice(fp);
                        fragments.push(frag);
                    }
                    black_box(fragments);
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 4. Fragment reassembly throughput
// ---------------------------------------------------------------------------

fn bench_reassembly(c: &mut Criterion) {
    let chunk_sizes = [4096, 16384, 65535];

    let mut group = c.benchmark_group("reassembly/throughput");
    for &chunk_size in &chunk_sizes {
        let payload = gen_data(chunk_size);
        let mtu = 1500;
        let frag_payload_size = mtu - FragmentHeader::LEN;

        // Build chunk header + payload
        let chunk_crc = compute_chunk_crc(0, &payload);
        let chunk_header = ChunkHeader::new(0, payload.len() as u16, 0, chunk_crc);
        let mut chunk_buf = Vec::with_capacity(ChunkHeader::LEN + payload.len());
        chunk_buf.extend_from_slice(&chunk_header.to_bytes());
        chunk_buf.extend_from_slice(&payload);

        // Build all fragments
        let total_fragments = (chunk_buf.len() + frag_payload_size - 1) / frag_payload_size;
        let fragments: Vec<Vec<u8>> = (0..total_fragments)
            .map(|fi| {
                let start = fi * frag_payload_size;
                let end = std::cmp::min(start + frag_payload_size, chunk_buf.len());
                let fp = &chunk_buf[start..end];
                let fcrc = compute_fragment_crc(fp);
                let fh = FragmentHeader {
                    chunk_id: 0,
                    fragment_index: fi as u16,
                    total_fragments: total_fragments as u16,
                    fragment_length: fp.len() as u16,
                    fragment_crc: fcrc,
                };
                let mut frag = Vec::with_capacity(FragmentHeader::LEN + fp.len());
                frag.extend_from_slice(&fh.to_bytes());
                frag.extend_from_slice(fp);
                frag
            })
            .collect();

        group.throughput(Throughput::Bytes(chunk_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(chunk_size),
            &fragments,
            |b, frags| {
                b.iter(|| {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    let (tx, _rx) = tokio::sync::mpsc::channel(16);
                    let pool = BufferPool::new(128, 65536);
                    let mut reassembler = FragmentReassembler::new(tx, 10 * 1024 * 1024, 60, pool);
                    for frag in frags {
                        let completed = rt
                            .block_on(reassembler.add_fragment(black_box(Bytes::from(
                                frag.clone(),
                            ))))
                            .unwrap();
                        if completed {
                            break;
                        }
                    }
                    black_box(reassembler.in_flight_count());
                });
            },
        );
    }
    group.finish();
}

/// Benchmark reassembly with out-of-order fragments (worst case).
fn bench_reassembly_out_of_order(c: &mut Criterion) {
    let chunk_size = 65535;
    let payload = gen_data(chunk_size);
    let mtu = 1500;
    let frag_payload_size = mtu - FragmentHeader::LEN;

    let chunk_crc = compute_chunk_crc(0, &payload);
    let chunk_header = ChunkHeader::new(0, payload.len() as u16, 0, chunk_crc);
    let mut chunk_buf = Vec::with_capacity(ChunkHeader::LEN + payload.len());
    chunk_buf.extend_from_slice(&chunk_header.to_bytes());
    chunk_buf.extend_from_slice(&payload);

    let total_fragments = (chunk_buf.len() + frag_payload_size - 1) / frag_payload_size;
    let mut fragments: Vec<Vec<u8>> = (0..total_fragments)
        .map(|fi| {
            let start = fi * frag_payload_size;
            let end = std::cmp::min(start + frag_payload_size, chunk_buf.len());
            let fp = &chunk_buf[start..end];
            let fcrc = compute_fragment_crc(fp);
            let fh = FragmentHeader {
                chunk_id: 0,
                fragment_index: fi as u16,
                total_fragments: total_fragments as u16,
                fragment_length: fp.len() as u16,
                fragment_crc: fcrc,
            };
            let mut frag = Vec::with_capacity(FragmentHeader::LEN + fp.len());
            frag.extend_from_slice(&fh.to_bytes());
            frag.extend_from_slice(fp);
            frag
        })
        .collect();
    // Reverse for worst-case out-of-order
    fragments.reverse();

    let mut group = c.benchmark_group("reassembly/out_of_order");
    group.throughput(Throughput::Bytes(chunk_size as u64));
    group.bench_function("65535B_reversed", |b| {
        b.iter(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let (tx, _rx) = tokio::sync::mpsc::channel(16);
            let pool = BufferPool::new(128, 65536);
            let mut reassembler = FragmentReassembler::new(tx, 10 * 1024 * 1024, 60, pool);
            for frag in &fragments {
                let completed = rt
                    .block_on(reassembler.add_fragment(black_box(Bytes::from(frag.clone()))))
                    .unwrap();
                if completed {
                    break;
                }
            }
            black_box(reassembler.in_flight_count());
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// 5. Buffer pool throughput
// ---------------------------------------------------------------------------

fn bench_buffer_pool_acquire_release(c: &mut Criterion) {
    let buffer_sizes = [64, 512, 4096, 65536];

    let mut group = c.benchmark_group("buffer_pool/acquire_release");
    for &buf_size in &buffer_sizes {
        let pool = BufferPool::new(128, buf_size);

        group.throughput(Throughput::Bytes(buf_size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(buf_size), &pool, |b, p| {
            b.iter(|| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                let guard = rt.block_on(p.acquire());
                black_box(guard.buffer[0]);
                drop(guard);
            });
        });
    }
    group.finish();
}

fn bench_buffer_pool_contention(c: &mut Criterion) {
    use std::sync::Arc;
    use std::thread;

    let pool = Arc::new(BufferPool::new(1024, 4096));
    let thread_counts = [1, 2, 4, 8];

    let mut group = c.benchmark_group("buffer_pool/contention");
    for &num_threads in &thread_counts {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_threads),
            &num_threads,
            |b, &n| {
                b.iter(|| {
                    let pool = Arc::clone(&pool);
                    let mut handles = Vec::with_capacity(n);
                    for _ in 0..n {
                        let p = Arc::clone(&pool);
                        handles.push(thread::spawn(move || {
                            let rt = tokio::runtime::Runtime::new().unwrap();
                            for _ in 0..1000 {
                                let guard = rt.block_on(p.acquire());
                                // Touch a byte to force actual memory access
                                let _byte = black_box(guard.buffer[0]);
                                drop(guard);
                            }
                        }));
                    }
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 6. Ring buffer throughput
// ---------------------------------------------------------------------------

fn bench_ring_buffer_push_pop(c: &mut Criterion) {
    let capacities = [16, 64, 256, 1024];

    let mut group = c.benchmark_group("ring_buffer/push_pop");
    for &cap in &capacities {
        let ring = RingBuffer::new(cap);

        group.bench_with_input(BenchmarkId::from_parameter(cap), &ring, |b, r| {
            b.iter(|| {
                for i in 0..cap {
                    r.push(black_box(i));
                }
                for _ in 0..cap {
                    let val: usize = r.pop();
                    black_box(val);
                }
            });
        });
    }
    group.finish();
}

fn bench_ring_buffer_concurrent(c: &mut Criterion) {
    use std::sync::Arc;
    use std::thread;

    let items = 10_000;
    let capacities = [16, 64, 256];

    let mut group = c.benchmark_group("ring_buffer/concurrent");
    for &cap in &capacities {
        group.throughput(Throughput::Elements((items * 2) as u64));
        group.bench_with_input(BenchmarkId::from_parameter(cap), &cap, |b, &c| {
            b.iter(|| {
                let ring = Arc::new(RingBuffer::new(c));
                let r = Arc::clone(&ring);
                let producer = thread::spawn(move || {
                    for i in 0..items {
                        r.push(black_box(i));
                    }
                });
                let consumer = thread::spawn(move || {
                    let mut sum = 0usize;
                    for _ in 0..items {
                        sum += ring.pop();
                    }
                    black_box(sum);
                });
                producer.join().unwrap();
                consumer.join().unwrap();
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 7. Full pipeline (end-to-end)
// ---------------------------------------------------------------------------

/// Full pipeline benchmark: splitter -> fragments -> reassembly.
///
/// This exercises the core data transformation path without I/O or networking.
fn bench_full_pipeline(c: &mut Criterion) {
    let sizes = [65535, 262140, 1048572];

    let mut group = c.benchmark_group("pipeline/full");
    for &size in &sizes {
        let input = gen_data(size);
        let mtu = 1500;
        let chunk_size = 65535;
        let frag_payload_size = mtu - FragmentHeader::LEN;

        // Build all fragments from the input
        let mut all_fragments: Vec<Vec<u8>> = Vec::new();

        for offset in (0..input.len()).step_by(chunk_size) {
            let end = std::cmp::min(offset + chunk_size, input.len());
            let payload = &input[offset..end];

            let chunk_crc = compute_chunk_crc((offset / chunk_size) as u64, payload);
            let chunk_header = ChunkHeader::new(
                0,
                payload.len() as u16,
                (offset / chunk_size) as u64,
                chunk_crc,
            );
            let mut chunk_buf = Vec::with_capacity(ChunkHeader::LEN + payload.len());
            chunk_buf.extend_from_slice(&chunk_header.to_bytes());
            chunk_buf.extend_from_slice(payload);

            let total_fragments = (chunk_buf.len() + frag_payload_size - 1) / frag_payload_size;
            for fi in 0..total_fragments {
                let fstart = fi * frag_payload_size;
                let fend = std::cmp::min(fstart + frag_payload_size, chunk_buf.len());
                let fp = &chunk_buf[fstart..fend];
                let fcrc = compute_fragment_crc(fp);
                let fh = FragmentHeader {
                    chunk_id: (offset / chunk_size) as u32,
                    fragment_index: fi as u16,
                    total_fragments: total_fragments as u16,
                    fragment_length: fp.len() as u16,
                    fragment_crc: fcrc,
                };
                let mut frag = Vec::with_capacity(FragmentHeader::LEN + fp.len());
                frag.extend_from_slice(&fh.to_bytes());
                frag.extend_from_slice(fp);
                all_fragments.push(frag);
            }
        }

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &all_fragments,
            |b, frags| {
                b.iter(|| {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
                    let pool = BufferPool::new(256, 65536);
                    let mut reassembler =
                        FragmentReassembler::new(tx, 10 * 1024 * 1024, 60, pool);

                    let mut total_reassembled = 0usize;
                    for frag in frags {
                        let frag_bytes = Bytes::from(frag.clone());
                        if let Ok(true) = rt
                            .block_on(reassembler.add_fragment(black_box(frag_bytes)))
                        {
                            // Drain the emitted chunk
                            let _ = rx.try_recv().map(|chunk| {
                                total_reassembled += chunk.len();
                            });
                        }
                    }
                    // Drain any remaining emitted chunks
                    while let Ok(chunk) = rx.try_recv() {
                        total_reassembled += chunk.len();
                    }
                    black_box(total_reassembled);
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Criterion configuration
// ---------------------------------------------------------------------------

criterion_group! {
    name = crc;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(100);
    targets = bench_crc_fragment, bench_crc_chunk
}

criterion_group! {
    name = serialize;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(100);
    targets = bench_fragment_header_encode, bench_fragment_header_decode,
              bench_chunk_header_encode, bench_chunk_header_decode
}

criterion_group! {
    name = splitter;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(100);
    targets = bench_splitter_fragment_construction
}

criterion_group! {
    name = reassembly;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(100);
    targets = bench_reassembly, bench_reassembly_out_of_order
}

criterion_group! {
    name = buffer_pool;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(100);
    targets = bench_buffer_pool_acquire_release, bench_buffer_pool_contention
}

criterion_group! {
    name = ring_buffer;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(100);
    targets = bench_ring_buffer_push_pop, bench_ring_buffer_concurrent
}

criterion_group! {
    name = pipeline;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(5))
        .sample_size(50);
    targets = bench_full_pipeline
}

criterion_main!(
    crc,
    serialize,
    splitter,
    reassembly,
    buffer_pool,
    ring_buffer,
    pipeline
);
