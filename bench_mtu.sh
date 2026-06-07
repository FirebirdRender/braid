#!/bin/bash
# BRAID throughput benchmarks at practical MTUs with optimized code
# Uses localhost loopback, 2 channels (hash-sharded), 4G data
set -e

BIN="./target/release/braid"
DATA_SIZE="4G"
PORT_BASE=9990
CHANNELS=2

echo "============================================================"
echo "BRAID throughput benchmarks - hash-sharded, $CHANNELS channels"
echo "Date: $(date)"
echo "============================================================"

for MTU in 1000 1400 8800; do
    PORT=$((PORT_BASE + MTU))
    echo ""
    echo "--- MTU=$MTU, ${CHANNELS} channels, port=$PORT ---"

    # Start receiver
    $BIN receive --listen 127.0.0.1:$PORT --channels $CHANNELS --mtu $MTU > /dev/null &
    RECV_PID=$!
    sleep 1

    # Run sender with mbuffer for measurement
    # /dev/zero is fastest data source for pure throughput measurement
    dd if=/dev/zero bs=1M count=4096 2>/dev/null | \
        mbuffer -q -m 256M -s ${MTU} -A "BRAID MTU=$MTU" 2>&1 | \
        $BIN send --host 127.0.0.1:$PORT --channels $CHANNELS --mtu $MTU

    wait $RECV_PID 2>/dev/null || true
    echo ""
done

echo ""
echo "============================================================"
echo "Benchmark complete."
echo "============================================================"
