#!/usr/bin/env bash
# Binnacle 100,000 Message Benchmark Automation
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

if [ ! -f "${ROOT_DIR}/bin/binnacle-bench" ]; then
    echo "Compiling native C benchmark harness..."
    make build
fi

HOST="${1:-127.0.0.1}"
PORT="${2:-1883}"
CLIENTS="${3:-16}"
MSGS="${4:-100000}"
PAYLOAD_SIZE="${5:-128}"

echo "Executing benchmark against ${HOST}:${PORT}..."
echo "  Worker clients : ${CLIENTS}"
echo "  Total messages : ${MSGS}"
echo "  Payload size   : ${PAYLOAD_SIZE} bytes"

"${ROOT_DIR}/bin/binnacle-bench" \
    --host "${HOST}" \
    --port "${PORT}" \
    --clients "${CLIENTS}" \
    --messages "$(( MSGS / CLIENTS ))" \
    --payload-size "${PAYLOAD_SIZE}"
