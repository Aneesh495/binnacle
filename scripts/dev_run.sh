#!/usr/bin/env bash
# Binnacle Developer Daemon Runner
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

echo "Starting Binnacle Broker Daemon in developer mode..."
exec pike -M"${ROOT_DIR}/src" src/bin/binnacle.pike --config binnacle.conf.example "$@"
