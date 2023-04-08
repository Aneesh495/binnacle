#!/usr/bin/env bash
# Binnacle Autonomous Vehicle Fleet Telemetry Simulator Launcher
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

export PYTHONPATH="${ROOT_DIR}/python:${PYTHONPATH:-}"

HOST="${1:-127.0.0.1}"
PORT="${2:-1883}"
FLEET_SIZE="${3:-50}"
DURATION="${4:-10}"

echo "Starting Binnacle Fleet Telemetry Simulation..."
exec python3 "${ROOT_DIR}/python/simulator/fleet_telemetry_sim.py" \
    --host "${HOST}" \
    --port "${PORT}" \
    --fleet-size "${FLEET_SIZE}" \
    --duration "${DURATION}"
