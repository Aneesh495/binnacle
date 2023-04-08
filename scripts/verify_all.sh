#!/usr/bin/env bash
# Binnacle Full End-to-End Verification Pipeline
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

echo "============================================================"
echo "          BINNACLE MASTER VERIFICATION PIPELINE             "
echo "============================================================"

echo ""
echo ">>> Step 1: Enforcing zero em dashes across repository..."
make check-dashes

echo ""
echo ">>> Step 2: Compiling native C acceleration libraries..."
make build

echo ""
echo ">>> Step 3: Running native C acceleration unit tests..."
clang -Wall -Wextra -std=c11 \
    c_src/accel/varint_accel.c \
    c_src/accel/trie_accel.c \
    c_src/accel/test_accel.c \
    -o /tmp/binnacle_test_accel
/tmp/binnacle_test_accel
rm -f /tmp/binnacle_test_accel

echo ""
echo ">>> Step 4: Running 14 Unit Test Suites in Pike..."
make unit-tests

echo ""
echo ">>> Step 5: Running 5 Regression Bug Fix Suites in Pike..."
make regression-tests

echo ""
echo ">>> Step 6: Running 10 Real Network Integration Suites in Pike..."
make integration-tests

echo ""
echo "============================================================"
echo "   ALL TESTS AND VERIFICATION PASSES COMPLETED WITH 100%   "
echo "============================================================"
