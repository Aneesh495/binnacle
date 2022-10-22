# Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
# Root Build and Automation Makefile

PIKE ?= pike
CC ?= clang
CFLAGS ?= -O3 -Wall -Wextra -pedantic -std=c11
MODULE_PATH := $(CURDIR)/src
BIN_DIR := $(CURDIR)/bin
SRC_DIR := $(CURDIR)/src
C_SRC_DIR := $(CURDIR)/c_src
TEST_DIR := $(CURDIR)/tests

.PHONY: all build clean test unit-tests integration-tests regression-tests run bench verify lint check-dashes

all: build

build:
	@mkdir -p $(BIN_DIR)
	@echo "Building native C acceleration libraries..."
	@if [ -d "$(C_SRC_DIR)/bench" ]; then \
		$(CC) $(CFLAGS) $(C_SRC_DIR)/bench/fast_bench.c $(C_SRC_DIR)/bench/hdr_histogram.c -o $(BIN_DIR)/binnacle-bench -lpthread; \
	fi
	@echo "Binnacle build complete."

test:
	@echo "Running all Binnacle test suites in Pike..."
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/run_all_tests.pike

unit-tests:
	@echo "Running unit test suites..."
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/unit/test_varint.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/unit/test_properties.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/unit/test_packet_codec.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/unit/test_topic_trie.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/unit/test_shared_subs.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/unit/test_qos_handler.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/unit/test_session_lifecycle.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/unit/test_offline_queue.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/unit/test_wal_storage.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/unit/test_rule_engine.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/unit/test_websocket_framing.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/unit/test_metrics_registry.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/unit/test_rate_limiter.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/unit/test_config_parser.pike

integration-tests:
	@echo "Running integration test suites..."
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/integration/test_tcp_connect_and_ping.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/integration/test_pub_sub_qos0.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/integration/test_pub_sub_qos1.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/integration/test_pub_sub_qos2.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/integration/test_wildcard_routing.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/integration/test_retained_messages.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/integration/test_last_will.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/integration/test_topic_alias.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/integration/test_rest_api.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/integration/test_bridge_clustering.pike

regression-tests:
	@echo "Running regression test suites..."
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/regressions/test_bug_varint_overflow.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/regressions/test_bug_trie_trailing_slash.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/regressions/test_bug_qos2_interleaved_packet_ids.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/regressions/test_bug_session_expiry_cleanup.pike
	@$(PIKE) -M$(MODULE_PATH) $(TEST_DIR)/regressions/test_bug_websocket_masking.pike

run:
	@$(PIKE) -M$(MODULE_PATH) $(SRC_DIR)/bin/binnacle.pike --config binnacle.conf.example

bench: build
	@echo "Running Binnacle benchmarking harness..."
	@./scripts/bench_100k.sh

verify: test check-dashes
	@echo "Verification pass passed with zero errors."

check-dashes:
	@echo "Verifying zero em dashes across all files..."
	@if grep -rn "—" src docs tests c_src python scripts Makefile README.md 2>/dev/null; then \
		echo "ERROR: Em dashes detected! Failing verification."; \
		exit 1; \
	else \
		echo "Em dash check clean: zero em dashes found."; \
	fi

clean:
	@rm -rf $(BIN_DIR) build data *.wal *.log *.snap
	@echo "Cleaned build and runtime artifacts."
