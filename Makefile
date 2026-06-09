SHELL := /bin/bash

LIME  := \033[38;2;138;206;0m
RED   := \033[31m
RESET := \033[0m

N_CLIENTS ?= 2
N_GATEWAYS ?= 2

REPLICAS ?= 3
TRANSACTIONS_FIELD_MAPPERS ?= $(REPLICAS)
ACCOUNTS_FIELD_MAPPERS     ?= $(REPLICAS)
DATE_FILTERS               ?= $(REPLICAS)
PAYMENT_FORMAT_FILTERS     ?= $(REPLICAS)
CURRENCY_MAPPERS           ?= $(REPLICAS)
LOW_AMOUNT_AGGREGATORS     ?= $(REPLICAS)
LOW_AMOUNT_REDUCERS        ?= 1
BANK_MAX_AGGREGATORS       ?= $(REPLICAS)
BANK_MAX_REDUCERS          ?= $(REPLICAS)
BANK_MAPPERS               ?= $(REPLICAS)
AMOUNT_FILTERS             ?= $(REPLICAS)
PAYMENT_FORMAT_AGGREGATORS ?= $(REPLICAS)
PAYMENT_FORMAT_REDUCERS    ?= $(REPLICAS)
ANOMALY_FILTERS            ?= $(REPLICAS)
BIDIRECTIONAL_SHARDERS     ?= $(REPLICAS)
ACCOUNT_FREQUENCY_FILTERS  ?= $(REPLICAS)
PATH_MAPPERS               ?= $(REPLICAS)
PATH_FREQUENCY_FILTERS     ?= $(REPLICAS)
DUPLICATE_ACCOUNT_FILTERS  ?= $(REPLICAS)

COMPOSE_FILE ?= docker-compose.yaml

DATASET_DIR       ?= ./data
TRANSACTIONS_FILE ?= HI-Small_Trans.csv
ACCOUNTS_FILE     ?= HI-Small_accounts.csv
OUTPUT_DIR        ?= ./output

EXPECTED_DIR        ?= ./expected_output
PANDAS_EXPECTED_DIR ?= ./pandas_expected_output

# Dataset size (Small/Medium/Large) derived from TRANSACTIONS_FILE (e.g. HI-Medium_Trans.csv -> Medium).
# Expected outputs are cached per size under $(EXPECTED_DIR)/$(SIZE).
SIZE              := $(patsubst HI-%_Trans.csv,%,$(TRANSACTIONS_FILE))
EXPECTED_SIZE_DIR := $(EXPECTED_DIR)/$(SIZE)

# Adjust this to ensure the containers have:
# 1. Enough time to fully start before the tests run
# 2. Enough time to gracefully stop running processes when interrupted
SLEEP_TIME ?= 30

COMPOSE_ARGS = \
	--clients                     $(N_CLIENTS) \
	--gateways                    $(N_GATEWAYS) \
	--transactions-field-mappers  $(TRANSACTIONS_FIELD_MAPPERS) \
	--accounts-field-mappers      $(ACCOUNTS_FIELD_MAPPERS) \
	--date-filters                $(DATE_FILTERS) \
	--payment-format-filters      $(PAYMENT_FORMAT_FILTERS) \
	--currency-mappers            $(CURRENCY_MAPPERS) \
	--low-amount-aggregators      $(LOW_AMOUNT_AGGREGATORS) \
	--low-amount-reducers         $(LOW_AMOUNT_REDUCERS) \
	--bank-max-aggregators        $(BANK_MAX_AGGREGATORS) \
	--bank-max-reducers           $(BANK_MAX_REDUCERS) \
	--bank-mappers                $(BANK_MAPPERS) \
	--amount-filters              $(AMOUNT_FILTERS) \
	--payment-format-aggregators  $(PAYMENT_FORMAT_AGGREGATORS) \
	--payment-format-reducers     $(PAYMENT_FORMAT_REDUCERS) \
	--anomaly-filters             $(ANOMALY_FILTERS) \
	--bidirectional-sharders      $(BIDIRECTIONAL_SHARDERS) \
	--account-frequency-filters   $(ACCOUNT_FREQUENCY_FILTERS) \
	--path-mappers                $(PATH_MAPPERS) \
	--path-frequency-filters      $(PATH_FREQUENCY_FILTERS) \
	--duplicate-account-filters   $(DUPLICATE_ACCOUNT_FILTERS) \
	--output-file                 $(COMPOSE_FILE)

.PHONY: compose build up down logs remove-output clean clean-all build-expected verify-output output-test up-and-stop verify-shutdown verify-exit-codes exit-test

all: compose build
	$(MAKE) output-test
	$(MAKE) exit-test

proto:
	docker run --rm -v $(PWD):/w -w /w python:3.11-slim sh -c "\
		pip install grpcio-tools==1.80.0 -q && \
		python -m grpc_tools.protoc \
			-I src \
			--python_out=src \
			src/common/protocol/common_protobuf/common_protobuf.proto \
			src/common/protocol/internal/internal.proto \
			src/common/protocol/external/external.proto \
			src/common/protocol/health/health.proto"

compose:
	python3 compose_generator.py $(COMPOSE_ARGS)

build: proto
	docker compose -f $(COMPOSE_FILE) build

up:
	DATASET_DIR=$(DATASET_DIR) OUTPUT_DIR=$(OUTPUT_DIR) TRANSACTIONS_FILE=$(TRANSACTIONS_FILE) ACCOUNTS_FILE=$(ACCOUNTS_FILE) docker compose -f $(COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) down -v

logs:
	docker compose -f $(COMPOSE_FILE) logs -f

remove-output:
	rm -f $(COMPOSE_FILE)
	rm -rf $(OUTPUT_DIR) $(EXPECTED_DIR) $(PANDAS_EXPECTED_DIR)

clean:
	docker compose -f $(COMPOSE_FILE) down -v --rmi local
	$(MAKE) remove-output

clean-all:
	docker compose -f $(COMPOSE_FILE) down -v --rmi local --remove-orphans
	docker system prune -f
	$(MAKE) remove-output

wait-clients:
	@client_names=""; \
	for i in $$(seq 1 $(N_CLIENTS)); do client_names="$$client_names client_$$i"; done; \
	docker container wait $$client_names

build-expected:
	@if [ -f "$(EXPECTED_SIZE_DIR)/q1_expected.csv" ] \
		&& [ -f "$(EXPECTED_SIZE_DIR)/q2_expected.csv" ] \
		&& [ -f "$(EXPECTED_SIZE_DIR)/q3_expected.csv" ] \
		&& [ -f "$(EXPECTED_SIZE_DIR)/q4_expected.csv" ] \
		&& [ -f "$(EXPECTED_SIZE_DIR)/q5_expected.csv" ]; then \
		printf "$(LIME)Using cached expected output for size '$(SIZE)' ($(EXPECTED_SIZE_DIR))$(RESET)\n"; \
	else \
		printf "$(LIME)Building expected output for size '$(SIZE)' -> $(EXPECTED_SIZE_DIR)$(RESET)\n"; \
		DATASET_DIR=$(DATASET_DIR) EXPECTED_DIR=$(EXPECTED_SIZE_DIR) TRANSACTIONS_FILE=$(TRANSACTIONS_FILE) ACCOUNTS_FILE=$(ACCOUNTS_FILE) python3 build_expected.py; \
	fi

verify-output:
	@mismatch=0; \
	for query_number in 1 2 3 4 5; do \
		for i in $$(seq 1 $(N_CLIENTS)); do \
			output_file="$(OUTPUT_DIR)/q$${query_number}_client_$${i}.csv"; \
			expected_file="$(EXPECTED_SIZE_DIR)/q$${query_number}_expected.csv"; \
			if diff <(LC_ALL=C sort "$$output_file") <(LC_ALL=C sort "$$expected_file") > /dev/null 2>&1; then \
				printf "$(LIME)✓ Q%s client %s: OK$(RESET)\n" "$$query_number" "$$i"; \
			else \
				printf "$(RED)✗ Q%s client %s: MISMATCH$(RESET)\n" "$$query_number" "$$i"; \
				diff <(LC_ALL=C sort "$$output_file") <(LC_ALL=C sort "$$expected_file") | head -20; \
				mismatch=1; \
			fi; \
		done; \
	done; \
	if [ $$mismatch -eq 0 ]; then \
		printf "$(LIME)Output test passed$(RESET)\n"; \
	else \
		printf "$(RED)Output test failed$(RESET)\n"; \
	fi; \
	[ $$mismatch -eq 0 ]

output-test: up wait-clients build-expected verify-output down

up-and-stop: up
	sleep $(SLEEP_TIME)
	docker compose -f $(COMPOSE_FILE) stop --timeout 10

verify-shutdown:
	@all_shutdown=0; \
	for name in $$(docker compose -f $(COMPOSE_FILE) ps --all --format '{{.Name}}'); do \
		if [ "$$name" != "rabbitmq" ]; then \
			logs=$$(docker logs $$name 2>&1); \
			if echo "$$logs" | grep -Eq "Shutting down|Shutdown"; then \
				printf "$(LIME)✓ %-45s shutdown detected$(RESET)\n" "$$name"; \
			else \
				printf "$(RED)✗ %-45s missing shutdown log$(RESET)\n" "$$name"; \
				all_shutdown=1; \
			fi; \
		fi; \
	done; \
	[ $$all_shutdown -eq 0 ]

verify-exit-codes:
	@has_successful_exit=1; \
	for name in $$(docker compose -f $(COMPOSE_FILE) ps --all --format '{{.Name}}'); do \
		code=$$(docker inspect $$name --format='{{.State.ExitCode}}'); \
		if [ "$$code" = "0" ]; then \
			printf "$(LIME)✓ %-45s exit code 0$(RESET)\n" "$$name"; \
			has_successful_exit=0; \
		else \
			printf "$(RED)✗ %-45s exit code $$code$(RESET)\n" "$$name"; \
		fi; \
	done; \
	[ $$has_successful_exit -eq 0 ]

exit-test: up-and-stop
	@$(MAKE) verify-shutdown; all_shutdown=$$?; \
	$(MAKE) verify-exit-codes; has_successful_exit=$$?; \
	if [ $$all_shutdown -eq 0 ] && [ $$has_successful_exit -eq 0 ]; then \
		printf "$(LIME)Graceful shutdown test passed$(RESET)\n"; \
	else \
		printf "$(RED)Graceful shutdown test failed$(RESET)\n"; \
	fi; \
	$(MAKE) down; \
	[ $$all_shutdown -eq 0 ] && [ $$has_successful_exit -eq 0 ]
