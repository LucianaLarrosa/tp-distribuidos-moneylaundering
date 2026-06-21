SHELL := /bin/bash

LIME  := \033[38;2;138;206;0m
RED   := \033[31m
CYAN  := \033[36m
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
WATCHDOGS                  ?= $(REPLICAS)

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

PROTECTED_PREFIXES ?= rabbitmq gateway proxy client

COMPOSE_ARGS = \
	--clients                     $(N_CLIENTS) \
	--gateways                    $(N_GATEWAYS) \
	--protected-prefixes          "$(PROTECTED_PREFIXES)" \
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
	--watchdogs                   $(WATCHDOGS) \
	--output-file                 $(COMPOSE_FILE)

CHAOS_INTERVAL            ?= 30
CHAOS_KILLS_PER_ROUND     ?= 3
CHAOS_WATCHDOG_FLOOR      ?= 1
CHAOS_INJECT_START_ROUND  ?= 3
CHAOS_INJECT_CLIENT_COUNT ?= 3
CHAOS_INJECT_DATASET_SIZE ?= Small
CHAOS_REF_CLIENT          ?= client_1

.PHONY: compose build up down logs remove-output clean clean-all build-expected verify-output output-test chaos-kill chaos-check-client chaos-inject-client chaos-monkey chaos-output-test chaos-all reaper-kill-clients reaper-test

all: compose build output-test

chaos-all: compose build chaos-output-test

proto:
	docker run --rm -v $(PWD):/w -w /w python:3.11-slim sh -c "\
		pip install grpcio-tools==1.80.0 -q && \
		python -m grpc_tools.protoc \
			-I src \
			--python_out=src \
			src/common/protocol/common_protobuf/common_protobuf.proto \
			src/common/protocol/internal/internal.proto \
			src/common/protocol/external/external.proto \
			src/common/protocol/health/health.proto \
			src/common/protocol/election/election.proto"

compose:
	python3 compose_generator.py $(COMPOSE_ARGS)

build: proto
	docker compose -f $(COMPOSE_FILE) build

up:
	DATASET_DIR=$(DATASET_DIR) OUTPUT_DIR=$(OUTPUT_DIR) TRANSACTIONS_FILE=$(TRANSACTIONS_FILE) ACCOUNTS_FILE=$(ACCOUNTS_FILE) docker compose -f $(COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) down -v

logs:
	@if [ -z "$(SERVICE)" ]; then \
		docker compose -f $(COMPOSE_FILE) logs -f; \
	else \
		docker compose -f $(COMPOSE_FILE) logs -f $(SERVICE); \
	fi

chaos-check-client:
	@if [ -z "$$(docker ps --format '{{.Names}}' | grep '^client_')" ]; then \
		printf "$(RED)No clients running. Run 'make up' first.$(RESET)\n"; \
		exit 1; \
	fi

chaos-kill:
	@protected_regex="^($$(echo $(PROTECTED_PREFIXES) | tr ' ' '|'))"; \
	running=$$(docker ps --format '{{.Names}}' | grep -vE "$$protected_regex"); \
	running_watchdogs=$$(echo "$$running" | grep -cE '^watchdog_[0-9]+$$'); \
	if [ -n "$(NODE)" ]; then \
		case "$(NODE)" in watchdog_*) \
			if [ "$$running_watchdogs" -le $(CHAOS_WATCHDOG_FLOOR) ]; then \
				printf "$(RED)Skipping $(NODE) because it's the last watchdog alive$(RESET)\n"; \
				exit 0; \
			fi ;; \
		esac; \
		target="$(NODE)"; \
	else \
		if [ "$$running_watchdogs" -le $(CHAOS_WATCHDOG_FLOOR) ]; then \
			pool=$$(echo "$$running" | grep -vE '^watchdog_'); \
		else \
			pool="$$running"; \
		fi; \
		if [ -z "$$pool" ]; then \
			printf "$(RED)No victims available$(RESET)\n"; \
			exit 0; \
		fi; \
		target=$$(echo "$$pool" | shuf -n 1); \
	fi; \
	printf "$(RED)Killing $$target...$(RESET)\n"; \
	docker kill "$$target"

chaos-inject-client:
	@trans="HI-$(CHAOS_INJECT_DATASET_SIZE)_Trans.csv"; \
	accounts="HI-$(CHAOS_INJECT_DATASET_SIZE)_accounts.csv"; \
	name="client_dyn_$(CHAOS_INJECT_IDX)"; \
	img=$$(docker inspect $(CHAOS_REF_CLIENT) -f '{{.Config.Image}}' 2>/dev/null); \
	net=$$(docker inspect proxy -f '{{range $$n,$$_ := .NetworkSettings.Networks}}{{$$n}}{{end}}' 2>/dev/null); \
	docker rm -f "$$name" >/dev/null 2>&1 || true; \
	if docker run -d --name "$$name" --network "$$net" \
		-v "$(abspath $(DATASET_DIR))":/data:ro \
		-v "$(abspath $(OUTPUT_DIR))":/output \
		-e PROXY_HOST=proxy -e PROXY_PORT=6000 \
		-e INPUT_CSV_TRANSACTIONS="/data/$$trans" \
		-e INPUT_CSV_ACCOUNTS="/data/$$accounts" \
		-e EXPECTED_QUERY_IDS=1,2,3,4,5 \
		-e OUTPUT_DIR=/output \
		-e CLIENT_ID="dyn_$(CHAOS_INJECT_IDX)" \
		"$$img" >/dev/null 2>&1; then \
		printf "$(LIME)  ＋ Inyected %s (%s)$(RESET)\n" "$$name" "$$trans"; \
	else \
		printf "$(RED)  ✗ Failed to inject %s$(RESET)\n" "$$name"; \
	fi

chaos-monkey: chaos-check-client
	@printf "$(LIME)Chaos Monkey: %s kill(s)/round every %ss, keeping %s watchdog(s) alive; injecting %s client(s) in batches of %s starting from round %s$(RESET)\n" \
    "$(CHAOS_KILLS_PER_ROUND)" "$(CHAOS_INTERVAL)" "$(CHAOS_WATCHDOG_FLOOR)" \
    "$(CHAOS_INJECT_CLIENT_COUNT)" "$(CHAOS_INJECT_DATASET_SIZE)" "$(CHAOS_INJECT_START_ROUND)"; \
	round=0; injected=0; \
	while [ -n "$$(docker ps --format '{{.Names}}' | grep '^client_')" ]; do \
		round=$$((round + 1)); \
		printf "$(CYAN)Round %s$(RESET)\n" "$$round"; \
		for i in $$(seq 1 $(CHAOS_KILLS_PER_ROUND)); do \
			$(MAKE) --no-print-directory chaos-kill || true; \
		done; \
		if [ "$$round" -ge $(CHAOS_INJECT_START_ROUND) ] && [ "$$injected" -lt $(CHAOS_INJECT_CLIENT_COUNT) ]; then \
			$(MAKE) --no-print-directory chaos-inject-client CHAOS_INJECT_IDX="$$injected" || true; \
			injected=$$((injected + 1)); \
		fi; \
		sleep $(CHAOS_INTERVAL); \
	done; \
	printf "$(LIME)Chaos Monkey finished: %s round(s); %s client(s) injected$(RESET)\n" "$$round" "$$injected"

REAPER_KEEP ?= client_1

reaper-kill-clients: chaos-check-client
	@victims=$$(docker ps --format '{{.Names}}' | grep '^client_' | grep -vx '$(REAPER_KEEP)'); \
	if [ -z "$$victims" ]; then \
		printf "$(RED)Need more than one running client (keeping $(REAPER_KEEP))$(RESET)\n"; \
		exit 0; \
	fi; \
	for v in $$victims; do \
		printf "$(RED)Killing $$v...$(RESET)\n"; \
		docker kill "$$v" >/dev/null; \
	done; \
	printf "$(LIME)Kept $(REAPER_KEEP) alive$(RESET)\n"

reaper-test: reaper-kill-clients
	@printf "$(CYAN)Watching reaper + cleanup prints (Ctrl-C to stop)...$(RESET)\n"
	@docker compose -f $(COMPOSE_FILE) logs -f 2>&1 | grep --line-buffered -iE "reaper|\[cleanup\]"

remove-output:
	rm -f $(COMPOSE_FILE)
	rm -rf $(OUTPUT_DIR) $(EXPECTED_DIR) $(PANDAS_EXPECTED_DIR)

clean:
	docker compose -f $(COMPOSE_FILE) down -v --rmi local
	$(MAKE) --no-print-directory remove-output

clean-all:
	docker compose -f $(COMPOSE_FILE) down -v --rmi local --remove-orphans
	docker system prune -f
	$(MAKE) --no-print-directory remove-output

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

chaos-output-test: up build-expected chaos-monkey wait-clients verify-output down
