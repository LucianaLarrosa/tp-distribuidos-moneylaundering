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

DATASET_DIR       ?= ./data
TRANSACTIONS_FILE ?= HI-Small_Trans.csv
ACCOUNTS_FILE     ?= HI-Small_accounts.csv
OUTPUT_DIR        ?= ./output

EXPECTED_DIR        ?= ./expected_output
SIZE ?= $(patsubst HI-%_Trans.csv,%,$(TRANSACTIONS_FILE))
EXPECTED_SIZE_DIR := $(EXPECTED_DIR)/$(SIZE)
PANDAS_EXPECTED_DIR ?= ./pandas_expected_output

PROTECTED_PREFIXES ?= rabbitmq proxy client
CHAOS_INTERVAL            ?= 30
CHAOS_KILLS_PER_ROUND     ?= 3
CHAOS_WATCHDOG_FLOOR      ?= 1
CHAOS_INJECT_START_ROUND  ?= 3
CHAOS_INJECT_CLIENT_COUNT ?= 3
CHAOS_INJECT_DATASET_SIZE ?= Small
CHAOS_REF_CLIENT          ?= client_1
CHAOS_CLIENTS_FILE        ?= .chaos_clients

VOLUME_TAIL  ?= 20
VOLUME_WIDTH ?= 200

STOP_TIMEOUT ?= 60

.PHONY: all chaos-all chaos-cli-all compose proto build up down logs remove-output remove-all clean clean-all chaos-kill chaos-kill-all chaos-inject-client chaos-monkey-round chaos-monkey chaos-monkey-cli volume-view volume-cli wait-clients wait-dyn-clients build-expected check-client verify-output output-test chaos-output-test chaos-cli-output-test

all: compose build output-test

chaos-all: compose build chaos-output-test

chaos-cli-all: compose build chaos-cli-output-test

compose:
	python3 compose_generator.py $(COMPOSE_ARGS)

proto:
	docker run --rm -v $(CURDIR):/w -w /w python:3.11-slim sh -c "\
		pip install grpcio-tools==1.80.0 -q && \
		python -m grpc_tools.protoc \
			-I src \
			--python_out=src \
			src/common/communication/protocol/proto/common_protobuf.proto \
			src/common/communication/protocol/proto/internal.proto \
			src/common/communication/protocol/proto/external.proto \
			src/common/communication/protocol/proto/health.proto \
			src/common/communication/protocol/proto/election.proto"

build: proto
	docker compose -f $(COMPOSE_FILE) build

up: remove-output
	DATASET_DIR=$(DATASET_DIR) OUTPUT_DIR=$(OUTPUT_DIR) TRANSACTIONS_FILE=$(TRANSACTIONS_FILE) ACCOUNTS_FILE=$(ACCOUNTS_FILE) docker compose -f $(COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) down -v -t $(STOP_TIMEOUT)
	@dyn=$$(docker ps -a --format '{{.Names}}' | grep '^client_dyn_' || true); \
	if [ -n "$$dyn" ]; then \
		echo "$$dyn" | xargs docker rm -f >/dev/null; \
	fi

logs:
	@if [ -z "$(SERVICE)" ]; then \
		docker compose -f $(COMPOSE_FILE) logs -f; \
	else \
		docker compose -f $(COMPOSE_FILE) logs -f $(SERVICE); \
	fi

remove-output:
	rm -f $(CHAOS_CLIENTS_FILE)
	rm -rf $(OUTPUT_DIR)

remove-all: remove-output
	rm -rf $(EXPECTED_DIR) $(PANDAS_EXPECTED_DIR)
	rm -f $(COMPOSE_FILE)

clean:
	docker compose -f $(COMPOSE_FILE) down -v --rmi local
	$(MAKE) --no-print-directory remove-all

clean-all:
	docker compose -f $(COMPOSE_FILE) down -v --rmi local --remove-orphans
	docker system prune -f
	$(MAKE) --no-print-directory remove-all

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

chaos-kill-all:
	@protected_regex="^($$(echo $(PROTECTED_PREFIXES) | tr ' ' '|'))"; \
	running=$$(docker ps --format '{{.Names}}' | grep -vE "$$protected_regex"); \
	running_watchdogs=$$(echo "$$running" | grep -cE '^watchdog_[0-9]+$$'); \
	if [ "$$running_watchdogs" -le $(CHAOS_WATCHDOG_FLOOR) ]; then \
		pool=$$(echo "$$running" | grep -vE '^watchdog_'); \
	else \
		to_kill=$$((running_watchdogs - $(CHAOS_WATCHDOG_FLOOR))); \
		pool=$$(echo "$$running" | grep -vE '^watchdog_'; echo "$$running" | grep -E '^watchdog_' | head -n "$$to_kill"); \
	fi; \
	if [ -z "$$pool" ]; then \
		printf "$(RED)No victims available$(RESET)\n"; \
	else \
		echo "$$pool" | while read -r target; do \
			[ -z "$$target" ] && continue; \
			printf "$(RED)Killing $$target...$(RESET)\n"; \
			docker kill "$$target"; \
		done; \
	fi

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
		-e CLIENT_NAME="dyn_$(CHAOS_INJECT_IDX)" \
		"$$img" >/dev/null 2>&1; then \
		printf "$(LIME)  ＋ Inyected %s (%s)$(RESET)\n" "$$name" "$$trans"; \
		echo "dyn_$(CHAOS_INJECT_IDX):$(CHAOS_INJECT_DATASET_SIZE)" >> "$(CHAOS_CLIENTS_FILE)"; \
	else \
		printf "$(RED)  ✗ Failed to inject %s$(RESET)\n" "$$name"; \
	fi

chaos-monkey-round:
	@printf "$(CYAN)Round: killing $(CHAOS_KILLS_PER_ROUND) node(s)$(RESET)\n"; \
	for i in $$(seq 1 $(CHAOS_KILLS_PER_ROUND)); do \
		$(MAKE) --no-print-directory chaos-kill || true; \
	done

chaos-monkey:
	@printf "$(LIME)Chaos Monkey: %s kill(s)/round every %ss, keeping %s watchdog(s) alive; injecting %s client(s) in batches of %s starting from round %s$(RESET)\n" \
    "$(CHAOS_KILLS_PER_ROUND)" "$(CHAOS_INTERVAL)" "$(CHAOS_WATCHDOG_FLOOR)" \
    "$(CHAOS_INJECT_CLIENT_COUNT)" "$(CHAOS_INJECT_DATASET_SIZE)" "$(CHAOS_INJECT_START_ROUND)"; \
	round=0; injected=0; \
	while [ -n "$$(docker ps --format '{{.Names}}' | grep '^client_')" ]; do \
		round=$$((round + 1)); \
		$(MAKE) --no-print-directory chaos-monkey-round || true; \
		if [ "$$round" -ge $(CHAOS_INJECT_START_ROUND) ] && [ "$$injected" -lt $(CHAOS_INJECT_CLIENT_COUNT) ]; then \
			$(MAKE) --no-print-directory chaos-inject-client CHAOS_INJECT_IDX="$$injected" || true; \
			injected=$$((injected + 1)); \
		fi; \
		sleep $(CHAOS_INTERVAL); \
	done; \
	printf "$(LIME)Chaos Monkey finished: %s round(s); %s client(s) injected$(RESET)\n" "$$round" "$$injected"

chaos-monkey-cli:
	@injected=0; \
	while [ -n "$$(docker ps --format '{{.Names}}' | grep '^client_')" ]; do \
		protected_regex="^($$(echo $(PROTECTED_PREFIXES) | tr ' ' '|'))"; \
		running=$$(docker ps --format '{{.Names}}' | grep -vE "$$protected_regex" | tr '\n' ' '); \
		printf "\n$(CYAN)Chaos Monkey CLI$(RESET)\n"; \
		printf "Active killable nodes: %s\n" "$${running:-(none)}"; \
		printf "  1) Run round (%s kills, floor %s watchdog)\n" "$(CHAOS_KILLS_PER_ROUND)" "$(CHAOS_WATCHDOG_FLOOR)"; \
		printf "  2) Inject dynamic client\n"; \
		printf "  3) Kill all nodes\n"; \
		printf "  4) Kill specific node\n"; \
		printf "  q) Quit\n"; \
		printf "Option: "; \
		read -r option </dev/tty; \
		case "$$option" in \
			1) $(MAKE) --no-print-directory chaos-monkey-round ;; \
			2) $(MAKE) --no-print-directory chaos-inject-client CHAOS_INJECT_IDX="$$injected"; injected=$$((injected + 1)) ;; \
			3) $(MAKE) --no-print-directory chaos-kill-all ;; \
			4) \
				printf "Node name: "; \
				read -r node </dev/tty; \
				$(MAKE) --no-print-directory chaos-kill NODE="$$node" ;; \
			q|Q) break ;; \
			*) printf "$(RED)Invalid option: %s$(RESET)\n" "$$option" ;; \
		esac; \
	done; \
	printf "$(LIME)CLI done. Dynamic clients injected: %s$(RESET)\n" "$$injected"

volume-view:
	@if [ -z "$(NODE)" ]; then \
		printf "$(RED)Pasá NODE=<nombre> (ej: NODE=bank_max_aggregator_0)$(RESET)\n"; exit 1; \
	fi; \
	mounts=$$(docker inspect "$(NODE)" -f '{{range .Mounts}}{{if eq .Type "volume"}}{{.Name}}::{{.Destination}} {{end}}{{end}}' 2>/dev/null); \
	if [ -z "$$mounts" ]; then \
		printf "$(RED)$(NODE): sin volúmenes nombrados (o no existe el contenedor)$(RESET)\n"; exit 0; \
	fi; \
	for m in $$mounts; do \
		name=$${m%%::*}; dest=$${m##*::}; \
		printf "$(CYAN)══ %s  →  volume %s$(RESET)\n" "$$dest" "$$name"; \
		docker run --rm -v "$$name":/v alpine sh -c '\
			ls -la /v; \
			find /v -type f | while read -r f; do \
				printf "\n$(LIME)--- %s (%s líneas, %s bytes) — últimas $(VOLUME_TAIL) ---$(RESET)\n" "$$f" "$$(wc -l < "$$f")" "$$(wc -c < "$$f")"; \
				if [ "$(VOLUME_WIDTH)" -gt 0 ]; then tail -n $(VOLUME_TAIL) "$$f" | cut -c1-$(VOLUME_WIDTH); \
				else tail -n $(VOLUME_TAIL) "$$f"; fi; \
			done' 2>/dev/null; \
	done

volume-cli:
	@while true; do \
		printf "\n$(CYAN)Volume Viewer$(RESET)\n"; \
		nodes=$$(docker ps -a --format '{{.Names}}' | while read -r n; do \
			v=$$(docker inspect "$$n" -f '{{range .Mounts}}{{if eq .Type "volume"}}x{{end}}{{end}}' 2>/dev/null); \
			[ -n "$$v" ] && echo "$$n"; \
		done); \
		if [ -z "$$nodes" ]; then printf "$(RED)No hay nodos con volúmenes$(RESET)\n"; break; fi; \
		i=0; \
		for n in $$nodes; do i=$$((i+1)); printf "  %s) %s\n" "$$i" "$$n"; done; \
		printf "  q) Quit\nNodo (número o nombre): "; \
		read -r sel </dev/tty; \
		case "$$sel" in q|Q) break ;; esac; \
		if echo "$$sel" | grep -qE '^[0-9]+$$'; then \
			node=$$(echo "$$nodes" | sed -n "$${sel}p"); \
		else node="$$sel"; fi; \
		if [ -z "$$node" ]; then printf "$(RED)Selección inválida$(RESET)\n"; continue; fi; \
		$(MAKE) --no-print-directory volume-view NODE="$$node"; \
	done

wait-clients:
	@client_names=""; \
	for i in $$(seq 1 $(N_CLIENTS)); do client_names="$$client_names client_$$i"; done; \
	docker container wait $$client_names

wait-dyn-clients:
	@if [ -s "$(CHAOS_CLIENTS_FILE)" ]; then \
		dyn_names=""; \
		while IFS=: read -r cname dataset; do \
			dyn_names="$$dyn_names client_$$cname"; \
		done < "$(CHAOS_CLIENTS_FILE)"; \
		if [ -n "$$dyn_names" ]; then \
			docker container wait $$dyn_names; \
		fi; \
	fi

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

check-client:
	@mismatch=0; \
	for query_number in 1 2 3 4 5; do \
		output_file="$(OUTPUT_DIR)/q$${query_number}_client_$(CLIENT_NAME).csv"; \
		expected_file="$(CLIENT_EXPECTED_DIR)/q$${query_number}_expected.csv"; \
		if [ ! -f "$$output_file" ]; then \
			printf "$(RED)✗ Q%s client $(CLIENT_NAME): NO OUTPUT$(RESET)\n" "$$query_number"; \
			mismatch=1; \
		elif diff <(LC_ALL=C sort "$$output_file") <(LC_ALL=C sort "$$expected_file") > /dev/null 2>&1; then \
			printf "$(LIME)✓ Q%s client $(CLIENT_NAME): OK$(RESET)\n" "$$query_number"; \
		else \
			printf "$(RED)✗ Q%s client $(CLIENT_NAME): MISMATCH$(RESET)\n" "$$query_number"; \
			diff <(LC_ALL=C sort "$$output_file") <(LC_ALL=C sort "$$expected_file") | head -20; \
			mismatch=1; \
		fi; \
	done; \
	[ $$mismatch -eq 0 ]

verify-output:
	@mismatch=0; \
	for i in $$(seq 1 $(N_CLIENTS)); do \
		$(MAKE) --no-print-directory check-client CLIENT_NAME="$$i" CLIENT_EXPECTED_DIR="$(EXPECTED_SIZE_DIR)" || mismatch=1; \
	done; \
	if [ -s "$(CHAOS_CLIENTS_FILE)" ]; then \
		while IFS=: read -r cname dataset; do \
			$(MAKE) --no-print-directory check-client CLIENT_NAME="$$cname" CLIENT_EXPECTED_DIR="$(EXPECTED_DIR)/$$dataset" || mismatch=1; \
		done < "$(CHAOS_CLIENTS_FILE)"; \
	fi; \
	if [ $$mismatch -eq 0 ]; then \
		printf "$(LIME)Output test passed$(RESET)\n"; \
	else \
		printf "$(RED)Output test failed$(RESET)\n"; \
	fi; \
	[ $$mismatch -eq 0 ]

output-test: up wait-clients build-expected verify-output down

chaos-output-test: up build-expected chaos-monkey wait-clients wait-dyn-clients verify-output down

chaos-cli-output-test: up build-expected chaos-monkey-cli wait-clients wait-dyn-clients verify-output down
