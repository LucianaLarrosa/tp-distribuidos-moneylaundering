SHELL := /bin/bash

GATEWAY_HOST ?=
GATEWAY_PORT ?= 5000
SERVER_HOST  ?= localhost
SERVER_PORT  ?= 5000
INPUT_CSV_TRANSACTIONS ?= ../dataset/HI-Small_Trans2.csv
INPUT_CSV_ACCOUNTS     ?= ../dataset/HI-Small_accounts.csv
BATCH_SIZE             ?= 1000

DEBUG_OUTPUT_DIR ?= ../tmp
POOL_SIZE        ?=

N_CLIENTS ?= 2

# Replica count for every scalable worker.
# gateway and low_amount_reducer stay fixed at one instance.
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

COMPOSE_FILE ?= docker-compose.yaml

DATASET_PATH ?= ./data
EXPECTED_DIR ?= ./output_expected
OUTPUT_DIR   ?= ./output

COMPOSE_ARGS = \
	--replicas                    $(REPLICAS) \
	--clients                     $(N_CLIENTS) \
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
	--output                      $(COMPOSE_FILE)

.PHONY: compose build up down logs clean gateway client clients verify clean-tmp build-expected wait-clients diff-output test

gateway:
	cd src && GATEWAY_HOST="$(GATEWAY_HOST)" GATEWAY_PORT=$(GATEWAY_PORT) \
	DEBUG_OUTPUT_DIR="$(DEBUG_OUTPUT_DIR)" $(if $(POOL_SIZE),POOL_SIZE=$(POOL_SIZE),) \
	python3 -m gateway.main

client:
	cd src && SERVER_HOST=$(SERVER_HOST) SERVER_PORT=$(SERVER_PORT) \
	INPUT_CSV_TRANSACTIONS=$(INPUT_CSV_TRANSACTIONS) INPUT_CSV_ACCOUNTS=$(INPUT_CSV_ACCOUNTS) \
	BATCH_SIZE=$(BATCH_SIZE) \
	python3 -m client.main

clients: clean-tmp
	@for i in $$(seq 1 $(N_CLIENTS)); do \
		$(MAKE) client & \
	done; wait

clean-tmp:
	rm -f tmp/gateway_received_*.csv

verify:
	@for f in tmp/gateway_received_transactions_*.csv; do \
		if diff -q <(tail -n +2 dataset/HI-Small_Trans2.csv) $$f >/dev/null; then \
			echo "✓ $$f matches transactions dataset"; \
		else \
			echo "✗ $$f differs from transactions dataset"; \
		fi; \
	done; \
	for f in tmp/gateway_received_accounts_*.csv; do \
		if diff -q <(tail -n +2 dataset/HI-Small_accounts.csv) $$f >/dev/null; then \
			echo "✓ $$f matches accounts dataset"; \
		else \
			echo "✗ $$f differs from accounts dataset"; \
		fi; \
	done
	@$(MAKE) clean-tmp

compose:
	python3 compose_generator.py $(COMPOSE_ARGS)

build: compose
	docker compose -f $(COMPOSE_FILE) build

up: compose
	DATASET_PATH=$(DATASET_PATH) docker compose -f $(COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) down -v

logs:
	docker compose -f $(COMPOSE_FILE) logs -f

clean:
	docker compose -f $(COMPOSE_FILE) down -v --rmi local
	rm -f $(COMPOSE_FILE)

build-expected:
	DATASET_PATH=$(DATASET_PATH) EXPECTED_DIR=$(EXPECTED_DIR) python3 build_expected.py

wait-clients:
	@client_names=""; \
	for i in $$(seq 1 $(N_CLIENTS)); do client_names="$$client_names client_$$i"; done; \
	docker container wait $$client_names

diff-output:
	@mismatch=0; \
	for query_number in 1 2 3 4 5; do \
		for i in $$(seq 1 $(N_CLIENTS)); do \
			output_file="$(OUTPUT_DIR)/q$${query_number}_client_$${i}.csv"; \
			expected_file="$(EXPECTED_DIR)/q$${query_number}_expected.csv"; \
			if diff <(sort "$$output_file") <(sort "$$expected_file") > /dev/null 2>&1; then \
				printf "✓ Q%s client %s: OK\n" "$$query_number" "$$i"; \
			else \
				printf "✗ Q%s client %s: MISMATCH\n" "$$query_number" "$$i"; \
				diff <(sort "$$output_file") <(sort "$$expected_file") | head -20; \
				mismatch=1; \
			fi; \
		done; \
	done; \
	[ $$mismatch -eq 0 ]

test: up wait-clients build-expected diff-output
