SHELL := /bin/bash

N_CLIENTS ?= 2
GATEWAYS  ?= 1

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

DATASET_DIR ?= ./data
EXPECTED_DIR ?= ./output_expected
OUTPUT_DIR   ?= ./output

COMPOSE_ARGS = \
	--clients                     $(N_CLIENTS) \
	--gateways                    $(GATEWAYS) \
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
	--output-file                 $(COMPOSE_FILE)

.PHONY: all compose build up down logs clean build-expected wait-clients diff-output test

all: build test

compose:
	python3 compose_generator.py $(COMPOSE_ARGS)

build: compose
	docker compose -f $(COMPOSE_FILE) build

up: compose
	DATASET_DIR=$(DATASET_DIR) OUTPUT_DIR=$(OUTPUT_DIR) docker compose -f $(COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) down -v

logs:
	docker compose -f $(COMPOSE_FILE) logs -f

clean:
	docker compose -f $(COMPOSE_FILE) down -v --rmi local
	rm -f $(COMPOSE_FILE)
	rm -rf $(OUTPUT_DIR) $(EXPECTED_DIR)

build-expected:
	DATASET_DIR=$(DATASET_DIR) EXPECTED_DIR=$(EXPECTED_DIR) python3 build_expected.py

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
