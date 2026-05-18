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

.PHONY: compose build up down logs clean gateway client clients verify clean-tmp

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
	python compose_generator.py > docker-compose.yaml

build: compose
	docker compose build

up: compose
	docker compose up -d

down:
	docker compose down -v

logs:
	docker compose logs -f

clean:
	docker compose down -v --rmi local
	rm -f docker-compose.yaml
