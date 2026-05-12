SHELL := /bin/bash

GATEWAY_HOST ?=
GATEWAY_PORT ?= 5000
SERVER_HOST  ?= localhost
SERVER_PORT  ?= 5000
INPUT_CSV    ?= ../dataset/HI-Small_Trans2.csv
BATCH_SIZE   ?= 1000

DEBUG_OUTPUT_FILE ?= ../tmp/gateway_received.csv

.PHONY: compose build up down logs clean gateway client verify

gateway:
	cd src && GATEWAY_HOST="$(GATEWAY_HOST)" GATEWAY_PORT=$(GATEWAY_PORT) \
	DEBUG_OUTPUT_FILE="$(DEBUG_OUTPUT_FILE)" \
	python3 -m gateway.main

client:
	cd src && SERVER_HOST=$(SERVER_HOST) SERVER_PORT=$(SERVER_PORT) \
	INPUT_CSV=$(INPUT_CSV) BATCH_SIZE=$(BATCH_SIZE) \
	python3 -m client.main

verify:
	@diff <(tail -n +2 dataset/HI-Small_Trans2.csv) tmp/gateway_received.csv \
		&& echo "✓ Protocol verified: all transactions match" \
		|| echo "✗ Mismatch found"

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
