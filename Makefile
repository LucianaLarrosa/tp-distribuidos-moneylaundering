SHELL := /bin/bash

.PHONY: compose build up down logs clean

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
