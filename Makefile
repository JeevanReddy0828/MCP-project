SHELL := /bin/bash

.PHONY: up down build logs ps topics connector connector-status connect-logs mcp-logs

up:
	docker compose up -d --build

down:
	docker compose down -v

build:
	docker compose build

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=200

topics:
	bash infra/kafka/create-topics.sh

connector:
	bash infra/connect/register_connector.sh

connector-status:
	curl -s http://localhost:8083/connectors/snowflake-sink-rtintel/status | jq .

connect-logs:
	docker compose logs -f --tail=200 connect

mcp-logs:
	docker compose logs -f --tail=200 mcp
