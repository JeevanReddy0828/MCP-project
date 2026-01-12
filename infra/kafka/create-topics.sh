#!/usr/bin/env bash
set -euo pipefail

docker compose exec -T kafka bash -lc "
kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic orders --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic payments --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server kafka:29092 --list
"
