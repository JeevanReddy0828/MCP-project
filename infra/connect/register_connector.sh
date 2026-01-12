#!/usr/bin/env bash
set -euo pipefail

if [ ! -f .env ]; then
  echo "Missing .env file. Copy .env.example -> .env and fill values."
  exit 1
fi

set -a
source .env
set +a

TEMPLATE="infra/connect/snowflake-sink.template.json"
RENDERED="/tmp/snowflake-sink.json"

if ! command -v envsubst >/dev/null 2>&1; then
  echo "envsubst not found. Install gettext (or run via WSL) to use this script."
  exit 1
fi

envsubst < "$TEMPLATE" > "$RENDERED"

echo "Registering connector with Kafka Connect..."
curl -sS -X PUT   -H "Content-Type: application/json"   --data @"$RENDERED"   http://localhost:8083/connectors/snowflake-sink-rtintel/config | jq .

echo ""
echo "Done. Check connector status:"
echo "curl -s http://localhost:8083/connectors/snowflake-sink-rtintel/status | jq ."
