# Kafka → Snowflake → MCP (Agent Tools) Project

This repo runs locally with Docker:
- Kafka + Zookeeper
- Kafka Connect (with Snowflake Sink connector JAR)
- A Python producer that generates fake `orders` + `payments` events
- An MCP server exposing tools to query Snowflake + inspect Kafka + check Kafka Connect health

## What you get
- **Kafka topics**: `orders`, `payments`
- **Snowflake RAW tables** loaded by Kafka connector:
  - `RT_INTEL.RAW.ORDERS_EVENTS`
  - `RT_INTEL.RAW.PAYMENTS_EVENTS`
- **CURATED tables** (typed/deduped):
  - `RT_INTEL.CURATED.ORDERS`
  - `RT_INTEL.CURATED.PAYMENTS`
- **Snowflake Streams/Task** to auto-run MERGEs every minute
- **MCP tools**:
  - `run_sql(query)` (read-only SELECT only)
  - `revenue_last_minutes(minutes)`
  - `kafka_end_offsets(topic)`
  - `kafka_consumer_lag(topic, group)`
  - `connector_health(connector_name=None)`

---

## Prereqs
- Docker Desktop
- A Snowflake account
- A Snowflake user configured for **key-pair authentication**

### RSA key pair (example)
Unencrypted PKCS#8 private key:
```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

1) In Snowflake, set the user public key (`RSA_PUBLIC_KEY`) using the **public key body** (remove header/footer).  
2) Copy the **private key body** (remove header/footer and newlines) into `.env` as `SNOWFLAKE_PRIVATE_KEY` (single line).

---

## Setup

### 1) Snowflake SQL
Run these scripts in Snowflake (in this order):
1. `snowflake/init.sql`
2. `snowflake/transforms.sql`
3. `snowflake/tasks.sql`  ✅ (creates streams + 1-minute task + resumes it)

> If your task does not run, ensure the role you’re using has TASK privileges and can use `WH_ETL`.

### 2) Configure env
```bash
cp .env.example .env
# edit .env with Snowflake values + private key body
```

### 3) Start everything
```bash
make up
```

### 4) Create topics + register connector
```bash
make topics
make connector
```

### 5) Validate
Kafka Connect:
- http://localhost:8083/connectors
- `make connector-status`

Snowflake:
- RAW tables should start filling
- CURATED tables should fill automatically (task runs every minute)

---

## Useful commands
```bash
make logs          # follow all logs
make connect-logs   # connect container logs
make mcp-logs       # mcp container logs
make down          # stop + remove volumes
```

---

## MCP usage
The MCP server runs in the `mcp` container. If your MCP client can run a stdio server command, you can point it to:

```bash
docker compose exec -T mcp python -m mcp_server.server
```

---

## Troubleshooting
- If connector registration fails, check Connect logs:
  ```bash
  make connect-logs
  ```
- If Snowflake python connector auth fails, re-check:
  - public key set on Snowflake user
  - `SNOWFLAKE_PRIVATE_KEY` is PKCS#8 body only, single line
