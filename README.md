# Kafka → Snowflake → MCP Pipeline

Real-time streaming ETL pipeline: a Python producer emits `orders` and `payments` events into Kafka, Kafka Connect sinks them into Snowflake RAW tables, a scheduled Snowflake MERGE task promotes data into typed CURATED tables every minute, and an MCP server exposes the pipeline state as queryable tools.

```
Producer → Kafka → Kafka Connect → Snowflake RAW → Streams → MERGE Task → CURATED → Analytics View
                                                                               ↑
                                                                           MCP Server
```

---

## Prerequisites

- Docker Desktop
- Snowflake account
- `curl`, `openssl` (`jq` optional — only used for pretty-printing connector status)

---

## Setup

### 1. Generate RSA key pair

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

In Snowflake, set the public key on your user (key body only, no header/footer lines):

```sql
ALTER USER <your_user> SET RSA_PUBLIC_KEY='<public key body>';
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env`. The critical field is `SNOWFLAKE_PRIVATE_KEY` — paste the private key body as a **single line with no header/footer and no newlines**.

### 3. Run Snowflake SQL scripts

Run each script in Snowflake in this order. In Snowflake Workspaces, run statements **one at a time** — the editor compiles the full batch before executing, so forward references (e.g. `CREATE SCHEMA` followed by `CREATE TABLE <that_schema>...`) will fail if run together.

```
snowflake/init.sql       -- database, schemas, RAW tables, views
snowflake/transforms.sql -- CURATED tables, analytics view
snowflake/tasks.sql      -- WH_ETL warehouse, streams, 1-minute MERGE task
```

### 4. Start the pipeline

```bash
make up          # build + start all containers
make topics      # create orders and payments Kafka topics
make connector   # register the Snowflake Sink connector
```

Verify the connector is running:

```bash
make connector-status
# or without jq:
curl -s http://localhost:8083/connectors/snowflake-sink-rtintel/status
```

Both tasks (one per topic group) should show `"state": "RUNNING"`.

---

## Validation

```sql
-- RAW tables fill immediately after connector starts
SELECT COUNT(*) FROM RT_INTEL.RAW.ORDERS_EVENTS;
SELECT COUNT(*) FROM RT_INTEL.RAW.PAYMENTS_EVENTS;

-- CURATED tables fill after the first 1-minute task fires
SELECT COUNT(*) FROM RT_INTEL.CURATED.ORDERS;
SELECT COUNT(*) FROM RT_INTEL.CURATED.PAYMENTS;

-- Revenue aggregated per minute (PAID orders only)
SELECT * FROM RT_INTEL.ANALYTICS.V_REVENUE_PER_MINUTE LIMIT 10;
```

---

## MCP Server

The MCP server exposes 5 tools over stdio:

| Tool | Description |
|---|---|
| `run_sql(query, limit)` | Read-only SELECT on Snowflake (SELECT only, DDL/DML blocked) |
| `revenue_last_minutes(minutes)` | Revenue from `V_REVENUE_PER_MINUTE` for the last N minutes |
| `kafka_end_offsets(topic)` | Latest partition offsets for a topic |
| `kafka_consumer_lag(topic, group)` | Consumer lag per partition + total |
| `connector_health(connector_name)` | Kafka Connect connector status |

To connect an MCP client to the server:

```bash
docker compose exec -T mcp python -m mcp_server.server
```

---

## Useful commands

```bash
make logs            # follow all container logs
make connect-logs    # Kafka Connect logs only
make mcp-logs        # MCP server logs only
make ps              # container status
make down            # stop everything and remove volumes
```

---

## Troubleshooting

**Producer exits immediately on startup** — Kafka may not be ready yet. Wait for Kafka to be fully up then restart: `docker compose restart producer`.

**Connector registration fails** — Check Connect is healthy first (`make ps`), then check logs with `make connect-logs`. Connect takes 30–60 seconds to become ready after `make up`.

**Snowflake auth error** — `SNOWFLAKE_PRIVATE_KEY` must be the PKCS#8 body only: no `-----BEGIN...-----END-----` lines, no newlines, all on one line.

**CURATED tables empty** — Check the MERGE task is running: `SHOW TASKS IN DATABASE RT_INTEL;`. The role must have USAGE on `WH_ETL` and EXECUTE TASK privilege.

**`jq: command not found` on `make connector`** — Register the connector manually:

```bash
source .env
curl -s -X PUT http://localhost:8083/connectors/snowflake-sink-rtintel/config \
  -H "Content-Type: application/json" \
  -d "{
    \"connector.class\": \"com.snowflake.kafka.connector.SnowflakeSinkConnector\",
    \"tasks.max\": \"2\",
    \"topics\": \"${TOPICS}\",
    \"snowflake.topic2table.map\": \"${TOPIC2TABLE_MAP}\",
    \"snowflake.url.name\": \"${SNOWFLAKE_ACCOUNT_URL}\",
    \"snowflake.user.name\": \"${SNOWFLAKE_USER}\",
    \"snowflake.private.key\": \"${SNOWFLAKE_PRIVATE_KEY}\",
    \"snowflake.database.name\": \"${SNOWFLAKE_DATABASE}\",
    \"snowflake.schema.name\": \"${SNOWFLAKE_SCHEMA}\",
    \"snowflake.role.name\": \"${SNOWFLAKE_ROLE}\",
    \"buffer.count.records\": \"5000\",
    \"buffer.flush.time\": \"30\",
    \"buffer.size.bytes\": \"5000000\",
    \"key.converter\": \"org.apache.kafka.connect.storage.StringConverter\",
    \"value.converter\": \"com.snowflake.kafka.connector.records.SnowflakeJsonConverter\",
    \"errors.tolerance\": \"all\",
    \"errors.log.enable\": \"true\",
    \"errors.log.include.messages\": \"true\"
  }"
```

---

## Snowflake schema

```
RT_INTEL
├── RAW
│   ├── ORDERS_EVENTS       -- written by Kafka Connect (VARIANT columns)
│   ├── PAYMENTS_EVENTS     -- written by Kafka Connect (VARIANT columns)
│   ├── V_ORDERS            -- view: unpacks RECORD_CONTENT JSON fields
│   └── V_PAYMENTS          -- view: unpacks RECORD_CONTENT JSON fields
├── CURATED
│   ├── ORDERS              -- typed, deduped by MERGE task
│   └── PAYMENTS            -- typed, deduped by MERGE task
└── ANALYTICS
    └── V_REVENUE_PER_MINUTE  -- revenue grouped by minute (PAID only)
```
