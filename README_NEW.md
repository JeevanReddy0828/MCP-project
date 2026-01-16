# Real-Time Streaming ETL Pipeline with Kafka, Snowflake & MCP
## Multi-Layer Data Processing: RAW → CURATED → ANALYTICS

**A production-grade streaming data pipeline** that ingests event data through Kafka, processes it incrementally with Snowflake Streams & Tasks, and exposes operational insights via a Python MCP (Model Context Protocol) server.

### 🎯 What This Does
- **Real-time ingestion**: Kafka brokers stream `orders` + `payments` events
- **Multi-layer transformation**: RAW (schema-agnostic) → CURATED (typed/deduplicated) → ANALYTICS (aggregated)
- **Sub-minute processing**: Snowflake Streams + scheduled MERGE tasks ensure fresh data every 60 seconds
- **Operational visibility**: MCP server exposes Kafka lag, connector health, and live revenue metrics
- **Agent-ready**: AI/LLM tools can query live pipeline state with SQL policy enforcement

---

## 📋 Table of Contents
1. [Quick Start (5 min)](#-quick-start)
2. [Architecture Overview](#-architecture)
3. [Prerequisites](#-prerequisites)
4. [Detailed Setup](#-detailed-setup)
5. [Component Reference](#-component-reference)
6. [MCP Tools](#-mcp-tools)
7. [Troubleshooting](#-troubleshooting)

---

## ⚡ Quick Start

### 1) Prerequisites
- Docker Desktop (running)
- Snowflake account with key-pair auth configured
- `curl`, `jq`, `openssl`

### 2) Generate RSA Key Pair
```bash
# Private key (PKCS#8)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

# Public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

In **Snowflake UI**:
1. Copy public key body (no `-----BEGIN...-----END-----`) to user `RSA_PUBLIC_KEY`
2. Copy private key body (no headers/footers, **remove all newlines**) → will use in `.env`

### 3) Configure Environment
```bash
cp .env.example .env
# Edit .env with Snowflake account details + private key body (single line, no newlines)
```

### 4) Run Snowflake Scripts
Execute in **Snowflake UI** (in this order):
```bash
# 1. Create schemas + RAW tables + views
snowflake/init.sql

# 2. Create CURATED tables + analytics view
snowflake/transforms.sql

# 3. Create warehouse + streams + 1-minute MERGE task
snowflake/tasks.sql
```

### 5) Start Pipeline
```bash
# Build + start all containers (Zookeeper, Kafka, Connect, Producer, MCP)
make up

# Create Kafka topics
make topics

# Register Snowflake Sink Connector
make connector

# Verify connector is healthy
make connector-status
```

### 6) Validate
```sql
-- In Snowflake, check data is flowing
SELECT COUNT(*) as orders_count FROM RT_INTEL.RAW.ORDERS_EVENTS;
SELECT COUNT(*) as curated_orders FROM RT_INTEL.CURATED.ORDERS;

-- Check analytics view
SELECT * FROM RT_INTEL.ANALYTICS.V_REVENUE_PER_MINUTE LIMIT 5;
```

**Done!** Events are now flowing: Producer → Kafka → Connector → Snowflake RAW → MERGE Task → CURATED tables.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   REAL-TIME DATA PIPELINE                       │
└─────────────────────────────────────────────────────────────────┘

Producer Container
    ↓ (generates fake orders/payments every 200ms)
    ↓
Kafka Broker (9092)
    ├─ orders topic (3 partitions)
    └─ payments topic (3 partitions)
    ↓
Kafka Connect (8083)
    ↓ (Snowflake Sink Connector v3.4.0)
    ↓
Snowflake
    ├─ RT_INTEL.RAW (loaded by Kafka)
    │   ├─ ORDERS_EVENTS (RECORD_METADATA + RECORD_CONTENT)
    │   └─ PAYMENTS_EVENTS (RECORD_METADATA + RECORD_CONTENT)
    │   └─ Views: V_ORDERS, V_PAYMENTS (extract nested JSON)
    │
    ├─ RT_INTEL.RAW Streams (APPEND_ONLY)
    │   ├─ ORDERS_EVENTS_STREAM
    │   └─ PAYMENTS_EVENTS_STREAM
    │
    ├─ Scheduled Task (1-minute interval)
    │   ↓ MERGE new stream rows
    │   ↓
    │   RT_INTEL.CURATED (typed, deduplicated)
    │   ├─ ORDERS (order_id, customer_id, amount, currency, event_ts, load_time)
    │   └─ PAYMENTS (payment_id, order_id, status, amount, event_ts, load_time)
    │
    └─ RT_INTEL.ANALYTICS (aggregations)
        └─ V_REVENUE_PER_MINUTE (minute-level revenue, paid only)

MCP Server (stdio)
    ↑ (5 tools: run_sql, revenue_last_minutes, kafka_end_offsets, 
       kafka_consumer_lag, connector_health)
    ↑
    └─ Queries Snowflake, Kafka, Kafka Connect API
```

### Data Flow Example
```
1. Producer emits:
   {order_id: "abc123", customer_id: "C1000", amount: 99.99, ...}
   
2. Kafka topic: orders
   
3. Kafka Connect polls & buffers (5000 records or 30s)
   
4. Connector writes to Snowflake:
   INSERT INTO RAW.ORDERS_EVENTS (RECORD_METADATA, RECORD_CONTENT)
   
5. Stream captures change (APPEND_ONLY=TRUE)
   
6. Task (every 1min) runs MERGE:
   MERGE INTO CURATED.ORDERS t
   USING RAW.ORDERS_EVENTS_STREAM s
   WHEN NOT MATCHED THEN INSERT (...)
   
7. Analytics view aggregates:
   SELECT DATE_TRUNC('minute', event_ts), SUM(amount)
   FROM CURATED.PAYMENTS WHERE status='PAID'
```

---

## ✅ Prerequisites

| Component | Requirement |
|-----------|-------------|
| **Docker** | Docker Desktop (v4.0+); Zookeeper, Kafka, Kafka Connect images |
| **Snowflake** | Account with compute warehouse; user with key-pair auth configured |
| **Network** | Internet (to download Snowflake Kafka connector JAR) |
| **Shell** | WSL/Git Bash on Windows (for `envsubst` in connector registration) |
| **Security** | Private key stored securely; `.env` never committed to git |

---

## 📝 Detailed Setup

### Step 1: Generate RSA Key Pair

**On macOS/Linux:**
```bash
# Generate private key (PKCS#8, unencrypted)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

# Generate public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

# Display key bodies (copy these values)
cat rsa_key.pub   # Copy body (lines 2-N, no header/footer)
cat rsa_key.p8    # Copy body (lines 2-N, no header/footer, JOIN INTO SINGLE LINE)
```

**On Windows (PowerShell):**
```powershell
# Use WSL or Git Bash, or install OpenSSL via Chocolatey
choco install openssl
# Then run bash commands above
```

### Step 2: Configure Snowflake User

In **Snowflake UI** (using ACCOUNTADMIN or user creation role):

```sql
-- Create user with key-pair auth
CREATE USER KAFKA_CONNECTOR_USER
  RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...'
  DEFAULT_ROLE = 'KAFKA_CONNECTOR_ROLE'
  MUST_CHANGE_PASSWORD = FALSE;

-- Create role + grant privileges
CREATE ROLE KAFKA_CONNECTOR_ROLE;
GRANT ROLE KAFKA_CONNECTOR_ROLE TO USER KAFKA_CONNECTOR_USER;

-- Grant schema + warehouse permissions
GRANT CREATE SCHEMA ON DATABASE RT_INTEL TO ROLE KAFKA_CONNECTOR_ROLE;
GRANT USAGE ON WAREHOUSE WH_ETL TO ROLE KAFKA_CONNECTOR_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE KAFKA_CONNECTOR_ROLE;

-- Grant table permissions (after init.sql runs)
GRANT INSERT ON ALL TABLES IN SCHEMA RT_INTEL.RAW TO ROLE KAFKA_CONNECTOR_ROLE;
```

### Step 3: Create `.env` File

```bash
cd /path/to/kafka-snowflake-mcp
cp .env.example .env
```

Edit `.env`:
```dotenv
# ===== KAFKA =====
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
CONNECT_REST_URL=http://localhost:8083

# ===== SNOWFLAKE (REQUIRED) =====
# Your Snowflake account locator (from account URL)
SNOWFLAKE_ACCOUNT_URL=yourorg-youracct.snowflakecomputing.com:443

# User created above
SNOWFLAKE_USER=KAFKA_CONNECTOR_USER
SNOWFLAKE_ROLE=KAFKA_CONNECTOR_ROLE
SNOWFLAKE_DATABASE=RT_INTEL
SNOWFLAKE_SCHEMA=RAW

# ===== PRIVATE KEY (CRITICAL) =====
# Paste ONLY the key body (no -----BEGIN/END----- lines, NO NEWLINES, SINGLE LINE)
# Example: MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7VJTUt9Us8cKjMzEfYyjiWA4...
SNOWFLAKE_PRIVATE_KEY=PASTE_KEY_BODY_HERE_NO_NEWLINES

# ===== KAFKA CONNECTOR MAPPING =====
# Topics to ingest
TOPICS=orders,payments

# Topic -> Snowflake table mapping (in RAW schema)
TOPIC2TABLE_MAP=orders:ORDERS_EVENTS,payments:PAYMENTS_EVENTS
```

⚠️ **Critical**: The private key must be:
- PKCS#8 format (not PKCS#1)
- No `-----BEGIN...-----END-----` lines
- **NO NEWLINES** (single continuous line)
- Stored in `.env` which is `.gitignore`'d (never commit!)

### Step 4: Run Snowflake SQL Scripts

Execute in **Snowflake UI** (must run in order):

**Script 1: `snowflake/init.sql`**
- Creates database: `RT_INTEL`
- Creates schemas: `RAW`, `CURATED`, `ANALYTICS`
- Creates RAW tables: `ORDERS_EVENTS`, `PAYMENTS_EVENTS` (variant columns for JSON)
- Creates views: `V_ORDERS`, `V_PAYMENTS` (extracts nested fields)

**Script 2: `snowflake/transforms.sql`**
- Creates CURATED tables (typed columns)
- Creates analytics view: `V_REVENUE_PER_MINUTE` (aggregated revenue by minute, paid only)

**Script 3: `snowflake/tasks.sql`**
- Creates warehouse: `WH_ETL` (XSMALL, auto-suspend 60s)
- Creates streams: `ORDERS_EVENTS_STREAM`, `PAYMENTS_EVENTS_STREAM` (APPEND_ONLY=TRUE)
- Creates task: `MERGE_RAW_TO_CURATED_TASK` (runs every 1 minute, MERGEs stream changes into CURATED)
- Resumes task (enabled by default)

### Step 5: Start Containers

```bash
# Build images + start all services (detached mode)
make up

# Wait ~30s for containers to be healthy
docker compose ps

# Expected output:
# SERVICE          STATUS
# zookeeper        Up (healthy)
# kafka            Up (healthy)
# connect          Up (healthy)
# producer         Up (running)
# mcp              Up (running)
```

### Step 6: Create Kafka Topics

```bash
make topics

# Output:
# Created topic orders.
# Created topic payments.
# orders
# payments
```

### Step 7: Register Snowflake Connector

```bash
make connector

# Output:
# Registering connector with Kafka Connect...
# {
#   "name": "snowflake-sink-rtintel",
#   "config": { ... },
#   "tasks": [...]
# }
```

### Step 8: Verify Everything Works

```bash
# Check connector is healthy
make connector-status

# Expected output:
# {
#   "name": "snowflake-sink-rtintel",
#   "connector": { "state": "RUNNING", ... },
#   "tasks": [
#     { "id": 0, "state": "RUNNING", ... },
#     { "id": 1, "state": "RUNNING", ... }
#   ]
# }
```

In **Snowflake UI**, run:
```sql
-- Should have data within 30-60 seconds
SELECT COUNT(*) as event_count FROM RT_INTEL.RAW.ORDERS_EVENTS;
SELECT COUNT(*) as order_count FROM RT_INTEL.CURATED.ORDERS;

-- Wait 1-2 minutes for task to run
SELECT * FROM RT_INTEL.ANALYTICS.V_REVENUE_PER_MINUTE LIMIT 5;
```

---

## 📦 Component Reference

### **Producer** (`producer/app.py`)
- Generates fake order + payment events every 200ms
- Emits to Kafka topics: `orders`, `payments`
- Event schema:
  ```json
  // orders
  {
    "event_type": "order_created",
    "event_ts": "2026-01-15T10:30:45.123456+00:00",
    "order_id": "550e8400-e29b-41d4-a716-446655440000",
    "customer_id": "C1234",
    "amount": 99.99,
    "currency": "USD"
  }
  
  // payments
  {
    "event_type": "payment",
    "event_ts": "2026-01-15T10:30:46.000000+00:00",
    "payment_id": "550e8400-e29b-41d4-a716-446655440001",
    "order_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "PAID",  // or "FAILED" (5% fail rate)
    "amount": 99.99
  }
  ```

### **Kafka Connect**
- Snowflake Sink Connector v3.4.0
- Pulls from `orders`, `payments` topics
- Buffers: 5000 records OR 30 seconds (whichever first)
- Writes to `RT_INTEL.RAW.*_EVENTS` tables
- Error handling: logs all errors; dead records dropped (configurable)

### **Snowflake Streams & Tasks**
- **Streams** capture INSERT-only changes to RAW tables
- **Task** merges streams into CURATED tables every 1 minute
- Deduplication logic: MERGE by order_id/payment_id; latest load_time wins
- Ensures exactly-once semantics (MERGE handles duplicates)

### **MCP Server** (`mcp_server/server.py`)
- Runs in `mcp` container
- Exposes 5 tools (see [MCP Tools](#-mcp-tools) below)
- Requires valid Snowflake auth (JWT via private key)
- Enforces read-only SQL policy (SELECT-only, blocks DML/DDL)

---

## 🔧 MCP Tools

The MCP server exposes these tools:

### 1. `run_sql(query: str, limit: int = 50) -> dict`
**Execute read-only SELECT queries on Snowflake.**

```python
# Example usage
run_sql("SELECT * FROM RT_INTEL.CURATED.ORDERS", limit=100)

# Returns
{
  "columns": ["ORDER_ID", "CUSTOMER_ID", "AMOUNT", "CURRENCY", "EVENT_TS", "LOAD_TIME"],
  "rows": [
    ["550e8400-e29b-41d4-a716-446655440000", "C1234", 99.99, "USD", "2026-01-15T10:30:45Z", "2026-01-15T10:31:12Z"],
    ...
  ]
}
```

**Policy**: Only SELECT queries allowed; blocks INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, MERGE.

### 2. `revenue_last_minutes(minutes: int = 60) -> dict`
**Aggregate revenue from last N minutes (paid orders only).**

```python
# Example usage
revenue_last_minutes(minutes=30)

# Returns
{
  "columns": ["MINUTE", "REVENUE"],
  "rows": [
    ["2026-01-15T10:30:00Z", 5234.56],
    ["2026-01-15T10:29:00Z", 4891.23],
    ...
  ]
}
```

### 3. `kafka_end_offsets(topic: str) -> dict`
**Get latest offset for each partition in a Kafka topic.**

```python
# Example usage
kafka_end_offsets("orders")

# Returns
{
  "topic": "orders",
  "partitions": {
    "0": 1523,
    "1": 1489,
    "2": 1501
  }
}
```

### 4. `kafka_consumer_lag(topic: str, group: str) -> dict`
**Calculate consumer lag per partition.**

```python
# Example usage
kafka_consumer_lag("orders", "snowflake-sink-rtintel")

# Returns
{
  "topic": "orders",
  "group": "snowflake-sink-rtintel",
  "lag": 12,  # Total lag across all partitions
  "partitions": {
    "0": {"end": 1523, "committed": 1520, "lag": 3},
    "1": {"end": 1489, "committed": 1487, "lag": 2},
    "2": {"end": 1501, "committed": 1499, "lag": 2}
  }
}
```

### 5. `connector_health(connector_name: str | None = None) -> dict`
**Check Kafka Connect connector status.**

```python
# Example usage (specific connector)
connector_health("snowflake-sink-rtintel")

# Returns
{
  "connect_rest_url": "http://connect:8083",
  "status": {
    "name": "snowflake-sink-rtintel",
    "connector": {
      "state": "RUNNING",
      "worker_id": "connect:8083"
    },
    "tasks": [
      {"id": 0, "state": "RUNNING", "worker_id": "connect:8083"},
      {"id": 1, "state": "RUNNING", "worker_id": "connect:8083"}
    ]
  }
}

# Example usage (list all connectors)
connector_health()

# Returns
{
  "connect_rest_url": "http://connect:8083",
  "connectors": ["snowflake-sink-rtintel", ...]
}
```

---

## 📊 Useful Commands

```bash
# Monitoring
make logs              # Follow all container logs (Ctrl+C to exit)
make connect-logs      # Kafka Connect logs only
make mcp-logs          # MCP server logs only
make ps                # Show container status

# Connector management
make connector-status  # Check connector health (curl + jq)

# Lifecycle
make up                # Start all containers (build + run)
make down              # Stop + remove containers + volumes
make build             # Rebuild images without starting

# Development
make topics            # Create Kafka topics
make connector         # Register Snowflake connector
```

---

## 🐛 Troubleshooting

### **Connector fails to register**
```bash
# Check Kafka Connect logs
make connect-logs

# Common issues:
# - .env file missing
# - Private key has newlines or header/footer
# - Snowflake user not configured for key-pair auth
# - Network issue downloading connector JAR
```

**Solution:**
1. Verify `.env` exists and has valid Snowflake credentials
2. Private key must be PKCS#8, no newlines, single line
3. Check Snowflake user has `RSA_PUBLIC_KEY` set
4. Retry: `make connector`

### **No data flowing to Snowflake**
```bash
# Check producer logs
docker compose logs producer

# Check connector logs
make connect-logs

# Check Kafka topics have data
docker compose exec -T kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic orders \
  --from-beginning \
  --max-messages 5
```

**Solution:**
1. Wait 30-60s for events to accumulate (producer runs every 200ms)
2. Check connector task status: `make connector-status`
3. If tasks are failed, check Snowflake user permissions on RAW schema/tables
4. Check Snowflake warehouse is running (`WH_ETL` used by task)

### **MCP server fails to authenticate**
```bash
# Test Snowflake connection directly
docker compose exec -T mcp python -c "
from mcp_server.snowflake_client import get_conn
try:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('SELECT 1')
        print('Connection OK')
except Exception as e:
    print(f'Auth failed: {e}')
"
```

**Solution:**
1. Verify `.env` has correct Snowflake URL, user, role
2. Check private key format (must be PKCS#8 DER, no PEM header)
3. Verify public key set on Snowflake user (`ALTER USER ... SET RSA_PUBLIC_KEY`)
4. Retry container: `docker compose restart mcp`

### **Task not running / CURATED tables empty**
```sql
-- In Snowflake, check task status
SELECT * FROM INFORMATION_SCHEMA.TASKS 
WHERE SCHEMA_NAME='CURATED';

-- Check task run history
SELECT * FROM INFORMATION_SCHEMA.TASK_HISTORY 
WHERE NAME='MERGE_RAW_TO_CURATED_TASK'
ORDER BY QUERY_START_TIME DESC
LIMIT 10;
```

**Solution:**
1. Ensure task is resumed: `ALTER TASK RT_INTEL.CURATED.MERGE_RAW_TO_CURATED_TASK RESUME;`
2. Check user role has TASK privilege: `GRANT EXECUTE TASK ON ACCOUNT TO ROLE KAFKA_CONNECTOR_ROLE;`
3. Verify warehouse `WH_ETL` exists and user can use it: `GRANT USAGE ON WAREHOUSE WH_ETL TO ROLE KAFKA_CONNECTOR_ROLE;`
4. Check RAW tables have data: `SELECT COUNT(*) FROM RT_INTEL.RAW.ORDERS_EVENTS;`
5. Wait 1-2 minutes (task runs on 1-minute schedule)

### **Docker compose fails to start**
```bash
# Check Docker is running
docker ps

# Check images build successfully
make build

# Check logs for specific service
docker compose logs connect
docker compose logs mcp

# Clean slate
make down
docker system prune
make up
```

---

## 📂 Project Structure

```
kafka-snowflake-mcp/
├── README.md                           # This file
├── Makefile                            # Build + deployment commands
├── docker-compose.yml                  # Multi-container orchestration
├── .env.example                        # Environment variables template
├── .gitignore                          # Excludes .env, volumes, etc.
│
├── infra/
│   ├── kafka/
│   │   └── create-topics.sh           # Kafka topic creation script
│   └── connect/
│       ├── Dockerfile                  # Kafka Connect + Snowflake JAR
│       ├── snowflake-sink.template.json # Connector config template
│       └── register_connector.sh       # Registers connector to Kafka Connect
│
├── producer/
│   ├── app.py                         # Event generator (orders/payments)
│   ├── requirements.txt                # Dependencies: kafka-python
│   └── Dockerfile
│
├── mcp_server/
│   ├── server.py                      # MCP service (5 tools)
│   ├── snowflake_client.py            # Snowflake JWT auth + connection
│   ├── kafka_client.py                # Kafka consumer + lag calculation
│   ├── connect_client.py              # Kafka Connect REST client
│   ├── policy.py                      # SQL validation (read-only enforcement)
│   ├── pyproject.toml                 # Dependencies + entry point
│   └── Dockerfile
│
└── snowflake/
    ├── init.sql                       # Create schemas + RAW tables + views
    ├── transforms.sql                 # Create CURATED tables + analytics
    └── tasks.sql                      # Create streams + 1-min MERGE task
```

---

## 🚀 Example Workflows

### **Monitor Pipeline Health**
```bash
# Terminal 1: Watch all logs
make logs

# Terminal 2: Check connector every 10s
while true; do make connector-status; sleep 10; done

# Terminal 3: Query Snowflake in real-time (every 30s)
while true; do
  echo "=== Orders ==="
  snowsql -c myaccount -d RT_INTEL -q "SELECT COUNT(*) FROM CURATED.ORDERS"
  sleep 30
done
```

### **Debug Consumer Lag**
```bash
# Check current Kafka offsets
docker compose exec -T mcp python -c "
from mcp_server.kafka_client import consumer_lag
print(consumer_lag('orders', 'snowflake-sink-rtintel'))
"
```

### **Test SQL Policy**
```bash
# Should fail (INSERT blocked)
docker compose exec -T mcp python -c "
from mcp_server.policy import validate_readonly_sql
validate_readonly_sql('INSERT INTO ORDERS VALUES (...)')
" 2>&1 | grep error

# Should succeed
docker compose exec -T mcp python -c "
from mcp_server.policy import validate_readonly_sql
validate_readonly_sql('SELECT * FROM ORDERS LIMIT 10')
print('OK')
"
```

---

## 📖 References

- [Snowflake Kafka Connector Docs](https://docs.snowflake.com/en/user-guide/kafka-connector)
- [Snowflake Streams](https://docs.snowflake.com/en/user-guide/streams)
- [Snowflake Tasks](https://docs.snowflake.com/en/user-guide/tasks)
- [Confluent Kafka Connect](https://docs.confluent.io/kafka-connect/current/index.html)
- [Model Context Protocol](https://modelcontextprotocol.io)

---

## 📝 License

[Add your license here]

---

## 💬 Questions?

Check [Troubleshooting](#-troubleshooting) or review container logs:
```bash
make logs
```
