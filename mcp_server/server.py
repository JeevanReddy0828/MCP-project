import os
from fastmcp import FastMCP

from mcp_server.policy import validate_readonly_sql
from mcp_server.snowflake_client import get_conn
from mcp_server.kafka_client import end_offsets, consumer_lag
from mcp_server.connect_client import list_connectors, connector_status

NAME = os.getenv("MCP_SERVER_NAME", "KafkaSnowflakeMCP")
mcp = FastMCP(NAME)

# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------

@mcp.tool()
def run_sql(query: str, limit: int = 50) -> dict:
    """Run a READ-ONLY SELECT query against Snowflake."""
    validate_readonly_sql(query)

    q = query.strip().rstrip(";")
    if " limit " not in q.lower():
        q = f"{q} LIMIT {int(limit)}"

    with get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(q)
            cols = [c[0] for c in cur.description] if cur.description else []
            rows = cur.fetchmany(int(limit))
            return {"columns": cols, "rows": rows}
        finally:
            cur.close()

# ---------------------------------------------------------------------------
# Revenue
# ---------------------------------------------------------------------------

@mcp.tool()
def revenue_last_minutes(minutes: int = 60) -> dict:
    """Revenue per minute for the last N minutes (PAID orders only)."""
    sql = f"""
    SELECT minute, revenue
    FROM RT_INTEL.ANALYTICS.V_REVENUE_PER_MINUTE
    WHERE minute >= DATEADD('minute', -{int(minutes)}, CURRENT_TIMESTAMP())
    ORDER BY minute DESC
    """
    return run_sql(sql, limit=500)

@mcp.tool()
def revenue_anomaly(lookback_minutes: int = 5, baseline_minutes: int = 30) -> dict:
    """
    Compare average revenue-per-minute over the last N minutes against the
    prior baseline window. Returns pct_change: negative = drop, positive = spike.
    """
    sql = f"""
    WITH recent AS (
      SELECT AVG(revenue) AS avg_revenue
      FROM RT_INTEL.ANALYTICS.V_REVENUE_PER_MINUTE
      WHERE minute >= DATEADD('minute', -{int(lookback_minutes)}, CURRENT_TIMESTAMP())
    ),
    baseline AS (
      SELECT AVG(revenue) AS avg_revenue
      FROM RT_INTEL.ANALYTICS.V_REVENUE_PER_MINUTE
      WHERE minute >= DATEADD('minute', -{int(baseline_minutes + lookback_minutes)}, CURRENT_TIMESTAMP())
        AND minute <  DATEADD('minute', -{int(lookback_minutes)}, CURRENT_TIMESTAMP())
    )
    SELECT
      ROUND(r.avg_revenue, 2)  AS recent_avg_per_min,
      ROUND(b.avg_revenue, 2)  AS baseline_avg_per_min,
      ROUND((r.avg_revenue - b.avg_revenue) / NULLIF(b.avg_revenue, 0) * 100, 1) AS pct_change
    FROM recent r, baseline b
    """
    return run_sql(sql, limit=1)

# ---------------------------------------------------------------------------
# Payments
# ---------------------------------------------------------------------------

@mcp.tool()
def payment_failure_rate(minutes: int = 10) -> dict:
    """
    Payment failure rate per minute for the last N minutes.
    A sudden spike in failure_rate_pct indicates a gateway issue or fraud wave.
    """
    sql = f"""
    SELECT minute, total_payments, failed_payments, failure_rate_pct
    FROM RT_INTEL.ANALYTICS.V_PAYMENT_FAILURE_RATE
    WHERE minute >= DATEADD('minute', -{int(minutes)}, CURRENT_TIMESTAMP())
    ORDER BY minute DESC
    """
    return run_sql(sql, limit=200)

@mcp.tool()
def order_conversion_rate(minutes: int = 10) -> dict:
    """
    Order-to-payment conversion rate per minute for the last N minutes.
    A drop in conversion_rate_pct suggests a checkout or payment funnel issue.
    """
    sql = f"""
    SELECT minute, total_orders, paid_orders, conversion_rate_pct
    FROM RT_INTEL.ANALYTICS.V_ORDER_CONVERSION
    WHERE minute >= DATEADD('minute', -{int(minutes)}, CURRENT_TIMESTAMP())
    ORDER BY minute DESC
    """
    return run_sql(sql, limit=200)

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------

@mcp.tool()
def pipeline_summary() -> dict:
    """
    Single-call health check combining:
    - Kafka Connect connector state and task states
    - Kafka consumer lag per topic (connector group)
    - Snowflake ingest throughput (rows landed in the last 5 minutes)
    Use this as a first call when investigating any pipeline issue.
    """
    health = connector_health("snowflake-sink-rtintel")
    orders_lag   = consumer_lag("orders",   "connect-cluster")
    payments_lag = consumer_lag("payments", "connect-cluster")

    throughput_sql = """
    SELECT
      (SELECT COUNT(*) FROM RT_INTEL.RAW.ORDERS_EVENTS
       WHERE LOAD_TIME >= DATEADD('minute', -5, CURRENT_TIMESTAMP()))   AS orders_5min,
      (SELECT COUNT(*) FROM RT_INTEL.RAW.PAYMENTS_EVENTS
       WHERE LOAD_TIME >= DATEADD('minute', -5, CURRENT_TIMESTAMP()))   AS payments_5min,
      (SELECT COUNT(*) FROM RT_INTEL.CURATED.ORDERS)                    AS curated_orders_total,
      (SELECT COUNT(*) FROM RT_INTEL.CURATED.PAYMENTS)                  AS curated_payments_total
    """
    throughput = run_sql(throughput_sql, limit=1)

    return {
        "connector":            health,
        "kafka_lag":            {"orders": orders_lag, "payments": payments_lag},
        "snowflake_throughput": throughput,
    }

@mcp.tool()
def kafka_end_offsets(topic: str) -> dict:
    """Latest partition offsets for a Kafka topic."""
    return end_offsets(topic)

@mcp.tool()
def kafka_consumer_lag(topic: str, group: str) -> dict:
    """Consumer lag per partition for a topic + group."""
    return consumer_lag(topic, group)

@mcp.tool()
def connector_health(connector_name: str | None = None) -> dict:
    """Kafka Connect connector status. Omit connector_name to list all connectors."""
    try:
        if connector_name:
            return connector_status(connector_name)
        return list_connectors()
    except Exception as e:
        return {"ok": False, "error": str(e), "hint": "Check CONNECT_REST_URL and that connect is running."}


def main():
    mcp.run()

if __name__ == "__main__":
    main()
