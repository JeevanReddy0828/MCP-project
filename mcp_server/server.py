import os
from fastmcp import FastMCP

from mcp_server.policy import validate_readonly_sql
from mcp_server.snowflake_client import get_conn
from mcp_server.kafka_client import end_offsets, consumer_lag
from mcp_server.connect_client import list_connectors, connector_status

NAME = os.getenv("MCP_SERVER_NAME", "KafkaSnowflakeMCP")
mcp = FastMCP(NAME)

@mcp.tool()
def run_sql(query: str, limit: int = 50) -> dict:
    """Run a READ-ONLY SELECT query in Snowflake."""
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

@mcp.tool()
def revenue_last_minutes(minutes: int = 60) -> dict:
    sql = f"""
    SELECT minute, revenue
    FROM RT_INTEL.ANALYTICS.V_REVENUE_PER_MINUTE
    WHERE minute >= DATEADD('minute', -{int(minutes)}, CURRENT_TIMESTAMP())
    ORDER BY minute DESC
    """
    return run_sql(sql, limit=500)

@mcp.tool()
def kafka_end_offsets(topic: str) -> dict:
    return end_offsets(topic)

@mcp.tool()
def kafka_consumer_lag(topic: str, group: str) -> dict:
    return consumer_lag(topic, group)

@mcp.tool()
def connector_health(connector_name: str | None = None) -> dict:
    """Kafka Connect health check."""
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
