import re

DISALLOWED = re.compile(
    r"\b(DELETE|DROP|ALTER|TRUNCATE|GRANT|REVOKE|CREATE|REPLACE|PUT|GET|COPY|MERGE|INSERT|UPDATE)\b",
    re.IGNORECASE,
)

def validate_readonly_sql(sql: str) -> None:
    s = (sql or "").strip().strip(";")
    if not s:
        raise ValueError("SQL is empty.")
    if not s.lower().startswith("select"):
        raise ValueError("Only SELECT queries are allowed.")
    if DISALLOWED.search(s):
        raise ValueError("Query contains disallowed keywords.")
