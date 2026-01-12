-- CURATED tables: typed + deduped
CREATE TABLE IF NOT EXISTS RT_INTEL.CURATED.ORDERS (
  order_id STRING,
  customer_id STRING,
  amount FLOAT,
  currency STRING,
  event_ts TIMESTAMP_NTZ,
  load_time TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS RT_INTEL.CURATED.PAYMENTS (
  payment_id STRING,
  order_id STRING,
  status STRING,
  amount FLOAT,
  event_ts TIMESTAMP_NTZ,
  load_time TIMESTAMP_NTZ
);

-- Analytics view: revenue per minute (paid only)
CREATE OR REPLACE VIEW RT_INTEL.ANALYTICS.V_REVENUE_PER_MINUTE AS
SELECT
  DATE_TRUNC('minute', p.event_ts) AS minute,
  SUM(CASE WHEN p.status = 'PAID' THEN p.amount ELSE 0 END) AS revenue
FROM RT_INTEL.CURATED.PAYMENTS p
GROUP BY 1
ORDER BY 1 DESC;
