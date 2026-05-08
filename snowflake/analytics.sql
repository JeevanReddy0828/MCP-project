-- Run after transforms.sql
-- Two new analytics views for payment operations monitoring

CREATE OR REPLACE VIEW RT_INTEL.ANALYTICS.V_PAYMENT_FAILURE_RATE AS
SELECT
  DATE_TRUNC('minute', event_ts)                                              AS minute,
  COUNT(*)                                                                    AS total_payments,
  SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)                         AS failed_payments,
  ROUND(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS failure_rate_pct
FROM RT_INTEL.CURATED.PAYMENTS
GROUP BY 1
ORDER BY 1 DESC;

CREATE OR REPLACE VIEW RT_INTEL.ANALYTICS.V_ORDER_CONVERSION AS
SELECT
  DATE_TRUNC('minute', o.event_ts)                                                  AS minute,
  COUNT(DISTINCT o.order_id)                                                        AS total_orders,
  COUNT(DISTINCT p.order_id)                                                        AS paid_orders,
  ROUND(COUNT(DISTINCT p.order_id) / NULLIF(COUNT(DISTINCT o.order_id), 0) * 100, 2) AS conversion_rate_pct
FROM RT_INTEL.CURATED.ORDERS o
LEFT JOIN RT_INTEL.CURATED.PAYMENTS p
  ON o.order_id = p.order_id AND p.status = 'PAID'
GROUP BY 1
ORDER BY 1 DESC;
