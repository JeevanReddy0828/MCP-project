-- =========================
-- Streams + Tasks (1 minute)
-- =========================

USE DATABASE RT_INTEL;

-- Warehouse for tasks
CREATE WAREHOUSE IF NOT EXISTS WH_ETL
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- Streams on RAW tables (append-only)
CREATE OR REPLACE STREAM RAW.ORDERS_EVENTS_STREAM
  ON TABLE RAW.ORDERS_EVENTS
  APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM RAW.PAYMENTS_EVENTS_STREAM
  ON TABLE RAW.PAYMENTS_EVENTS
  APPEND_ONLY = TRUE;

-- Task: every minute merge only new stream rows
CREATE OR REPLACE TASK CURATED.MERGE_RAW_TO_CURATED_TASK
  WAREHOUSE = WH_ETL
  SCHEDULE = '1 minute'
AS
BEGIN
  MERGE INTO CURATED.ORDERS t
  USING (
    SELECT
      RECORD_CONTENT:order_id::STRING        AS order_id,
      RECORD_CONTENT:customer_id::STRING     AS customer_id,
      RECORD_CONTENT:amount::FLOAT           AS amount,
      RECORD_CONTENT:currency::STRING        AS currency,
      RECORD_CONTENT:event_ts::TIMESTAMP_NTZ AS event_ts,
      LOAD_TIME                               AS load_time
    FROM RAW.ORDERS_EVENTS_STREAM
  ) s
  ON t.order_id = s.order_id
  WHEN MATCHED AND s.load_time > t.load_time THEN
    UPDATE SET
      customer_id = s.customer_id,
      amount      = s.amount,
      currency    = s.currency,
      event_ts    = s.event_ts,
      load_time   = s.load_time
  WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, amount, currency, event_ts, load_time)
    VALUES (s.order_id, s.customer_id, s.amount, s.currency, s.event_ts, s.load_time);

  MERGE INTO CURATED.PAYMENTS t
  USING (
    SELECT
      RECORD_CONTENT:payment_id::STRING      AS payment_id,
      RECORD_CONTENT:order_id::STRING        AS order_id,
      RECORD_CONTENT:status::STRING          AS status,
      RECORD_CONTENT:amount::FLOAT           AS amount,
      RECORD_CONTENT:event_ts::TIMESTAMP_NTZ AS event_ts,
      LOAD_TIME                               AS load_time
    FROM RAW.PAYMENTS_EVENTS_STREAM
  ) s
  ON t.payment_id = s.payment_id
  WHEN MATCHED AND s.load_time > t.load_time THEN
    UPDATE SET
      order_id  = s.order_id,
      status    = s.status,
      amount    = s.amount,
      event_ts  = s.event_ts,
      load_time = s.load_time
  WHEN NOT MATCHED THEN
    INSERT (payment_id, order_id, status, amount, event_ts, load_time)
    VALUES (s.payment_id, s.order_id, s.status, s.amount, s.event_ts, s.load_time);
END;

ALTER TASK CURATED.MERGE_RAW_TO_CURATED_TASK RESUME;
