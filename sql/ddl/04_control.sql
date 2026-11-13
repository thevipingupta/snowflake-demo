-- =============================================================================
-- 04_control.sql  (Snowflake)  -- CONTROL: reconciliation and data-quality results
-- =============================================================================
-- Purpose : record, for each batch, that row counts and key measures agree from
--           bronze to silver to gold, and the result of each data-quality rule.
-- Note    : CONTROL.load_log and CONTROL.schema_drift_log are created in
--           01_bronze.sql, next to the bronze loads that write them.
--
-- Naming  : these two tables use the column names given in the brief. CLAUDE.md
--           lists different names for the DQ and load tables, so CLAUDE.md
--           needs updating to match (see the assumptions list).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- CC_DEMO.CONTROL.reconciliation
--   Grain   : one row per (batch_id, source_table, metric) per check run.
--   Purpose : compares a metric across layers for one batch. Example: the count of
--             CALLLOG lines in bronze, the count of unique calls in silver
--             (stg_call), and the count of calls in gold (fact_call_daily,
--             summed).
--   status  : MATCH | MISMATCH | EXPECTED_DIFFERENCE (for example, dedupe or
--             quarantine rows, where the difference is explained in the row).
-- Clustering: checked_ts. Recent runs are read most often.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CC_DEMO.CONTROL.reconciliation (
    batch_id VARCHAR(16) NOT NULL,
    source_table VARCHAR(128) NOT NULL,         -- e.g. raw_api_calllog
    metric VARCHAR(128) NOT NULL,               -- e.g. row_count, handle_seconds_total
    source_value NUMBER(18,2),                  -- the value from the Five9 export file (line count)
    bronze_value NUMBER(18,2),
    silver_value NUMBER(18,2),
    gold_value NUMBER(18,2),
    status VARCHAR(32) NOT NULL,                -- MATCH | MISMATCH | EXPECTED_DIFFERENCE
    checked_ts TIMESTAMP_NTZ NOT NULL
)
CLUSTER BY (checked_ts);

-- -----------------------------------------------------------------------------
-- CC_DEMO.CONTROL.dq_result
--   Grain   : one row per rule per layer per table per batch per run.
--   Purpose : the result of each data-quality rule. Example rule_ids:
--             DQ_ACD_SL_RANGE (service level 0-100), DQ_CALL_HANDLE_NONNEG,
--             DQ_CALL_DEDUP_CONTACT_ID, DQ_SEG_CALL_FK, DQ_PII_MASKED,
--             DQ_AGENT_IN_WFM.
--   severity: ERROR (stops the layer) | WARN (kept, reported) | INFO.
-- Clustering: run_ts. Recent runs are read most often.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CC_DEMO.CONTROL.dq_result (
    rule_id VARCHAR(64) NOT NULL,
    layer VARCHAR(16) NOT NULL,                 -- BRONZE | SILVER | GOLD
    table_name VARCHAR(128) NOT NULL,
    batch_id VARCHAR(16),
    rows_checked NUMBER(12,0) NOT NULL,
    rows_failed NUMBER(12,0) NOT NULL,
    severity VARCHAR(8) NOT NULL,               -- ERROR | WARN | INFO
    run_ts TIMESTAMP_NTZ NOT NULL
)
CLUSTER BY (run_ts);
