-- =============================================================================
-- test_pipeline.sql  (Snowflake)  -- end-to-end tests for the procedures in sql/procs
-- =============================================================================
-- Run after sql/ddl/01-04 and sql/procs/00-06, with the generated data uploaded to
-- CC_DEMO.BRONZE.src_stage, the planted-issue log uploaded to CC_DEMO.CONTROL.test_stage,
-- and CUSTOMER_KEY_SECRET set in the session.
--
--   PUT file://data/source/_planted_issues.csv @CC_DEMO.CONTROL.test_stage AUTO_COMPRESS=FALSE;
--
-- Results: CONTROL.test_results (PASS, FAIL, or NO_RULE). A test that raises an
-- error is recorded as FAIL and the rest still run.
--
-- Tests:
--   T1  clean batch         ACD batch with no planted ACD issue: zero quarantined rows,
--                           zero ERROR DQ failures.
--   T2  planted batch       run the pipeline on the batch with the most planted issues;
--                           quarantine count per reason equals the planted count per type.
--   T3  schema evolution    load B058 then B059 (week 9). B059 logs the two new columns
--                           (NEW_VS_PRIOR_BATCH) and loads with status SUCCEEDED.
--   T4  re-run              re-run the planted batch; no new rows in any layer.
--   T5  rules with no test  late_acd_file and duplicate_column_name have no DQ rule;
--                           reported as NO_RULE.
-- =============================================================================

CREATE TABLE IF NOT EXISTS CC_DEMO.CONTROL.test_results (
    test_name VARCHAR,
    status VARCHAR,          -- PASS | FAIL | NO_RULE
    expected VARCHAR,
    actual VARCHAR,
    detail VARCHAR,
    run_ts TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE CC_DEMO.CONTROL.test_planted (
    issue_id VARCHAR,
    issue_type VARCHAR,
    source VARCHAR,
    batch_id VARCHAR,
    file_name VARCHAR,
    line_number NUMBER,
    column_name VARCHAR,
    record_key VARCHAR,
    detail VARCHAR,
    expected_detection_rule VARCHAR
);

COPY INTO CC_DEMO.CONTROL.test_planted
  FROM @CC_DEMO.CONTROL.test_stage/_planted_issues.csv
  FILE_FORMAT = (FORMAT_NAME = 'CC_DEMO.CONTROL.ff_csv_planted')
  ON_ERROR = ABORT_STATEMENT;

EXECUTE IMMEDIATE $$
DECLARE
    v_clean        VARCHAR;
    v_planted      VARCHAR;
    v_res          VARIANT;
    v_before       VARIANT;
    v_after        VARIANT;
    v_run          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
BEGIN
    CREATE OR REPLACE TEMPORARY TABLE tmp_batches AS
    SELECT 'B' || LPAD(ROW_NUMBER() OVER (ORDER BY 1)::VARCHAR, 3, '0') AS batch_id
      FROM TABLE(GENERATOR(ROWCOUNT => 98));

    -- ---------------------------------------------------------------------
    -- T1: clean batch for ACD (no planted ACD issue, no late file in it).
    -- No batch is clean for every source, so the clean batch is per source.
    -- ---------------------------------------------------------------------
    BEGIN
        v_clean := (SELECT MIN(b.batch_id) FROM tmp_batches b
                     WHERE NOT EXISTS (SELECT 1 FROM CC_DEMO.CONTROL.test_planted p
                                        WHERE p.batch_id = b.batch_id
                                          AND p.issue_type IN ('service_level_above_100', 'late_acd_file')));
        CALL CC_DEMO.CONTROL.sp_load_bronze('API_ACD_ALL', :v_clean);
        CALL CC_DEMO.CONTROL.sp_load_silver('API_ACD_ALL', :v_clean);
        SELECT $1 INTO :v_res FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

        INSERT INTO CC_DEMO.CONTROL.test_results
        SELECT 'T1 clean batch: zero quarantined rows (' || :v_clean || ', API_ACD_ALL)',
               IFF(q.n = 0 AND e.n = 0, 'PASS', 'FAIL'), '0', q.n || ' quarantined; ' || e.n || ' ERROR failures',
               'silver status ' || :v_res:status::VARCHAR, :v_run
          FROM (SELECT COUNT(*) AS n FROM CC_DEMO.SILVER.stg_quarantine_record
                 WHERE batch_id = :v_clean AND source_table = 'raw_api_acd_all') q,
               (SELECT COUNT(*) AS n FROM CC_DEMO.CONTROL.dq_result
                 WHERE batch_id = :v_clean AND layer = 'SILVER' AND table_name = 'stg_acd_interval'
                   AND severity = 'ERROR' AND rows_failed > 0) e;
    EXCEPTION
        WHEN OTHER THEN
            INSERT INTO CC_DEMO.CONTROL.test_results
            VALUES ('T1 clean batch', 'FAIL', NULL, NULL, SQLERRM, :v_run);
    END;

    -- ---------------------------------------------------------------------
    -- T2: planted batch. Pipeline run; quarantine per reason == planted count per type.
    -- ---------------------------------------------------------------------
    BEGIN
        v_planted := (SELECT batch_id FROM CC_DEMO.CONTROL.test_planted
                       WHERE issue_type IN ('duplicate_contact_id', 'negative_handle_time', 'agent_not_in_wfm',
                                            'service_level_above_100', 'orphan_segment_call_id')
                       GROUP BY batch_id ORDER BY COUNT(*) DESC LIMIT 1);
        CALL CC_DEMO.CONTROL.sp_run_pipeline(:v_planted, 0.05);
        SELECT $1 INTO :v_res FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

        INSERT INTO CC_DEMO.CONTROL.test_results
        SELECT 'T2 planted batch: pipeline completes (' || :v_planted || ')',
               IFF(:v_res:status::VARCHAR IN ('SUCCEEDED', 'SUCCEEDED_WITH_QUARANTINE'), 'PASS', 'FAIL'),
               'SUCCEEDED or SUCCEEDED_WITH_QUARANTINE', :v_res:status::VARCHAR,
               COALESCE(:v_res:failed_step::VARCHAR, ''), :v_run;

        -- One row per planted type with a quarantine rule. Set-based comparison.
        INSERT INTO CC_DEMO.CONTROL.test_results
        SELECT 'T2 quarantine == planted: ' || m.issue_type,
               IFF(expected = actual, 'PASS', 'FAIL'),
               expected::VARCHAR, actual::VARCHAR,
               m.raw_table || ' / ' || m.reason_code, :v_run
          FROM (SELECT * FROM VALUES
                    ('duplicate_contact_id',       'raw_api_calllog',               'DUPLICATE_KEY'),
                    ('negative_handle_time',       'raw_api_calllog',               'NEGATIVE_HANDLE_TIME'),
                    ('agent_not_in_wfm',           'raw_api_calllog',               'UNKNOWN_AGENT'),
                    ('service_level_above_100',    'raw_api_acd_all',               'SERVICE_LEVEL_OUT_OF_RANGE'),
                    ('orphan_segment_call_id',     'raw_api_call_segment',          'ORPHAN_CALL_ID')
                AS v(issue_type, raw_table, reason_code)) m,
               LATERAL (SELECT
                  (SELECT COUNT(*) FROM CC_DEMO.CONTROL.test_planted p
                    WHERE p.batch_id = :v_planted AND p.issue_type = m.issue_type)       AS expected,
                  (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_quarantine_record q
                    WHERE q.batch_id = :v_planted AND q.source_table = m.raw_table
                      AND q.reason_code = m.reason_code)                                  AS actual) c;
    EXCEPTION
        WHEN OTHER THEN
            INSERT INTO CC_DEMO.CONTROL.test_results
            VALUES ('T2 planted batch', 'FAIL', NULL, NULL, SQLERRM, :v_run);
    END;

    -- ---------------------------------------------------------------------
    -- T3: schema evolution. B059 adds FIRST CALL RESOLUTION and QUEUE OVERFLOW
    -- relative to B058. Both must be logged, and B059 must load.
    -- ---------------------------------------------------------------------
    BEGIN
        CALL CC_DEMO.CONTROL.sp_load_bronze('API_ACD_ALL', 'B058');
        CALL CC_DEMO.CONTROL.sp_load_bronze('API_ACD_ALL', 'B059');
        SELECT $1 INTO :v_res FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

        INSERT INTO CC_DEMO.CONTROL.test_results
        SELECT 'T3 schema evolution: B059 loads (API_ACD_ALL)',
               IFF(:v_res:status::VARCHAR = 'SUCCEEDED', 'PASS', 'FAIL'),
               'SUCCEEDED', :v_res:status::VARCHAR, :v_res:message::VARCHAR, :v_run;

        INSERT INTO CC_DEMO.CONTROL.test_results
        SELECT 'T3 schema evolution: new column logged: ' || c.col,
               IFF(d.n > 0, 'PASS', 'FAIL'), '>= 1', d.n::VARCHAR,
               'B059 NEW_VS_PRIOR_BATCH', :v_run
          FROM (SELECT 'FIRST CALL RESOLUTION' AS col UNION ALL SELECT 'QUEUE OVERFLOW') c,
               LATERAL (SELECT COUNT(*) AS n FROM CC_DEMO.CONTROL.schema_drift_log
                         WHERE batch_id = 'B059' AND source_table = 'API_ACD_ALL'
                           AND column_name = c.col AND detail = 'NEW_VS_PRIOR_BATCH') d;
    EXCEPTION
        WHEN OTHER THEN
            INSERT INTO CC_DEMO.CONTROL.test_results
            VALUES ('T3 schema evolution', 'FAIL', NULL, NULL, SQLERRM, :v_run);
    END;

    -- ---------------------------------------------------------------------
    -- T4: re-run the planted batch. Row counts in every layer must not change.
    -- ---------------------------------------------------------------------
    BEGIN
        v_before := OBJECT_CONSTRUCT(
            'stg_call',      (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_call WHERE _batch_id = :v_planted),
            'stg_segment',   (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_call_segment WHERE _batch_id = :v_planted),
            'quarantine',    (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_quarantine_record WHERE batch_id = :v_planted),
            'dq_result',     (SELECT COUNT(*) FROM CC_DEMO.CONTROL.dq_result WHERE batch_id = :v_planted),
            'fact_call',     (SELECT COUNT(*) FROM CC_DEMO.GOLD.fact_call_daily),
            'fact_repeat',   (SELECT COUNT(*) FROM CC_DEMO.GOLD.fact_repeat_call),
            'fact_sl',       (SELECT COUNT(*) FROM CC_DEMO.GOLD.fact_service_level_interval));

        CALL CC_DEMO.CONTROL.sp_load_bronze('API_CALLLOG', :v_planted);
        SELECT $1 INTO :v_res FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
        INSERT INTO CC_DEMO.CONTROL.test_results
        SELECT 'T4 re-run: bronze skips a loaded batch (' || :v_planted || ')',
               IFF(:v_res:status::VARCHAR = 'SKIPPED_ALREADY_LOADED', 'PASS', 'FAIL'),
               'SKIPPED_ALREADY_LOADED', :v_res:status::VARCHAR, NULL, :v_run;

        CALL CC_DEMO.CONTROL.sp_run_pipeline(:v_planted, 0.05);

        v_after := OBJECT_CONSTRUCT(
            'stg_call',      (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_call WHERE _batch_id = :v_planted),
            'stg_segment',   (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_call_segment WHERE _batch_id = :v_planted),
            'quarantine',    (SELECT COUNT(*) FROM CC_DEMO.SILVER.stg_quarantine_record WHERE batch_id = :v_planted),
            'dq_result',     (SELECT COUNT(*) FROM CC_DEMO.CONTROL.dq_result WHERE batch_id = :v_planted),
            'fact_call',     (SELECT COUNT(*) FROM CC_DEMO.GOLD.fact_call_daily),
            'fact_repeat',   (SELECT COUNT(*) FROM CC_DEMO.GOLD.fact_repeat_call),
            'fact_sl',       (SELECT COUNT(*) FROM CC_DEMO.GOLD.fact_service_level_interval));

        INSERT INTO CC_DEMO.CONTROL.test_results
        SELECT 'T4 re-run: no new rows in any layer (' || :v_planted || ')',
               IFF(:v_before = :v_after, 'PASS', 'FAIL'),
               :v_before::VARCHAR, :v_after::VARCHAR, NULL, :v_run;
    EXCEPTION
        WHEN OTHER THEN
            INSERT INTO CC_DEMO.CONTROL.test_results
            VALUES ('T4 re-run', 'FAIL', NULL, NULL, SQLERRM, :v_run);
    END;

    -- ---------------------------------------------------------------------
    -- T5: planted issue types that have no DQ rule. Reported, not passed or failed.
    -- ---------------------------------------------------------------------
    INSERT INTO CC_DEMO.CONTROL.test_results
    SELECT 'T5 no rule: ' || p.issue_type, 'NO_RULE',
           p.n::VARCHAR, NULL,
           CASE p.issue_type
             WHEN 'late_acd_file' THEN 'ACD file for 2026-03-30 lands in B030; no batch-completeness rule'
             WHEN 'duplicate_column_name' THEN 'no planted instance; a repeated header name would fail the bronze load'
           END, :v_run
      FROM (SELECT issue_type, COUNT(*) AS n FROM CC_DEMO.CONTROL.test_planted
             WHERE issue_type IN ('late_acd_file', 'duplicate_column_name') GROUP BY issue_type
            UNION ALL SELECT 'duplicate_column_name', 0
             WHERE NOT EXISTS (SELECT 1 FROM CC_DEMO.CONTROL.test_planted WHERE issue_type = 'duplicate_column_name')) p;
END;
$$;

-- Summary. Any FAIL means the procedures or the expectations need a look.
SELECT test_name, status, expected, actual, detail
  FROM CC_DEMO.CONTROL.test_results
 WHERE run_ts = (SELECT MAX(run_ts) FROM CC_DEMO.CONTROL.test_results)
 ORDER BY test_name;
