-- =============================================================================
-- 03_sp_load_agent_scd2.sql  (Snowflake Scripting)
-- =============================================================================
-- CC_DEMO.CONTROL.sp_load_agent_scd2(p_batch_id) RETURNS VARIANT
--
-- Type 2 agent dimension: SILVER.dim_agent_scd2.
--
-- Business key: agent_id. Tracked attribute: agent_group.
-- A new version is written only when the tracked attribute changes. The prior
-- version is closed on the day before the change.
--
-- Sources for the batch:
--   * SILVER.stg_call     agents who took calls in this batch (after DQ006, so
--                         an agent_id that is not on the WFM extract is never added)
--   * SILVER.stg_wfm_daily  agents on the WFM extract for this batch's dates,
--                         including agents with no calls yet (group = NULL)
--
-- ASSUMPTION (see sql/ddl/02_silver.sql): the workbook has no agent effective
-- dates. effective_start_date is the first date an agent_id appears for that
-- group. Replace this with the client's effective dates when they arrive.
--
-- Idempotent: re-running a batch compares each observation with the open
-- version. If the group is the same, nothing changes.
--
-- Names: agent_first_name, agent_last_name, and agent_email are stored as the
-- masked value '***' (PII rule). They are not tracked attributes, so a change in
-- name does not create a version.
--
-- Returns: {batch_id, source_table, status, rows_in, rows_loaded, rows_quarantined,
--           dq_errors, dq_warnings, message}. rows_loaded = versions opened here.
-- =============================================================================

CREATE OR REPLACE PROCEDURE CC_DEMO.CONTROL.sp_load_agent_scd2(p_batch_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_done     NUMBER;
    v_obs      NUMBER;
    v_closed   NUMBER;
    v_opened   NUMBER;
    v_inwfm    NUMBER;
    v_started  TIMESTAMP_NTZ;
BEGIN
    v_started := CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ;

    -- The silver load for this batch must have run for both sources.
    SELECT COUNT(*) INTO :v_done FROM CC_DEMO.CONTROL.load_log
     WHERE batch_id = :p_batch_id AND source_table IN ('API_CALLLOG', 'WFM_DAILY') AND status = 'SUCCEEDED';
    IF (v_done < 2) THEN
        RETURN OBJECT_CONSTRUCT('batch_id', p_batch_id, 'source_table', 'DIM_AGENT_SCD2',
            'status', 'FAILED', 'message', 'bronze for API_CALLLOG and WFM_DAILY must be loaded first');
    END IF;

    -- Observations: one row per agent_id and group in this batch, with its first date.
    -- Agents seen only on WFM get a NULL group.
    CREATE OR REPLACE TEMPORARY TABLE tmp_agent_obs AS
    SELECT agent_id, agent_group, MIN(call_date_et) AS obs_date
      FROM CC_DEMO.SILVER.stg_call
     WHERE _batch_id = :p_batch_id AND agent_id IS NOT NULL
     GROUP BY agent_id, agent_group
    UNION ALL
    SELECT w.agent_id, NULL AS agent_group, MIN(w.work_date) AS obs_date
      FROM CC_DEMO.SILVER.stg_wfm_daily w
     WHERE w._batch_id = :p_batch_id
       AND NOT EXISTS (SELECT 1 FROM CC_DEMO.SILVER.stg_call c
                        WHERE c.agent_id = w.agent_id AND c._batch_id = :p_batch_id)
     GROUP BY w.agent_id;

    -- One observation per agent_id (a batch with two groups for one agent keeps the earliest).
    CREATE OR REPLACE TEMPORARY TABLE tmp_obs AS
    SELECT agent_id, agent_group, obs_date,
           EXISTS (SELECT 1 FROM CC_DEMO.SILVER.stg_wfm_daily w
                    WHERE w.agent_id = o.agent_id) AS in_wfm
      FROM (SELECT o.*, ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY obs_date, agent_group) AS rn
              FROM tmp_agent_obs o) o
     WHERE rn = 1;
    SELECT COUNT(*) INTO :v_obs FROM tmp_obs;

    -- Observations that change the open version, or that have no open version.
    CREATE OR REPLACE TEMPORARY TABLE tmp_changed AS
    SELECT o.agent_id, o.agent_group, o.obs_date, o.in_wfm,
           d.agent_key AS open_key
      FROM tmp_obs o
      LEFT JOIN CC_DEMO.SILVER.dim_agent_scd2 d
        ON d.agent_id = o.agent_id AND d.is_current
     WHERE d.agent_key IS NULL
        OR d.agent_group IS DISTINCT FROM o.agent_group;

    BEGIN TRANSACTION;

    -- Close the open version. effective_end_date is the day before the change.
    -- GREATEST keeps a version from ending before it starts.
    UPDATE CC_DEMO.SILVER.dim_agent_scd2 d
       SET effective_end_date = GREATEST(c.obs_date - 1, d.effective_start_date),
           is_current = FALSE
      FROM tmp_changed c
     WHERE d.agent_id = c.agent_id AND d.is_current AND c.open_key IS NOT NULL;
    SELECT COUNT(*) INTO :v_closed FROM tmp_changed WHERE open_key IS NOT NULL;

    -- Open the new version.
    INSERT INTO CC_DEMO.SILVER.dim_agent_scd2
        (agent_id, agent_group, agent_first_name, agent_last_name, agent_email,
         effective_start_date, effective_end_date, is_current, in_wfm_flag, row_hash, _batch_id, _load_ts)
    SELECT c.agent_id, c.agent_group, '***', '***', '***',
           c.obs_date, DATE '9999-12-31', TRUE, IFF(c.in_wfm, 1, 0),
           SHA2(CONCAT_WS('|', c.agent_id, COALESCE(c.agent_group, '<null>')), 256),
           :p_batch_id, :v_started
      FROM tmp_changed c;
    SELECT COUNT(*) INTO :v_opened FROM tmp_changed;

    -- Non-tracked flag: keep in_wfm_flag current on open versions.
    UPDATE CC_DEMO.SILVER.dim_agent_scd2 d
       SET in_wfm_flag = IFF(o.in_wfm, 1, 0)
      FROM tmp_obs o
     WHERE d.agent_id = o.agent_id AND d.is_current
       AND d.in_wfm_flag IS DISTINCT FROM IFF(o.in_wfm, 1, 0);
    SELECT COUNT(*) INTO :v_inwfm FROM tmp_obs WHERE in_wfm;

    COMMIT;

    RETURN OBJECT_CONSTRUCT(
        'batch_id', p_batch_id,
        'source_table', 'DIM_AGENT_SCD2',
        'status', 'SUCCEEDED',
        'rows_in', v_obs,
        'rows_loaded', v_opened,
        'rows_quarantined', 0,
        'dq_errors', 0,
        'dq_warnings', 0,
        'message', 'versions closed: ' || v_closed || '; versions opened: ' || v_opened
                   || '; agents on WFM in batch: ' || v_inwfm);

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        INSERT INTO CC_DEMO.CONTROL.load_log (batch_id, source_table, source_file, rows_read, rows_loaded,
                                              rows_skipped_duplicate_batch, status, error_message, started_ts, ended_ts)
        SELECT :p_batch_id, 'DIM_AGENT_SCD2', NULL, 0, 0, 0, 'FAILED', SQLERRM, :v_started,
               CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ;
        RAISE;
END;
$$;
