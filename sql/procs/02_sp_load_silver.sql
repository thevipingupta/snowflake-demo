-- =============================================================================
-- 02_sp_load_silver.sql  (Snowflake Scripting)
-- =============================================================================
-- CC_DEMO.CONTROL.sp_load_silver(p_source_table, p_batch_id) RETURNS VARIANT
--
-- Transforms one batch from BRONZE into the matching SILVER table.
--
--   API_CALLLOG                  -> SILVER.stg_call
--   API_CALL_SEGMENTREPORTALL    -> SILVER.stg_call_segment
--   API_VOICEMAIL_LIFECYCLE_LOG  -> SILVER.stg_voicemail
--   API_ACD_ALL                  -> SILVER.stg_acd_interval
--   WFM_DAILY                    -> SILVER.stg_wfm_daily   (ASSUMPTION source)
--
-- Order matters. Run the WFM batch first (DQ006 checks agents against it), then
-- ACD, CALLLOG, CALL_SEGMENT, VOICEMAIL. sp_run_pipeline does this.
--
-- How each batch is processed (set-based, no cursor loops):
--   1. Dedupe on the business key. The most recent _load_ts wins; ties go to
--      the lower _record_hash. Extra copies are rejected as DUPLICATE_KEY.
--   2. Parse values with TRY_ functions and parse_duration_seconds. A bad value
--      rejects that row (reason INVALID_VALUE). The load does not fail.
--   3. Each rejected row gets one reason code (the first rule it fails) and goes
--      to SILVER.stg_quarantine_record with its raw payload. Surviving rows go
--      to the silver table.
--   4. Mask PII (is_pii = Y in docs/source_column_map.csv) to '***' after the
--      customer_key is computed from the raw ANI / SALESFORCE_ID.
--   5. Run the DQ rules and write one row per rule to CONTROL.dq_result.
--   6. MERGE into the silver table on the business key, so a re-run does not
--      add rows. Quarantine rows for the batch are replaced, not appended.
--
-- Status: SUCCEEDED, or SUCCEEDED_WITH_QUARANTINE when any row was rejected.
--   Rows rejected by a rule are quarantined. The pipeline does not stop on them.
--   A failed step (exception) or a failed reconciliation stops the pipeline.
--
-- Returns: {batch_id, source_table, status, rows_in, rows_loaded, rows_quarantined,
--           dq_errors, dq_warnings, message}
--   dq_errors / dq_warnings = number of ERROR / WARN rules with at least one failure.
--
-- Requires: session variable CUSTOMER_KEY_SECRET (see 00_prereq.sql).
-- =============================================================================

CREATE OR REPLACE PROCEDURE CC_DEMO.CONTROL.sp_load_silver(p_source_table VARCHAR, p_batch_id VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    e_not_loaded EXCEPTION (-20201, 'bronze batch is not loaded; run sp_load_bronze first');
    e_no_secret  EXCEPTION (-20202, 'session variable CUSTOMER_KEY_SECRET is not set');
    e_unknown_source EXCEPTION (-20203, 'no silver transform is defined for this source_table');
    v_raw        VARCHAR;
    v_silver     VARCHAR;
    v_key_cond   VARCHAR;
    v_sql        VARCHAR;
    v_secret     VARCHAR;
    v_done       NUMBER;
    v_rows_in    NUMBER;
    v_ok         NUMBER;
    v_quar       NUMBER;
    v_dq_err     NUMBER;
    v_dq_warn    NUMBER;
    v_started    TIMESTAMP_NTZ;
BEGIN
    v_started := CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ;

    SELECT raw_table, silver_table INTO :v_raw, :v_silver
      FROM CC_DEMO.CONTROL.source_config WHERE source_table = :p_source_table;

    SELECT COUNT(*) INTO :v_done FROM CC_DEMO.CONTROL.load_log
     WHERE batch_id = :p_batch_id AND source_table = :p_source_table AND status = 'SUCCEEDED';
    IF (v_done = 0) THEN
        RAISE e_not_loaded;
    END IF;

    v_secret := GETVARIABLE('CUSTOMER_KEY_SECRET');
    IF (v_secret IS NULL) THEN
        RAISE e_no_secret;
    END IF;

    -- Shared work tables. Filled by the branch below, read by the writes at the end.
    CREATE OR REPLACE TEMPORARY TABLE tmp_quar (
        record_key VARCHAR, reason_code VARCHAR, reason_detail VARCHAR, payload VARIANT);
    CREATE OR REPLACE TEMPORARY TABLE tmp_dq (
        rule_id VARCHAR, rows_checked NUMBER, rows_failed NUMBER, severity VARCHAR);

    -- ------------------------------------------------------------------------
    -- Branch: API_CALLLOG -> stg_call
    -- ------------------------------------------------------------------------
    IF (p_source_table = 'API_CALLLOG') THEN
        v_key_cond := 't.contact_id = s.contact_id';

        CREATE OR REPLACE TEMPORARY TABLE tmp_src AS
        SELECT r.*, OBJECT_CONSTRUCT(r.*) AS raw_payload,
               ROW_NUMBER() OVER (PARTITION BY r."CONTACT ID"
                                  ORDER BY r._load_ts DESC, r._record_hash) AS rn
          FROM CC_DEMO.BRONZE.raw_api_calllog r
         WHERE r._batch_id = :p_batch_id;
        SELECT COUNT(*) INTO :v_rows_in FROM tmp_src;

        CREATE OR REPLACE TEMPORARY TABLE tmp_parsed AS
        SELECT s.*,
               TRY_TO_TIMESTAMP_NTZ(s."TIMESTAMP")                              AS call_start_utc,
               CONVERT_TIMEZONE('UTC', 'America/New_York',
                                TRY_TO_TIMESTAMP_NTZ(s."TIMESTAMP"))            AS call_start_et,
               parse_duration_seconds(s."HANDLE TIME")                          AS handle_seconds,
               parse_duration_seconds(s."HOLD TIME")                            AS hold_seconds,
               parse_duration_seconds(s."TALK TIME")                            AS talk_seconds,
               parse_duration_seconds(s."AFTER CALL WORK TIME")                 AS acw_seconds,
               parse_duration_seconds(s."RING TIME")                            AS ring_seconds,
               parse_duration_seconds(s."QUEUE WAIT TIME")                      AS queue_wait_seconds,
               parse_duration_seconds(s."TIME TO ABANDON")                      AS tta_seconds,
               TRY_TO_NUMBER(s."ABANDONED")                                     AS abandoned_flag,
               TRY_TO_NUMBER(s."CONTACTED")                                     AS contacted_flag,
               TRY_TO_NUMBER(s."SERVICE LEVEL")                                 AS service_level_flag,
               TRY_TO_NUMBER(s."SOA MIN/THRESHOLD (SEC)")                       AS soa_threshold_sec,
               TRY_TO_NUMBER(s."FIRST CALL RESOLUTION")                         AS fcr_flag,
               TRY_TO_NUMBER(s."QUEUE OVERFLOW")                                AS overflow_flag,
               DATE(CONVERT_TIMEZONE('UTC', 'America/New_York',
                                     TRY_TO_TIMESTAMP_NTZ(s."TIMESTAMP")))     AS call_date_et,
               COALESCE(NULLIF(s."SALESFORCE_ID", ''), NULLIF(s."ANI", ''))     AS key_source
          FROM tmp_src s;

        -- One reason per row: the first rule that fails, in this order.
        CREATE OR REPLACE TEMPORARY TABLE tmp_cls AS
        SELECT p.*,
               CASE
                 WHEN p."CONTACT ID" IS NULL OR TRIM(p."CONTACT ID") = ''               THEN 'NULL_KEY'
                 WHEN p.rn > 1                                                          THEN 'DUPLICATE_KEY'
                 WHEN p.call_start_utc IS NULL                                          THEN 'INVALID_TIMESTAMP'
                 WHEN LOWER(TRIM(p."MEDIA TYPE")) IN ('web_form', 'secure_message')     THEN 'UNSUPPORTED_CHANNEL'
                 WHEN p.handle_seconds < 0                                              THEN 'NEGATIVE_HANDLE_TIME'
                 WHEN LEAST(COALESCE(p.hold_seconds, 0), COALESCE(p.talk_seconds, 0),
                            COALESCE(p.acw_seconds, 0), COALESCE(p.ring_seconds, 0),
                            COALESCE(p.queue_wait_seconds, 0)) < 0                      THEN 'NEGATIVE_DURATION'
                 WHEN (p."HANDLE TIME" IS NOT NULL AND p.handle_seconds IS NULL)
                   OR (p."HOLD TIME" IS NOT NULL AND p.hold_seconds IS NULL)            THEN 'INVALID_VALUE'
                 WHEN p."AGENT ID" IS NOT NULL AND NOT EXISTS (
                        SELECT 1 FROM CC_DEMO.SILVER.stg_wfm_daily w
                         WHERE w.agent_id = p."AGENT ID" AND w.work_date = p.call_date_et)   THEN 'UNKNOWN_AGENT'
               END AS reason_code
          FROM tmp_parsed p;

        INSERT INTO tmp_quar (record_key, reason_code, reason_detail, payload)
        SELECT c."CONTACT ID", c.reason_code,
               'HANDLE TIME=' || COALESCE(c."HANDLE TIME", '') || '; MEDIA TYPE=' || COALESCE(c."MEDIA TYPE", '')
               || '; AGENT ID=' || COALESCE(c."AGENT ID", ''),
               c.raw_payload
          FROM tmp_cls c WHERE c.reason_code IS NOT NULL;

        CREATE OR REPLACE TEMPORARY TABLE tmp_ok AS
        SELECT
            c."CONTACT ID"                       AS contact_id,
            c."CALL ID"                          AS call_id,
            c.call_date_et                       AS call_date_et,
            c.call_start_utc                     AS call_start_utc,
            c.call_start_et                      AS call_start_et,
            c."MEDIA TYPE"                       AS channel,
            c."SKILL"                            AS skill,
            c."CAMPAIGN"                         AS campaign,
            c."CALL TYPE"                        AS call_type,
            c."AGENT ID"                         AS agent_id,
            c."AGENT GROUP"                      AS agent_group,
            IFF(c."AGENT ID" IS NULL, NULL, 1)   AS agent_in_wfm_flag,
            c."DISPOSITION"                      AS disposition,
            c."DISPOSITION GROUP A"              AS disposition_group_a,
            c."DISPOSITION GROUP B"              AS disposition_group_b,
            c."DISPOSITION GROUP C"              AS disposition_group_c,
            c."DISPOSITION PATH"                 AS disposition_path,
            c.abandoned_flag                     AS abandoned_flag,
            c.contacted_flag                     AS contacted_flag,
            c.service_level_flag                 AS service_level_flag,
            c.soa_threshold_sec                  AS soa_threshold_sec,
            c.handle_seconds                     AS handle_seconds,
            c.hold_seconds                       AS hold_seconds,
            c.talk_seconds                       AS talk_seconds,
            c.acw_seconds                        AS after_call_work_seconds,
            c.ring_seconds                       AS ring_seconds,
            c.queue_wait_seconds                 AS queue_wait_seconds,
            c.tta_seconds                        AS time_to_abandon_seconds,
            c.fcr_flag                           AS first_call_resolution_flag,
            c.overflow_flag                      AS queue_overflow_flag,
            -- customer_key is computed from raw ANI / SALESFORCE_ID before masking.
            IFF(c.key_source IS NULL, NULL,
                SHA2(CONCAT(:v_secret, '|', c.key_source), 256))                     AS customer_key,
            IFF(c."ANI" IS NULL, NULL, '***')                                         AS ani,
            IFF(c."CUSTOMER NAME" IS NULL, NULL, '***')                              AS customer_name,
            IFF(c."FIRST_NAME" IS NULL, NULL, '***')                                 AS first_name,
            IFF(c."LAST_NAME" IS NULL, NULL, '***')                                  AS last_name,
            IFF(c."EMAIL" IS NULL, NULL, '***')                                      AS email,
            IFF(c."STREET" IS NULL, NULL, '***')                                     AS street,
            IFF(c."CITY" IS NULL, NULL, '***')                                       AS city,
            IFF(c."ZIP" IS NULL, NULL, '***')                                        AS zip,
            c."SALESFORCE_ID"                                                        AS salesforce_id,
            IFF(c."AGENT EMAIL" IS NULL, NULL, '***')                                AS agent_email,
            IFF(c."AGENT FIRST NAME" IS NULL, NULL, '***')                           AS agent_first_name,
            IFF(c."AGENT LAST NAME" IS NULL, NULL, '***')                            AS agent_last_name,
            c."FILE_NAME"                                                            AS five9_file_name,
            TRY_TO_TIMESTAMP_NTZ(c."LOAD_TIMESTAMP")                                 AS five9_load_timestamp,
            c._batch_id, c._source_file, c._load_ts, c._record_hash,
            -- DQ004 (warning): case-related disposition with no customer key.
            IFF(c.key_source IS NULL
                AND EXISTS (SELECT 1 FROM CC_DEMO.CONTROL.ref_case_disposition r
                             WHERE r.disposition = c."DISPOSITION" AND r.requires_followup),
                'WARN', 'PASS')                                                      AS dq_status
          FROM tmp_cls c
         WHERE c.reason_code IS NULL;

        -- Data-quality rules for this batch. Business meaning is in the header and below.
        -- DQ001 (error): every CONTACT ID is present and unique within the batch.
        --        Duplicate copies are rejected; the most recent copy is kept.
        INSERT INTO tmp_dq SELECT 'DQ001', COUNT(*), COUNT_IF(reason_code IN ('NULL_KEY', 'DUPLICATE_KEY')), 'ERROR' FROM tmp_cls;
        -- DQ002 (error): durations are not negative. A negative handle time is a
        --        logging error and cannot be used in service-level or workload reports.
        INSERT INTO tmp_dq SELECT 'DQ002', COUNT(*), COUNT_IF(reason_code IN ('NEGATIVE_HANDLE_TIME', 'NEGATIVE_DURATION', 'INVALID_VALUE')), 'ERROR' FROM tmp_cls;
        -- DQ004 (warning): a case-related disposition with no customer key cannot be
        --        followed up. Flagged, not rejected. The list is a placeholder until VEC confirms it.
        INSERT INTO tmp_dq SELECT 'DQ004', COUNT(*), COUNT_IF(dq_status = 'WARN'), 'WARN' FROM tmp_ok;
        -- DQ006 (error): every answered call's AGENT ID is on the WFM extract for that
        --        date. The WFM extract is the reference, not dim_agent (dim_agent is
        --        built from call data, so it cannot prove an agent is real).
        INSERT INTO tmp_dq SELECT 'DQ006', COUNT_IF("AGENT ID" IS NOT NULL), COUNT_IF(reason_code = 'UNKNOWN_AGENT'), 'ERROR' FROM tmp_cls;
        -- DQ009 (error, proposed; not in the brief): channel and timestamp validity.
        INSERT INTO tmp_dq SELECT 'DQ009', COUNT(*), COUNT_IF(reason_code IN ('UNSUPPORTED_CHANNEL', 'INVALID_TIMESTAMP')), 'ERROR' FROM tmp_cls;

    -- ------------------------------------------------------------------------
    -- Branch: API_CALL_SEGMENTREPORTALL -> stg_call_segment
    -- ------------------------------------------------------------------------
    ELSEIF (p_source_table = 'API_CALL_SEGMENTREPORTALL') THEN
        v_key_cond := 't.call_segment_id = s.call_segment_id';

        CREATE OR REPLACE TEMPORARY TABLE tmp_src AS
        SELECT r.*, OBJECT_CONSTRUCT(r.*) AS raw_payload,
               ROW_NUMBER() OVER (PARTITION BY r."CALL SEGMENT ID"
                                  ORDER BY r._load_ts DESC, r._record_hash) AS rn
          FROM CC_DEMO.BRONZE.raw_api_call_segment r
         WHERE r._batch_id = :p_batch_id;
        SELECT COUNT(*) INTO :v_rows_in FROM tmp_src;

        CREATE OR REPLACE TEMPORARY TABLE tmp_parsed AS
        SELECT s.*,
               TRY_TO_TIMESTAMP_NTZ(s."TIMESTAMP")                                 AS seg_start_utc,
               CONVERT_TIMEZONE('UTC', 'America/New_York', TRY_TO_TIMESTAMP_NTZ(s."TIMESTAMP")) AS seg_start_et,
               DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', TRY_TO_TIMESTAMP_NTZ(s."TIMESTAMP"))) AS seg_date_et,
               parse_duration_seconds(s."SEGMENT TIME")                             AS segment_seconds,
               parse_duration_seconds(s."QUEUE WAIT TIME")                          AS queue_wait_seconds,
               parse_duration_seconds(s."HANDLE TIME")                              AS handle_seconds,
               parse_duration_seconds(s."TALK TIME")                                AS talk_seconds,
               parse_duration_seconds(s."HOLD TIME")                                AS hold_seconds,
               parse_duration_seconds(s."IVR TIME")                                 AS ivr_seconds,
               TRY_TO_NUMBER(s."ABANDONED")                                         AS abandoned_flag,
               TRY_TO_NUMBER(s."SERVICE LEVEL")                                     AS service_level_flag
          FROM tmp_src s;

        CREATE OR REPLACE TEMPORARY TABLE tmp_cls AS
        SELECT p.*,
               CASE
                 WHEN p."CALL SEGMENT ID" IS NULL OR TRIM(p."CALL SEGMENT ID") = ''     THEN 'NULL_KEY'
                 WHEN p.rn > 1                                                          THEN 'DUPLICATE_KEY'
                 WHEN p.seg_start_utc IS NULL                                           THEN 'INVALID_TIMESTAMP'
                 WHEN LEAST(COALESCE(p.segment_seconds, 0), COALESCE(p.queue_wait_seconds, 0),
                            COALESCE(p.handle_seconds, 0), COALESCE(p.talk_seconds, 0),
                            COALESCE(p.hold_seconds, 0), COALESCE(p.ivr_seconds, 0)) < 0 THEN 'NEGATIVE_DURATION'
                 WHEN NOT EXISTS (SELECT 1 FROM CC_DEMO.BRONZE.raw_api_calllog b
                                   WHERE b."CALL ID" = p."CALL ID")                     THEN 'ORPHAN_CALL_ID'
                 WHEN NOT EXISTS (SELECT 1 FROM CC_DEMO.SILVER.stg_call c
                                   WHERE c.call_id = p."CALL ID")                       THEN 'PARENT_CALL_REJECTED'
               END AS reason_code
          FROM tmp_parsed p;

        INSERT INTO tmp_quar (record_key, reason_code, reason_detail, payload)
        SELECT c."CALL SEGMENT ID", c.reason_code,
               'CALL ID=' || COALESCE(c."CALL ID", '') || '; SEGMENT TYPE=' || COALESCE(c."SEGMENT TYPE", ''),
               c.raw_payload
          FROM tmp_cls c WHERE c.reason_code IS NOT NULL;

        CREATE OR REPLACE TEMPORARY TABLE tmp_ok AS
        SELECT
            c."CALL SEGMENT ID"           AS call_segment_id,
            c."CALL ID"                   AS call_id,
            c."CONTACT ID"                AS contact_id,
            c.seg_date_et                 AS segment_date_et,
            c.seg_start_utc               AS segment_start_utc,
            c.seg_start_et                AS segment_start_et,
            c."SEGMENT TYPE"              AS segment_type,
            c."RESULT"                    AS result,
            c."MEDIA TYPE"                AS channel,
            c."SKILL"                     AS skill,
            c."AGENT"                     AS agent_id,
            c."DISPOSITION GROUP A"       AS disposition_group_a,
            c.segment_seconds, c.queue_wait_seconds, c.handle_seconds, c.talk_seconds,
            c.hold_seconds, c.ivr_seconds, c.abandoned_flag, c.service_level_flag,
            c."FILE_NAME"                 AS five9_file_name,
            TRY_TO_TIMESTAMP_NTZ(c."LOAD_TIMESTAMP") AS five9_load_timestamp,
            c._batch_id, c._source_file, c._load_ts, c._record_hash,
            'PASS'                        AS dq_status
          FROM tmp_cls c
         WHERE c.reason_code IS NULL;

        -- DQ001 (error): every CALL SEGMENT ID is present and unique within the batch.
        INSERT INTO tmp_dq SELECT 'DQ001', COUNT(*), COUNT_IF(reason_code IN ('NULL_KEY', 'DUPLICATE_KEY')), 'ERROR' FROM tmp_cls;
        -- DQ002 (error): durations are not negative.
        INSERT INTO tmp_dq SELECT 'DQ002', COUNT(*), COUNT_IF(reason_code = 'NEGATIVE_DURATION'), 'ERROR' FROM tmp_cls;
        -- DQ005 (error): every call segment has a matching call in stg_call. A segment
        --        whose CALL ID is not in bronze at all is an orphan. A segment whose call
        --        was itself rejected is reported separately (PARENT_CALL_REJECTED), so the
        --        orphan count reflects the source problem only.
        INSERT INTO tmp_dq SELECT 'DQ005', COUNT(*), COUNT_IF(reason_code IN ('ORPHAN_CALL_ID', 'PARENT_CALL_REJECTED')), 'ERROR' FROM tmp_cls;

    -- ------------------------------------------------------------------------
    -- Branch: API_VOICEMAIL_LIFECYCLE_LOG -> stg_voicemail
    -- ------------------------------------------------------------------------
    ELSEIF (p_source_table = 'API_VOICEMAIL_LIFECYCLE_LOG') THEN
        v_key_cond := 't.voicemail_id = s.voicemail_id';

        CREATE OR REPLACE TEMPORARY TABLE tmp_src AS
        SELECT r.*, OBJECT_CONSTRUCT(r.*) AS raw_payload,
               ROW_NUMBER() OVER (PARTITION BY r."VOICEMAIL ID"
                                  ORDER BY r._load_ts DESC, r._record_hash) AS rn
          FROM CC_DEMO.BRONZE.raw_api_voicemail_lifecycle r
         WHERE r._batch_id = :p_batch_id;
        SELECT COUNT(*) INTO :v_rows_in FROM tmp_src;

        CREATE OR REPLACE TEMPORARY TABLE tmp_parsed AS
        SELECT s.*,
               TRY_TO_DATE(s."RECEIVED DATE")                                       AS received_date_et,
               TRY_TO_TIMESTAMP_NTZ(s."RECEIVED TIMESTAMP")                         AS received_ts_utc,
               CONVERT_TIMEZONE('UTC', 'America/New_York', TRY_TO_TIMESTAMP_NTZ(s."RECEIVED TIMESTAMP")) AS received_ts_et,
               TRY_TO_TIMESTAMP_NTZ(s."RETURNED TIMESTAMP")                         AS returned_ts_utc,
               parse_duration_seconds(s."TIME TO PROCESSED")                        AS time_to_process_seconds,
               parse_duration_seconds(s."TIME TO RETURN")                           AS time_to_return_seconds,
               parse_duration_seconds(s."HANDLE TIME")                              AS handle_seconds,
               parse_duration_seconds(s."QUEUE WAIT TIME")                          AS queue_wait_seconds,
               TRY_TO_NUMBER(s."PROCESSED")                                         AS processed_flag,
               TRY_TO_NUMBER(s."LISTENED")                                          AS listened_flag,
               TRY_TO_NUMBER(s."DELETED")                                           AS deleted_flag,
               COALESCE(NULLIF(s."SALESFORCE_ID", ''), NULLIF(s."ANI", ''))         AS key_source
          FROM tmp_src s;

        CREATE OR REPLACE TEMPORARY TABLE tmp_cls AS
        SELECT p.*,
               CASE
                 WHEN p."VOICEMAIL ID" IS NULL OR TRIM(p."VOICEMAIL ID") = ''          THEN 'NULL_KEY'
                 WHEN p.rn > 1                                                         THEN 'DUPLICATE_KEY'
                 WHEN p.received_ts_utc IS NULL                                        THEN 'INVALID_TIMESTAMP'
                 WHEN LEAST(COALESCE(p.time_to_process_seconds, 0), COALESCE(p.time_to_return_seconds, 0),
                            COALESCE(p.handle_seconds, 0), COALESCE(p.queue_wait_seconds, 0)) < 0 THEN 'NEGATIVE_DURATION'
                 WHEN NOT EXISTS (SELECT 1 FROM CC_DEMO.BRONZE.raw_api_calllog b
                                   WHERE b."CONTACT ID" = p."CONTACT ID")               THEN 'ORPHAN_CONTACT_ID'
                 WHEN NOT EXISTS (SELECT 1 FROM CC_DEMO.SILVER.stg_call c
                                   WHERE c.contact_id = p."CONTACT ID")                 THEN 'PARENT_CALL_REJECTED'
               END AS reason_code
          FROM tmp_parsed p;

        INSERT INTO tmp_quar (record_key, reason_code, reason_detail, payload)
        SELECT c."VOICEMAIL ID", c.reason_code,
               'CONTACT ID=' || COALESCE(c."CONTACT ID", ''), c.raw_payload
          FROM tmp_cls c WHERE c.reason_code IS NOT NULL;

        CREATE OR REPLACE TEMPORARY TABLE tmp_ok AS
        SELECT
            c."VOICEMAIL ID"                   AS voicemail_id,
            c."CALL ID"                        AS call_id,
            c."CONTACT ID"                     AS contact_id,
            c.received_date_et                 AS received_date_et,
            c.received_ts_utc                  AS received_ts_utc,
            c.received_ts_et                   AS received_ts_et,
            c."VOICEMAIL STATUS"               AS voicemail_status,
            c."VOICEMAIL TYPE"                 AS voicemail_type,
            c."SKILL"                          AS skill,
            c.processed_flag, c.listened_flag, c.deleted_flag,
            c."RETURNED CALL ID"               AS returned_call_id,
            c.returned_ts_utc,
            c.time_to_process_seconds, c.time_to_return_seconds, c.handle_seconds, c.queue_wait_seconds,
            IFF(c.key_source IS NULL, NULL, SHA2(CONCAT(:v_secret, '|', c.key_source), 256)) AS customer_key,
            IFF(c."ANI" IS NULL, NULL, '***')                                  AS ani,
            IFF(c."NAME" IS NULL, NULL, '***')                                 AS customer_name,
            IFF(c."EMAIL" IS NULL, NULL, '***')                                AS email,
            IFF(c."STREET" IS NULL, NULL, '***')                               AS street,
            IFF(c."CITY" IS NULL, NULL, '***')                                 AS city,
            IFF(c."ZIP" IS NULL, NULL, '***')                                  AS zip,
            c."SALESFORCE_ID"                                                  AS salesforce_id,
            c."FILE_NAME"                                                      AS five9_file_name,
            TRY_TO_TIMESTAMP_NTZ(c."LOAD_TIMESTAMP")                           AS five9_load_timestamp,
            c._batch_id, c._source_file, c._load_ts, c._record_hash,
            'PASS'                                                             AS dq_status
          FROM tmp_cls c
         WHERE c.reason_code IS NULL;

        -- DQ001 (error): every VOICEMAIL ID is present and unique within the batch.
        INSERT INTO tmp_dq SELECT 'DQ001', COUNT(*), COUNT_IF(reason_code IN ('NULL_KEY', 'DUPLICATE_KEY')), 'ERROR' FROM tmp_cls;
        -- DQ002 (error): durations are not negative.
        INSERT INTO tmp_dq SELECT 'DQ002', COUNT(*), COUNT_IF(reason_code = 'NEGATIVE_DURATION'), 'ERROR' FROM tmp_cls;
        -- DQ005 (error): every voicemail links to a call in stg_call (same rule as segments).
        INSERT INTO tmp_dq SELECT 'DQ005', COUNT(*), COUNT_IF(reason_code IN ('ORPHAN_CONTACT_ID', 'PARENT_CALL_REJECTED')), 'ERROR' FROM tmp_cls;

    -- ------------------------------------------------------------------------
    -- Branch: API_ACD_ALL -> stg_acd_interval
    -- ------------------------------------------------------------------------
    ELSEIF (p_source_table = 'API_ACD_ALL') THEN
        v_key_cond := 't.skill = s.skill AND t.campaign = s.campaign AND t.interval_start_et = s.interval_start_et';

        CREATE OR REPLACE TEMPORARY TABLE tmp_src AS
        SELECT r.*, OBJECT_CONSTRUCT(r.*) AS raw_payload
          FROM CC_DEMO.BRONZE.raw_api_acd_all r
         WHERE r._batch_id = :p_batch_id;
        SELECT COUNT(*) INTO :v_rows_in FROM tmp_src;

        -- Interval start: DATE + the start of TIME INTERVAL, America/New_York local.
        CREATE OR REPLACE TEMPORARY TABLE tmp_parsed AS
        SELECT s.*,
               TRY_TO_TIMESTAMP_NTZ(s."DATE" || ' ' || SPLIT_PART(s."TIME INTERVAL", '-', 1) || ':00') AS interval_start_et,
               TRY_TO_NUMBER(s."CALLS")                                       AS calls_offered,
               TRY_TO_NUMBER(s."ABANDONED")                                   AS calls_abandoned,
               TRY_TO_NUMBER(s."SERVICE LEVEL")                               AS sl_raw,
               TRY_TO_NUMBER(s."SOA MIN/THRESHOLD (SEC)")                     AS soa_threshold_sec,
               parse_duration_seconds(s."QUEUE WAIT TIME")                    AS queue_wait_seconds,
               parse_duration_seconds(s."TOTAL QUEUE TIME")                   AS total_queue_seconds,
               parse_duration_seconds(s."HANDLE TIME")                        AS handle_seconds,
               parse_duration_seconds(s."TALK TIME")                          AS talk_seconds,
               parse_duration_seconds(s."HOLD TIME")                          AS hold_seconds,
               parse_duration_seconds(s."AFTER CALL WORK TIME")               AS acw_seconds,
               parse_duration_seconds(s."RING TIME")                          AS ring_seconds,
               parse_duration_seconds(s."SPEED OF ANSWER")                    AS speed_seconds,
               parse_duration_seconds(s."TIME TO ABANDON")                    AS tta_seconds,
               TRY_TO_NUMBER(s."FIRST CALL RESOLUTION")                       AS fcr_flag,
               TRY_TO_NUMBER(s."QUEUE OVERFLOW")                              AS overflow_flag
          FROM tmp_src s;

        CREATE OR REPLACE TEMPORARY TABLE tmp_cls AS
        SELECT p.*,
               ROW_NUMBER() OVER (PARTITION BY p."SKILL", p."CAMPAIGN", p.interval_start_et
                                  ORDER BY p._load_ts DESC, p._record_hash) AS rn,
               CASE
                 WHEN p."SKILL" IS NULL OR p."CAMPAIGN" IS NULL OR p.interval_start_et IS NULL
                      THEN CASE WHEN p.interval_start_et IS NULL THEN 'INVALID_TIMESTAMP' ELSE 'NULL_KEY' END
                 WHEN LEAST(COALESCE(p.queue_wait_seconds, 0), COALESCE(p.handle_seconds, 0),
                            COALESCE(p.talk_seconds, 0), COALESCE(p.hold_seconds, 0),
                            COALESCE(p.acw_seconds, 0), COALESCE(p.ring_seconds, 0)) < 0 THEN 'NEGATIVE_DURATION'
               END AS reason_code_1
          FROM tmp_parsed p;

        CREATE OR REPLACE TEMPORARY TABLE tmp_call_base AS
        SELECT skill, campaign, TIME_SLICE(call_start_et, 30, 'MINUTE', 'START') AS interval_start_et,
               COUNT(*) AS calls_total,
               SUM(IFF(abandoned, 1, 0)) AS calls_abandoned_total
          FROM (
            SELECT b."SKILL" AS skill, b."CAMPAIGN" AS campaign, b."CONTACT ID" AS contact_id,
                   CONVERT_TIMEZONE('UTC', 'America/New_York', TRY_TO_TIMESTAMP_NTZ(b."TIMESTAMP")) AS call_start_et,
                   (TRY_TO_NUMBER(b."ABANDONED") = 1) AS abandoned
              FROM CC_DEMO.BRONZE.raw_api_calllog b
             WHERE DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', TRY_TO_TIMESTAMP_NTZ(b."TIMESTAMP")))
                   IN (SELECT DISTINCT TRY_TO_DATE("DATE") FROM tmp_src)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY b."CONTACT ID" ORDER BY b._load_ts DESC) = 1
          ) x
         GROUP BY skill, campaign, interval_start_et;

        -- Rows kept with a flag: service level out of range (DQ003) and DQ008 mismatch.
        CREATE OR REPLACE TEMPORARY TABLE tmp_flag AS
        SELECT a.*,
               cb.calls_total, cb.calls_abandoned_total,
               (a.sl_raw < 0 OR a.sl_raw > 100)                                        AS sl_out_of_range,
               (ABS(COALESCE(a.calls_offered, 0) - COALESCE(cb.calls_total, 0))
                  > 0.01 * COALESCE(cb.calls_total, 0)
                OR ABS(COALESCE(a.calls_abandoned, 0) - COALESCE(cb.calls_abandoned_total, 0))
                  > 0.01 * COALESCE(cb.calls_abandoned_total, 0))                      AS dq008_mismatch
          FROM tmp_cls a
          LEFT JOIN tmp_call_base cb
            ON cb.skill = a."SKILL" AND cb.campaign = a."CAMPAIGN" AND cb.interval_start_et = a.interval_start_et
         WHERE a.reason_code_1 IS NULL AND a.rn = 1;

        -- Reject rows: NULL_KEY, DUPLICATE_KEY, INVALID_TIMESTAMP, NEGATIVE_DURATION.
        INSERT INTO tmp_quar (record_key, reason_code, reason_detail, payload)
        SELECT COALESCE(p."SKILL", '') || '|' || COALESCE(p."CAMPAIGN", '') || '|' || COALESCE(TO_VARCHAR(p.interval_start_et), ''),
               CASE WHEN p.reason_code_1 IS NOT NULL THEN p.reason_code_1 ELSE 'DUPLICATE_KEY' END,
               'DATE=' || COALESCE(p."DATE", '') || '; TIME INTERVAL=' || COALESCE(p."TIME INTERVAL", ''),
               p.raw_payload
          FROM tmp_cls p
         WHERE p.reason_code_1 IS NOT NULL OR p.rn > 1;

        -- Rows kept with NULL service level: the value is out of range.
        INSERT INTO tmp_quar (record_key, reason_code, reason_detail, payload)
        SELECT f."SKILL" || '|' || f."CAMPAIGN" || '|' || TO_VARCHAR(f.interval_start_et),
               'SERVICE_LEVEL_OUT_OF_RANGE',
               'SERVICE LEVEL=' || COALESCE(f."SERVICE LEVEL", '') || '; row kept with NULL service_level_pct',
               f.raw_payload
          FROM tmp_flag f WHERE f.sl_out_of_range;

        CREATE OR REPLACE TEMPORARY TABLE tmp_ok AS
        SELECT
            f."SKILL"                                             AS skill,
            f."CAMPAIGN"                                          AS campaign,
            f.interval_start_et                                   AS interval_start_et,
            CONVERT_TIMEZONE('America/New_York', 'UTC', f.interval_start_et) AS interval_start_utc,
            DATE(f.interval_start_et)                             AS interval_date_et,
            f.calls_offered                                       AS calls_offered,
            f.calls_offered - f.calls_abandoned                   AS calls_answered,
            f.calls_abandoned                                     AS calls_abandoned,
            IFF(f.sl_out_of_range, NULL, f.sl_raw)                AS service_level_pct,
            f.soa_threshold_sec                                   AS soa_threshold_sec,
            f.queue_wait_seconds, f.total_queue_seconds, f.handle_seconds, f.talk_seconds,
            f.hold_seconds, f.acw_seconds AS after_call_work_seconds, f.ring_seconds,
            f.speed_seconds AS speed_of_answer_seconds, f.tta_seconds AS time_to_abandon_seconds,
            f.fcr_flag AS first_call_resolution_flag, f.overflow_flag AS queue_overflow_flag,
            f."MEDIA TYPE"                                        AS media_type,
            IFF(f.sl_out_of_range, 'FAIL', IFF(f.dq008_mismatch, 'WARN', 'PASS')) AS dq_status,
            f."FILE_NAME"                                         AS five9_file_name,
            TRY_TO_TIMESTAMP_NTZ(f."LOAD_TIMESTAMP")              AS five9_load_timestamp,
            f._batch_id, f._source_file, f._load_ts, f._record_hash
          FROM tmp_flag f;

        -- DQ001 (error): skill, campaign, and interval are present; no duplicate interval rows.
        INSERT INTO tmp_dq SELECT 'DQ001', COUNT(*), COUNT_IF(reason_code_1 = 'NULL_KEY' OR rn > 1), 'ERROR' FROM tmp_cls;
        -- DQ002 (error): durations are not negative.
        INSERT INTO tmp_dq SELECT 'DQ002', COUNT(*), COUNT_IF(reason_code_1 = 'NEGATIVE_DURATION'), 'ERROR' FROM tmp_cls;
        -- DQ003 (error): SERVICE LEVEL is between 0 and 100. The row is kept with a NULL
        --        value, so the interval still counts toward volume.
        INSERT INTO tmp_dq SELECT 'DQ003', COUNT(*), COUNT_IF(sl_out_of_range), 'ERROR' FROM tmp_flag;
        -- DQ008 (warning): CALLS and ABANDONED match the call-level counts within 1%.
        --        A mismatch is flagged in dq_status, not rejected.
        INSERT INTO tmp_dq SELECT 'DQ008', COUNT(*), COUNT_IF(dq008_mismatch), 'WARN' FROM tmp_flag;

    -- ------------------------------------------------------------------------
    -- Branch: WFM_DAILY -> stg_wfm_daily (ASSUMPTION: WFM is not a Five9 source)
    -- ------------------------------------------------------------------------
    ELSEIF (p_source_table = 'WFM_DAILY') THEN
        v_key_cond := 't.work_date = s.work_date AND t.agent_id = s.agent_id';

        CREATE OR REPLACE TEMPORARY TABLE tmp_src AS
        SELECT r.*, OBJECT_CONSTRUCT(r.*) AS raw_payload,
               ROW_NUMBER() OVER (PARTITION BY r."work_date", r."agent_id"
                                  ORDER BY r._load_ts DESC, r._record_hash) AS rn
          FROM CC_DEMO.BRONZE.raw_wfm_daily r
         WHERE r._batch_id = :p_batch_id;
        SELECT COUNT(*) INTO :v_rows_in FROM tmp_src;

        CREATE OR REPLACE TEMPORARY TABLE tmp_cls AS
        SELECT s.*,
               TRY_TO_DATE(s."work_date")                  AS work_date_v,
               TRY_TO_NUMBER(s."scheduled_minutes")        AS scheduled_v,
               TRY_TO_NUMBER(s."logged_in_minutes")        AS logged_v,
               TRY_TO_NUMBER(s."adherence_pct", 5, 1)      AS adherence_v,
               CASE
                 WHEN s."work_date" IS NULL OR s."agent_id" IS NULL OR TRIM(s."agent_id") = '' THEN 'NULL_KEY'
                 WHEN s.rn > 1                                                       THEN 'DUPLICATE_KEY'
                 WHEN TRY_TO_DATE(s."work_date") IS NULL                             THEN 'INVALID_VALUE'
                 WHEN LEAST(COALESCE(TRY_TO_NUMBER(s."scheduled_minutes"), 0),
                            COALESCE(TRY_TO_NUMBER(s."logged_in_minutes"), 0)) < 0   THEN 'NEGATIVE_DURATION'
                 WHEN TRY_TO_NUMBER(s."adherence_pct", 5, 1) NOT BETWEEN 0 AND 100
                      AND s."adherence_pct" IS NOT NULL                              THEN 'INVALID_VALUE'
               END AS reason_code
          FROM tmp_src s;

        INSERT INTO tmp_quar (record_key, reason_code, reason_detail, payload)
        SELECT c."work_date" || '|' || c."agent_id", c.reason_code,
               'scheduled_minutes=' || COALESCE(c."scheduled_minutes", '') || '; adherence_pct=' || COALESCE(c."adherence_pct", ''),
               c.raw_payload
          FROM tmp_cls c WHERE c.reason_code IS NOT NULL;

        CREATE OR REPLACE TEMPORARY TABLE tmp_ok AS
        SELECT c.work_date_v AS work_date, c."agent_id" AS agent_id,
               c.scheduled_v AS scheduled_minutes, c.logged_v AS logged_in_minutes,
               c.adherence_v AS adherence_pct,
               c._batch_id, c._source_file, c._load_ts, c._record_hash
          FROM tmp_cls c
         WHERE c.reason_code IS NULL;

        -- DQ001 (error): work_date and agent_id are present; one row per agent per day.
        INSERT INTO tmp_dq SELECT 'DQ001', COUNT(*), COUNT_IF(reason_code IN ('NULL_KEY', 'DUPLICATE_KEY')), 'ERROR' FROM tmp_cls;
        -- DQ002 (error): scheduled and logged minutes are not negative.
        INSERT INTO tmp_dq SELECT 'DQ002', COUNT(*), COUNT_IF(reason_code = 'NEGATIVE_DURATION'), 'ERROR' FROM tmp_cls;

    ELSE
        RAISE e_unknown_source;
    END IF;

    -- ------------------------------------------------------------------------
    -- Common: DQ007 (warning) new columns in this batch, then the writes.
    -- ------------------------------------------------------------------------
    INSERT INTO tmp_dq
    SELECT 'DQ007', COALESCE(:v_rows_in, 0), COUNT(*), 'WARN'
      FROM CC_DEMO.CONTROL.schema_drift_log
     WHERE batch_id = :p_batch_id AND source_table = :p_source_table
       AND detail IN ('NEW_VS_PRIOR_BATCH', 'ADDED_TO_PAYLOAD');

    -- Staging DDL above has run. The writes below run in one transaction.
    BEGIN TRANSACTION;

    DELETE FROM CC_DEMO.SILVER.stg_quarantine_record
     WHERE batch_id = :p_batch_id AND source_table = :v_raw;

    INSERT INTO CC_DEMO.SILVER.stg_quarantine_record
        (source_table, batch_id, record_key, reason_code, reason_detail, original_payload, created_ts)
    SELECT :v_raw, :p_batch_id, record_key, reason_code, reason_detail, payload, :v_started
      FROM tmp_quar;

    DELETE FROM CC_DEMO.CONTROL.dq_result
     WHERE batch_id = :p_batch_id AND layer = 'SILVER' AND table_name = :v_silver;

    INSERT INTO CC_DEMO.CONTROL.dq_result (rule_id, layer, table_name, batch_id, rows_checked, rows_failed, severity, run_ts)
    SELECT rule_id, 'SILVER', :v_silver, :p_batch_id, rows_checked, rows_failed, severity, :v_started
      FROM tmp_dq;

    -- Idempotent upsert on the business key. Re-running the batch changes nothing.
    v_sql := 'MERGE INTO CC_DEMO.SILVER.' || v_silver || ' t USING tmp_ok s ON ' || v_key_cond
          || ' WHEN MATCHED THEN UPDATE ALL BY NAME WHEN NOT MATCHED THEN INSERT ALL BY NAME';
    EXECUTE IMMEDIATE :v_sql;

    SELECT COUNT(*) INTO :v_ok FROM tmp_ok;
    SELECT COUNT(*) INTO :v_quar FROM tmp_quar;
    SELECT COUNT_IF(rows_failed > 0 AND severity = 'ERROR'), COUNT_IF(rows_failed > 0 AND severity = 'WARN')
      INTO :v_dq_err, :v_dq_warn
      FROM tmp_dq;

    COMMIT;

    RETURN OBJECT_CONSTRUCT(
        'batch_id', p_batch_id,
        'source_table', p_source_table,
        'status', IFF(v_quar > 0, 'SUCCEEDED_WITH_QUARANTINE', 'SUCCEEDED'),
        'rows_in', v_rows_in,
        'rows_loaded', v_ok,
        'rows_quarantined', v_quar,
        'dq_errors', v_dq_err,
        'dq_warnings', v_dq_warn,
        'message', 'silver load complete');

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        INSERT INTO CC_DEMO.CONTROL.load_log (batch_id, source_table, source_file, rows_read, rows_loaded,
                                              rows_skipped_duplicate_batch, status, error_message, started_ts, ended_ts)
        SELECT :p_batch_id, :p_source_table, 'silver:' || :v_silver, 0, 0, 0, 'FAILED', SQLERRM, :v_started,
               CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ;
        RAISE;
END;
$$;
