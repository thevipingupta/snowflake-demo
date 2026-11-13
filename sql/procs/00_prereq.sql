-- =============================================================================
-- 00_prereq.sql  (Snowflake)  -- objects the procedures need, beyond 01-04 DDL
-- =============================================================================
-- Run once, after sql/ddl/01-04. Idempotent (IF NOT EXISTS / ADD COLUMN IF NOT EXISTS).
--
-- Adds:
--   * dq_status column on silver tables that the procedures flag (stg_call,
--     stg_call_segment, stg_voicemail). stg_acd_interval already has it.
--   * CONTROL.source_config: one row per Five9 source, used to route the
--     generic procedures to the right raw, silver, and file settings.
--   * CONTROL.batch_header: header names per batch, used to detect new columns
--     by comparing a batch with the previous batch (schema drift).
--   * CONTROL.ref_case_disposition: the case-related disposition list for DQ004.
--     VEC must confirm it. The rows below are PLACEHOLDERS.
--   * File formats and the internal stage the bronze procedure reads.
--
-- Session secret: sp_load_silver reads the customer key secret from a session
-- variable named CUSTOMER_KEY_SECRET. Set it per session from the environment,
-- for example  SET CUSTOMER_KEY_SECRET = '<value from secret store>';
-- Do not put the value in any file in this repository.
-- =============================================================================

ALTER TABLE CC_DEMO.SILVER.stg_call ADD COLUMN IF NOT EXISTS dq_status VARCHAR(8) DEFAULT 'PASS';
ALTER TABLE CC_DEMO.SILVER.stg_call_segment ADD COLUMN IF NOT EXISTS dq_status VARCHAR(8) DEFAULT 'PASS';
ALTER TABLE CC_DEMO.SILVER.stg_voicemail ADD COLUMN IF NOT EXISTS dq_status VARCHAR(8) DEFAULT 'PASS';

CREATE TABLE IF NOT EXISTS CC_DEMO.CONTROL.source_config (
    source_table VARCHAR(64) NOT NULL,      -- API_CALLLOG, ...; also WFM_DAILY
    raw_table VARCHAR(128) NOT NULL,        -- raw_api_calllog, ...
    silver_table VARCHAR(128) NOT NULL,     -- stg_call, ...
    folder VARCHAR(64) NOT NULL,            -- five9/ or wfm/
    file_prefix VARCHAR(64) NOT NULL,       -- CALLLOG, ACD_ALL, wfm_daily, ...
    header_row NUMBER(2,0) NOT NULL,        -- 1 for Five9 files, 3 for WFM (two '#' lines first)
    file_format VARCHAR(64) NOT NULL,       -- ff_csv_five9 | ff_csv_wfm
    load_order NUMBER(2,0) NOT NULL,        -- pipeline order
    CONSTRAINT pk_source_config PRIMARY KEY (source_table)
);

MERGE INTO CC_DEMO.CONTROL.source_config t
USING (
    SELECT * FROM VALUES
      ('WFM_DAILY',                  'raw_wfm_daily',                 'stg_wfm_daily',       'wfm/',   'wfm_daily',         3, 'ff_csv_wfm',   1),
      ('API_ACD_ALL',                'raw_api_acd_all',               'stg_acd_interval',    'five9/', 'ACD_ALL',           1, 'ff_csv_five9', 2),
      ('API_CALLLOG',                'raw_api_calllog',               'stg_call',            'five9/', 'CALLLOG',           1, 'ff_csv_five9', 3),
      ('API_CALL_SEGMENTREPORTALL',  'raw_api_call_segment',          'stg_call_segment',    'five9/', 'CALL_SEGMENT',      1, 'ff_csv_five9', 4),
      ('API_VOICEMAIL_LIFECYCLE_LOG','raw_api_voicemail_lifecycle',   'stg_voicemail',       'five9/', 'VOICEMAIL_LIFECYCLE',1,'ff_csv_five9', 5)
      AS v(source_table, raw_table, silver_table, folder, file_prefix, header_row, file_format, load_order)
) s
ON t.source_table = s.source_table
WHEN MATCHED THEN UPDATE SET raw_table = s.raw_table, silver_table = s.silver_table, folder = s.folder,
    file_prefix = s.file_prefix, header_row = s.header_row, file_format = s.file_format, load_order = s.load_order
WHEN NOT MATCHED THEN INSERT (source_table, raw_table, silver_table, folder, file_prefix, header_row, file_format, load_order)
    VALUES (s.source_table, s.raw_table, s.silver_table, s.folder, s.file_prefix, s.header_row, s.file_format, s.load_order);

CREATE TABLE IF NOT EXISTS CC_DEMO.CONTROL.batch_header (
    batch_id VARCHAR(16) NOT NULL,
    source_table VARCHAR(64) NOT NULL,
    header_names VARIANT,                   -- ARRAY of bronze column names, in file order
    loaded_ts TIMESTAMP_NTZ,
    CONSTRAINT pk_batch_header PRIMARY KEY (batch_id, source_table)
);

-- DQ004 reference list. PLACEHOLDER rows: VEC must confirm the real list.
CREATE TABLE IF NOT EXISTS CC_DEMO.CONTROL.ref_case_disposition (
    disposition VARCHAR(128) NOT NULL,
    requires_followup BOOLEAN NOT NULL,
    list_status VARCHAR(64) NOT NULL,       -- PLACEHOLDER until VEC confirms
    CONSTRAINT pk_ref_case_disposition PRIMARY KEY (disposition)
);

MERGE INTO CC_DEMO.CONTROL.ref_case_disposition t
USING (SELECT * FROM VALUES
    ('Appeal logged',      TRUE, 'PLACEHOLDER - VEC to confirm'),
    ('Callback scheduled', TRUE, 'PLACEHOLDER - VEC to confirm')
    AS v(disposition, requires_followup, list_status)) s
ON t.disposition = s.disposition
WHEN NOT MATCHED THEN INSERT (disposition, requires_followup, list_status)
    VALUES (s.disposition, s.requires_followup, s.list_status);

-- File formats. ff_line reads whole lines, used only to read header lines.
CREATE FILE FORMAT IF NOT EXISTS CC_DEMO.BRONZE.ff_line
    TYPE = CSV FIELD_DELIMITER = NONE RECORD_DELIMITER = '\n' SKIP_HEADER = 0;

CREATE FILE FORMAT IF NOT EXISTS CC_DEMO.BRONZE.ff_csv_five9
    TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    EMPTY_FIELD_AS_NULL = FALSE NULL_IF = ('') TRIM_SPACE = FALSE;

CREATE FILE FORMAT IF NOT EXISTS CC_DEMO.BRONZE.ff_csv_wfm
    TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 3 FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    EMPTY_FIELD_AS_NULL = FALSE NULL_IF = ('') TRIM_SPACE = FALSE;

-- Internal stage. Upload the generated files with PUT, keeping the folders:
--   PUT file://data/source/five9/*  @CC_DEMO.BRONZE.src_stage/five9/
--   PUT file://data/source/wfm/*    @CC_DEMO.BRONZE.src_stage/wfm/
CREATE STAGE IF NOT EXISTS CC_DEMO.BRONZE.src_stage;

-- Stage for the planted-issue log, used by the tests only.
CREATE STAGE IF NOT EXISTS CC_DEMO.CONTROL.test_stage;
CREATE FILE FORMAT IF NOT EXISTS CC_DEMO.CONTROL.ff_csv_planted
    TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- Added for the analytics layer (Q3): disposition of the earlier call in each repeat pair.
ALTER TABLE CC_DEMO.GOLD.fact_repeat_call ADD COLUMN IF NOT EXISTS prior_disposition_key NUMBER(38,0);
