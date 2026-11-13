-- =============================================================================
-- 00_prereq.sql  (Snowflake)  -- reference data the analytics views need
-- =============================================================================
-- ref_team_skill maps the agent group on each call to the skill it answers.
-- ASSUMPTION: the workbook has no agent-to-skill mapping. These rows match the
-- generator (CLAIMANT -> CLAIMANT_PHONE, and so on). VEC must confirm them.
-- Q2 and Q4 use this mapping to attach WFM staffing to a skill.
-- =============================================================================
CREATE TABLE IF NOT EXISTS CC_DEMO.GOLD.ref_team_skill (
    agent_group VARCHAR(64) NOT NULL,
    skill_name VARCHAR(64) NOT NULL,
    list_status VARCHAR(128) NOT NULL,
    CONSTRAINT pk_ref_team_skill PRIMARY KEY (agent_group)
);

MERGE INTO CC_DEMO.GOLD.ref_team_skill t
USING (SELECT * FROM VALUES
    ('CLAIMANT', 'CLAIMANT_PHONE', 'ASSUMPTION - confirm with client'),
    ('EMPLOYER', 'EMPLOYER_PHONE', 'ASSUMPTION - confirm with client'),
    ('GENERAL',  'GENERAL_PHONE',  'ASSUMPTION - confirm with client')
    AS v(agent_group, skill_name, list_status)) s
ON t.agent_group = s.agent_group
WHEN NOT MATCHED THEN INSERT (agent_group, skill_name, list_status)
    VALUES (s.agent_group, s.skill_name, s.list_status);

-- Q3 needs the prior call's disposition on the repeat fact (see sql/procs/00_prereq.sql,
-- which adds the column and sql/procs/04_sp_load_gold.sql, which fills it).
