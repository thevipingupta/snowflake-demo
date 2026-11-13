# Metric definitions

Single source for every metric used in the analytics views (`sql/analytics/`), the
business-question write-ups (`business_questions.md`), and the charts
(`src/analytics/build_charts.py`). Field names are Five9 names from
`reference/Contact_Center.xlsx`, the source of truth. WFM fields come from the WFM
extract, which is an **assumption**: Five9 has no workforce data.

## Service level

- **Definition:** the `SERVICE LEVEL` value from the Five9 source, as delivered.
- **Threshold:** each skill's `SOA MIN/THRESHOLD (SEC)` is part of this definition. The
  service level is the percent of calls answered within that skill's threshold, as Five9
  calculates it. The threshold differs by skill and is carried on each row
  (`soa_threshold_sec`). It is not restated as a fixed number anywhere in this project.
- **Not recalculated.** The silver and gold layers do not recompute service level.
- **Aggregation:** when intervals are combined (by week, hour, or day), service level is
  the calls-weighted average of the interval values (sum of `service_level_pct x
  calls_offered` divided by sum of `calls_offered` over intervals with a service level).
- **Values outside 0-100** are kept with a NULL service level and quarantined (DQ003).

## Abandonment rate

- **Definition:** `calls_abandoned / calls_offered`, shown as a percent.
- **Source:** the interval counts in `fact_service_level_interval`, or the call-level
  `ABANDONED` flag in `stg_call`.
- **Note:** abandonment rises when queue wait passes the skill's SOA threshold.
  This is a behavior of the calls, not a definition. See `expected_findings.md`.

## Adherence

- **Definition:** `logged_in_minutes / scheduled_minutes`, from the WFM extract.
- **Dependency:** WFM assumption. Adherence uses data that Five9 does not supply.
- **Days with zero scheduled minutes** (off days) are excluded.
- **Also stored, not used as the definition:** the WFM `adherence_pct` column as
  delivered (`source_adherence_pct` in `v_q4_adherence_service_level`). The two measures
  answer different questions. Logged minutes measure headcount. The ratio measures
  schedule adherence.

## Staffing (Q2)

- **Logged minutes:** `SUM(logged_in_minutes)` across the agents mapped to a skill on a
  day. The mapping is `dim_agent.agent_group` to `ref_team_skill.skill_name`, which is an
  **assumption** (`sql/analytics/00_prereq.sql`).
- **Staffing ratio:** logged minutes divided by the same-weekday average over the
  previous four weeks.
- **Dependency:** WFM assumption, plus the team-to-skill assumption.

## Repeat call (VEC must confirm)

- **Definition:** a call from the same `customer_key` within 7 calendar days of an
  earlier call.
- **customer_key:** `SALESFORCE_ID`, or `ANI` when `SALESFORCE_ID` is empty. In silver it
  is a salted hash of that value (see `sql/procs/02_sp_load_silver.sql`), not the raw
  value, so the key matches across calls after masking.
- **Window:** 0 to 7 calendar days (`DATEDIFF('day', earlier, later) BETWEEN 0 AND 7`),
  with the earlier call strictly before the later one.
- **Pairing:** each repeat call links to its most recent earlier call with the same key
  in the window (`fact_repeat_call`).
- **Status:** VEC must confirm this definition, including whether a repeat must be a
  different contact, and whether same-day repeats count.

## Repeat rate (Q3)

- **Definition:** calls that have a repeat within 7 days, divided by all calls with the
  same disposition. The disposition is that of the **earlier** call.
- **Limitation:** calls in the last 7 days of loaded data have not had their full window,
  so their rates are understated.

## Disposition hierarchy

- **Fields:** `DISPOSITION`, `DISPOSITION GROUP A`, `DISPOSITION GROUP B`,
  `DISPOSITION GROUP C`, and `DISPOSITION PATH`, as defined in the workbook.
- **Path:** `DISPOSITION PATH` is taken as delivered. In the synthetic data it reads
  `group A > group B > group C > disposition`. Confirm this layout against the workbook
  definition before using path values for reporting.
- **Gold:** `dim_disposition` holds one row per distinct path (`sql/ddl/03_gold.sql`).

## Volume and spike rules (Q2)

- **Volume ratio:** calls offered divided by the same-weekday average over the previous
  four weeks.
- **Abandonment spike:** the day's abandonment rate is at least 5 points above its
  same-weekday baseline.
- **Surge:** volume ratio of at least 1.5. **Shortfall:** staffing ratio at or below 0.85.

## Slipping service level (Q1)

- **Baseline:** the average service level of the same skill and hour over the previous
  four weeks.
- **Slipping:** service level is at least 10 points below baseline, on at least 10 calls.
- The 10-point and 10-call values are analytic choices, not client thresholds. Change them
  in `v_q1_service_level_slippage.sql` if VEC sets different values.
