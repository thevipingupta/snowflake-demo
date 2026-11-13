# Demo run sheet

Order for presenting the four business questions, with the talking point, the planted
pattern each one reveals, and the layer to show alongside it.

**Data notes for the room**
- Five9 field names come from `reference/Contact_Center.xlsx`, the source of truth.
- The WFM extract is an **assumption**. Five9 has no workforce data. Anything that uses
  staffing (Q2 shortfall, Q4) is dependent on that assumption, and we say so on the slide.
- All data is synthetic. The planted patterns are the generator's design.

**Recommended order:** Q1, Q2, Q4, Q3. Q1 sets up the problem. Q2 splits surge from
staffing. Q4 tests whether adherence is the reason, and it isn't. Q3 closes on a null result,
which shows the analysis does not overclaim.

## 1. Where are service levels slipping? (Q1)

- **Talking point:** Service level fell hardest in two weeks, the week of 2026-04-13 and the
  week of 2026-05-04. Claimant phone dropped to about 38% in the first and about 67% in the
  second. Show by hour, not only by week.
- **Planted pattern:** the policy-deadline surge (week 7), and the phone staffing shortfall
  (week 10).
- **Show alongside:** `v_q1_service_level_slippage`, filtered to `slipping_flag`. Layer check:
  `fact_service_level_interval.rows_vs_silver` in `CONTROL.reconciliation` (PASS).
- **Caution:** the late-afternoon dip at 16:00 and 17:00 appears in every week. It is a
  shift-end effect, not an incident.

## 2. Why did abandonment spike? Surge or staffing? (Q2)

- **Talking point:** the same spike can come from two causes. Surge days show more calls
  with staffing held. Shortfall days show staffing down with normal volume. We can tell them
  apart. **This answer depends on the WFM assumption.**
- **Planted pattern:** the surge (2026-04-08 to 04-17), the week 10 shortfall (for example,
  2026-05-05 and 05-07), and the random weekend crew (100% abandonment on zero-staff weekend
  days).
- **Show alongside:** `v_q2_abandonment_driver`, filtered to spike days. Layer checks:
  `fact_agent_daily.rows_vs_silver` (PASS), and DQ007 warnings in `CONTROL.dq_result`,
  which report the new-column change.
- **Caution:** the weekend zero-staff days are a generator artifact. Explain that, or fix
  the generator first. Do not present them as a client finding.

## 3. Why does adherence matter for service level? (Q4)

- **Talking point:** we tested the obvious explanation, that agents not sticking to their
  schedule cause poor service. Schedule adherence does not correlate with service level
  (about zero). Staffed hours do (about 0.57). The problem is how many agents are on the
  schedule. **This answer depends on the WFM assumption.**
- **Planted pattern:** the week 10 headcount shortfall. Adherence does not show it, and
  logged minutes do.
- **Show alongside:** `v_q4_adherence_service_level`, with the correlation query. Layer
  check: `fact_agent_daily.rows_vs_silver` (PASS), and the WFM assumption noted on the slide.

## 4. Which dispositions lead to repeat calls? (Q3)

- **Talking point:** about 15% of calls have a repeat within 7 days, and that rate is
  steady across dispositions, at about 14-16%. No disposition is singled out as a driver.
  This is a valid result. The repeat definition is pending VEC confirmation.
- **Planted pattern:** the repeat contacts (about 15% of calls). The generator does not
  link them to disposition, so the flat result is correct.
- **Show alongside:** `v_q3_disposition_repeat`, sorted by rank. Layer check:
  `fact_repeat_call.rows_vs_fact_call_daily` in `CONTROL.reconciliation` (PASS).
- **Caution:** calls in the last 7 days of data have incomplete windows, so their rates are
  understated.

## Layer check to show at the end

- `CONTROL.reconciliation`: all `file_rows_vs_bronze` and `bronze_vs_silver_accounted` rows
  are PASS, and the gold count checks are PASS.
- `CONTROL.dq_result`: the planted issues are caught by DQ001, DQ002, DQ003, DQ005, and DQ006
  (see `sql/tests/test_pipeline.sql`). The late ACD file has no rule and is shown as a gap.
- `CONTROL.test_results`: the pipeline tests (clean batch, planted batch, schema evolution,
  re-run).

## Before the demo

- Run `sql/analytics/` and confirm each view returns rows.
- Run `src/analytics/build_charts.py` with the Snowflake environment variables set. Charts
  go to `output/charts/`.
- Confirm the four assumptions are stated on the slides: WFM, team-to-skill, repeat
  definition, and disposition path layout.
