# Business questions

Four questions the analytics layer answers, each on a gold-layer view. Each section gives
the question, the metric definitions it uses (see `metric_definitions.md`), how to read the
result, and any dependency on the WFM assumption.

Sources: Five9 field names come from `reference/Contact_Center.xlsx`. The WFM extract is an
**assumption** (Five9 has no workforce data). Team-to-skill mapping is an **assumption**.

---

## Q1. Where are service levels slipping, and by which skill and hour of day?

- **View:** `CC_DEMO.GOLD.v_q1_service_level_slippage`
- **Grain:** week, skill, hour of day (America/New_York).
- **Metrics:** service level (source SERVICE LEVEL, calls-weighted, with the skill's SOA
  threshold), abandonment rate, baseline service level (prior four weeks, same skill and
  hour), `sl_change_pts`, `slipping_flag`.
- **How to read it:** filter `slipping_flag` and sort by `sl_change_pts`. The most negative
  rows are where service level has fallen furthest below its own recent history. Compare
  skills at the same hour, using each row's `soa_threshold_sec`. A skill with a 20-second
  threshold and a 60-second threshold are not directly comparable on the same bar.
- **WFM dependency:** none. This question uses Five9 data only.
- **Sample query:** see the comment above the query in `v_q1_service_level_slippage.sql`.

## Q2. Why did abandonment spike on a given day? Surge or staffing?

- **View:** `CC_DEMO.GOLD.v_q2_abandonment_driver`
- **Grain:** calendar date and skill.
- **Metrics:** abandonment rate and its same-weekday baseline, volume ratio, staffing ratio
  (logged minutes against baseline), and `driver`.
- **Drivers:**
  - `VOLUME_SURGE`: volume is at least 1.5 times baseline, and staffing is above 85% of baseline.
  - `STAFFING_SHORTFALL`: staffing is at or below 85% of baseline, and volume is below 1.5 times baseline.
  - `SURGE_AND_SHORTFALL`: both conditions.
  - `UNEXPLAINED`: a spike with neither condition.
  - `NO_SPIKE` and `NO_BASELINE` otherwise.
- **How to read it:** start with spike days (`driver` not `NO_SPIKE` or `NO_BASELINE`).
  A `VOLUME_SURGE` means more calls came in than usual while staffing held. A
  `STAFFING_SHORTFALL` means staffing fell, with normal or lower volume. A day with zero
  logged minutes shows 100% abandonment, which means no agents were rostered for that skill.
- **WFM dependency: yes.** The shortfall side uses `logged_in_minutes` from the WFM
  extract, and the team-to-skill mapping is an assumption. Label any shortfall finding as
  dependent on the WFM assumption.

## Q3. Which dispositions lead to repeat calls from the same customer_key within 7 days?

- **View:** `CC_DEMO.GOLD.v_q3_disposition_repeat`
- **Grain:** disposition path of the **earlier** call.
- **Metrics:** calls with that disposition, calls followed by a repeat within 7 days, repeat
  rate, average days to repeat, and a rank by repeat rate.
- **Repeat definition:** a call from the same customer_key (SALESFORCE_ID, or ANI when
  SALESFORCE_ID is empty) within 7 calendar days of an earlier call. **VEC must confirm
  this definition.**
- **How to read it:** rank by `repeat_rate_pct`. Treat differences of about one point as
  noise unless the volume behind them is large. A flat ranking is a valid result: it says
  no single disposition drives repeats.
- **Limitation:** calls in the last 7 days of loaded data have incomplete windows, so
  their rates are understated.
- **WFM dependency:** none.

## Q4. How does agent adherence relate to service level on the same day and skill?

- **View:** `CC_DEMO.GOLD.v_q4_adherence_service_level`
- **Grain:** calendar date and skill.
- **Metrics:** adherence ratio (logged minutes over scheduled minutes), source
  `adherence_pct` as delivered, logged minutes, scheduled minutes, agents scheduled,
  service level, and calls offered.
- **How to read it:** correlate on the sample query. Adherence ratio and logged minutes
  answer different questions. A near-zero adherence correlation says schedule adherence is
  not what moves service level in this data. A positive logged-minutes correlation says
  staffed hours are.
- **WFM dependency: yes.** Adherence and logged minutes come from the WFM extract, which is
  an assumption. Label any adherence finding as dependent on the WFM assumption.
