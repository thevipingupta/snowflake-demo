# Expected findings (synthetic data)

**This document describes expected behavior from the synthetic generator. It is not a
finding about real VEC data.** The planted patterns are the generator's own design. The
measured values below come from running the same logic over the synthetic extracts in
`data/source/` (the generated Five9 files and the WFM extract). They are rounded, and the
gold views compute slightly different windows, so expect small differences.

## Planted patterns

| Pattern | Where | Set by |
|---|---|---|
| Policy-deadline surge | 2026-04-08 to 2026-04-17 (week 6-7), all skills; deadline 2026-04-17 | `POLICY_DEADLINE`, `SURGE_DAYS` (2.0x ramping to 3.0x) |
| Phone staffing shortfall | Week 10 (2026-05-04 to 2026-05-10), claimant phone | `SHORTFALL_WEEK`; 40% of claimant weekday shifts unfilled |
| Abandonment near threshold | All skills; callers give up near each skill's SOA threshold | `sample_patience`: 15% of callers give up at 0.8-1.2 x threshold |
| Repeat contacts | About 15% of calls share a customer_key with a call in the prior 7 days | `REPEAT_PROBABILITY` |
| Monday peak and lunch dip | Every weekday | `DOW_FACTOR`, `INTERVAL_WEIGHTS` |

Skill thresholds: CLAIMANT_PHONE 20 s, EMPLOYER_PHONE 30 s, GENERAL_PHONE 60 s.

## Expected results by question

### Q1. Service-level slippage
- **Week 7 surge:** claimant phone service level falls to about **38%** in the week of
  2026-04-13 (baseline about 85-90%), and abandonment rises to about **25%**. Employer phone
  falls to about **46%**. General phone is much less affected (about 95%).
- **Week 10 shortfall:** claimant phone service level falls to about **67%** and
  abandonment to about **16%**. The drop is largest from 09:00 to 10:00 (about 62% against a
  93% baseline at 09:00) and at 15:00 (about 71% against 91%).
- **Week 6:** claimant and employer service level already dip (about 72% and 67%) as the
  surge begins. This is the lead-in to the week 7 surge.
- **Late afternoon:** service level is lowest at 16:00 and 17:00 in every week, including
  baseline weeks (about 56% and 50% in the baseline average). This is a shift-end effect:
  some full-time shifts finish at 16:00. It is not a planted pattern, but it will appear in
  Q1, so present it honestly.

### Q2. Abandonment drivers
- **Surge:** claimant phone calls on 2026-04-08 to 2026-04-17 run about **2x to 3x** the
  same-weekday baseline. Those days classify as `VOLUME_SURGE`, with 2026-04-15 as
  `SURGE_AND_SHORTFALL`.
- **Shortfall:** claimant phone days 2026-05-05 and 2026-05-07 classify as
  `STAFFING_SHORTFALL` (logged minutes at about 50-65% of baseline, call volume normal).
- **Weekend crew artifact:** some weekend days have no rostered agents for a skill. They show
  100% abandonment with zero logged minutes (claimant 2026-04-25 and 2026-04-26; employer
  2026-04-11 and 2026-04-12). These are `STAFFING_SHORTFALL` days. The cause is the random
  weekend crew in the generator, not a planted pattern. Explain this, or fix the generator
  before the demo.
- **Spike count:** across all skills, about **19** days classify as shortfall, **13** as
  surge, **5** as both, and **4** as unexplained. Counts depend on the baseline window.

### Q3. Dispositions and repeats
- Overall, about **15%** of calls have a repeat within 7 days.
- By disposition, repeat rates sit between about **14% and 16%**. The highest is about
  16% (Escalated > Appeals > Logged > Appeal logged). Abandoned calls are about 16% as well.
- **Expected result: no disposition stands out.** The generator does not link repeats to
  disposition, so a flat ranking is the correct answer. Do not present the top disposition
  as a driver.

### Q4. Adherence against service level
- Correlation of **adherence ratio** (logged over scheduled) with service level is about
  **0** overall (about -0.00), and between about -0.13 and +0.09 within each skill.
- Correlation of **logged minutes** with service level is about **0.57**. Staffed hours
  track service level.
- The WFM `adherence_pct` column as delivered correlates at about 0.14. It does not show the
  week 10 shortfall either.
- **Interpretation:** the shortfall is a headcount effect. Schedule adherence in this data
  does not explain service level. Present this as an observation about the synthetic data.

## Layer checks that should pass
- Reconciliation (`CONTROL.reconciliation`): every `file_rows_vs_bronze` and
  `bronze_vs_silver_accounted` row is PASS, and the gold count checks are PASS.
- DQ results (`CONTROL.dq_result`): DQ005 and DQ006 fail only on the planted segment and
  agent issues, and DQ008 warnings are limited to intervals with rejected calls.
