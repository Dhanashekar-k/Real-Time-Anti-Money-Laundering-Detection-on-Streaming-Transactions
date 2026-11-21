# M10 — FDE validation: onboard Bank C by config, run the triage study

**Timebox:** 6 h fast / 10 h full · **Trigger:** reviewers unavailable → 3 reviewers, or report T10 only and label T9 "not run".
**Role signal:** FDE. You can integrate a new customer's messy data fast, and you can show the tool saves an analyst time.

## Outputs
| Output | Where |
|---|---|
| Bank C format + mapping | `stream/replayer` Bank C renderer, `connectors/banks/bank_c.yaml` |
| Onboarding runbook with timings (T10) | `docs/onboarding.md`, `results/M10/onboarding.json`, screen recording (local file, not committed) |
| Optional LLM mapping assistant | `tools/propose_mapping.py`, `results/M10/mapping_assistant.json` |
| Triage study protocol + results (T9, T8b) | `docs/triage_study.md`, `results/M10/triage_raw.csv` (anonymized), `results/M10/triage_summary.json` |

## Part 1 — Bank C onboarding (T10)

### 1. [CC] Define Bank C (deliberately awkward, like real customer files)
- CSV with **semicolons** and a header row
- decimal **comma**, thousands **dot**: `1.234,56`
- timestamps ISO-8601 **with an offset**: `2022-09-01T02:20:00+02:00` (must normalize to UTC)
- account numbers as `<branch>-<account>` (e.g. `0042-8000EBD30`), where the branch must be dropped to match the canonical account id
- payment methods in Spanish: `TRANSFERENCIA`, `CHEQUE`, `TARJETA`, `EFECTIVO`, `CRIPTO`, `ACH`, `REINVERSION`
- currencies as ISO codes, but the **receiving** currency column is missing when it equals the paying currency (empty means "same")
- an extra free-text column `concepto` (memo) that must be **ignored** (and must never reach the LLM; see M6's injection note)

Bank C owns a third of the senders: change the owner rule to `hash % 3` for the M10 replays only, recorded in decisions.

### 2. [YOU] Onboard it, timed, config only
Start a timer and a screen recording:
1. Read a 1,000-row sample.
2. Write `bank_c.yaml`. Any engine operation missing from M3's list (e.g. "default to another field when empty", "strip prefix before '-'") is **a code change**, so stop the clock. Either add the operation and restart, or note honestly that onboarding needed one generic engine feature.
3. `make onboard-check BANK=c SAMPLE=…` until coverage is 100% and the DLQ rate is 0 on the sample.
4. Add Bank C to `config/banks.yaml` (topic, thresholds copied from the budget rule), create its topic, start its connector service.
5. Replay one day with Bank C on. Its transactions appear in `tx.canonical`, `tx.features` and alerts.
Stop the clock. Record per-step minutes and the total in `onboarding.json`. Target ≤ 60 minutes with zero code changes.

**Honesty rule:** M3's engine already has a generic operation set (`default_from`, `strip_prefix`/regex extract, `decimal_locale`, timezone offsets) designed *before* Bank C existed. Don't add Bank C-specific operations ahead of the timed run, because that would hide real onboarding cost. If Bank C needs something the engine lacks, that's a code change: count it and report it. Generic transforms belong in the engine. Bank-specific facts belong in YAML. That separation is the FDE design point.

### 3. Optional: LLM mapping assistant (2–4 h, strong FDE + LLM signal)
`tools/propose_mapping.py --sample bank_c_sample.csv`. It sends Claude Sonnet 5.5 the canonical schema, `bank_a.yaml` and `bank_b.yaml` as examples, the engine's operation list and 50 sample rows (with `concepto` **removed** first). The model proposes `bank_c.yaml`, which is immediately dry-run through `onboard-check`. The script loops up to 3 times, feeding back the dry-run errors. A human reviews the final YAML diff.
Measure: minutes to a passing mapping with the assistant vs. your manual run, and how many fields the assistant mapped correctly the first time. Hold the proposal to the same validation as hand-written config.

## Part 2 — Triage study (T9 and T8b)

### Protocol (write it in `docs/triage_study.md` before running)
- **Participants:** 3–5 people (classmates or friends), 30 min each. Verbal consent. No personal data recorded; reviewers are R1…R5.
- **Case pool:** 40 cases from the hi_small test replay with drafts that passed the validator. Balanced: 20 where the alerted transaction is truly laundering (mixed pattern types), 20 false positives. Ground truth hidden.
- **Design:** each reviewer does 20 cases, 10 in **baseline** (`?mode=baseline`: alert info + evidence table) and 10 in **assisted** (`?mode=assisted`: graph + draft + SHAP). Counterbalance: odd reviewers do baseline first, even reviewers assisted first. Cases are assigned so each case is seen in both modes across reviewers.
- **Training:** 5 minutes on the typologies and both screens, using 2 practice cases not in the pool.
- **Measures:**
  - time per case (server-side, `view_start` → `decision`)
  - decision accuracy vs ground truth
  - in assisted mode, whether they edited the draft (`edit_distance_ratio`; "minor" ≤ 0.10)
  - after the session, 3 Likert questions: "I trusted the summary", "The graph helped", "I'd use this"
- **Analysis:** median time per mode per reviewer and pooled, accuracy per mode, and the assisted-mode draft acceptance rate (no or minor edits). **Descriptive only; n is stated, no p-values.**

### Run it
Use your own laptop and the local stack, with one reviewer at a time. Export `study_events` + `decisions` → `triage_raw.csv` → `triage_summary.json` (scripted).

## Definition of Done
- [ ] T10: Bank C onboarded, timed, with the code-change count stated (0 is the target)
- [ ] T9 / T8b: protocol written before running; results with n; raw data anonymized
- [ ] `docs/onboarding.md` usable by someone else (ask a friend to read it)

## Interview questions (FDE loops love these)
- Tell me about integrating a customer whose data didn't fit your schema.
- How did you decide what goes in config vs code?
- How do you know your tool helps the analyst, not just looks nice?
- What would you do differently with a real bank's compliance team? (Data residency, PII, model governance and documentation, audit requirements, change management.)
