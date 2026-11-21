# M07 — Analyst review UI, decisions, nightly retrain with a promotion gate

**Timebox:** 12 h fast / 18 h full · **Trigger:** > 16 h → UI is queue + case + decision only; retrain via `make retrain`; add Airflow last. · **v2:** +12 h (section "Revision v2 additions")
**Role signal:** FDE (a real user workflow with an audit trail), ML engineering (label delay, feedback loop, gated promotion), full-stack (TypeScript).

## Inputs
Postgres cases/alerts/drafts/evidence; MLflow; `features_tx`; `raw_transactions` labels; `split.json`.

## Outputs
| Output | Where |
|---|---|
| API | `services/api/` (FastAPI) |
| UI | `web/` (Vite + React + TS) |
| Decisions + audit | Postgres `decisions`, `audit_log`, `study_events` |
| Training-set builder with label delay | `batch/spark/build_training_set.py` |
| Promotion gate | `ml/promote.py` |
| DAGs | `airflow/dags/train_nightly.py`, `data_quality_daily.py` |
| Loop evidence | `results/M07/loop_run.json` + screenshots |

## Steps

### 1. API (CC)
| Method | Path | Returns / does |
|---|---|---|
| GET | `/cases?status=&bank=&sort=max_score&limit=&cursor=` | queue rows |
| GET | `/cases/{id}` | case, alerts (scores, model_version, top_contribs, features), evidence (nodes, edges), latest draft + validator status, audit log |
| POST | `/cases/{id}/decision` | body `{decision: escalate\|dismiss, reason_code, edited_narrative?, reviewer, ui_mode, view_started_at}`. Computes `edit_distance_ratio` (rapidfuzz, normalized Levenshtein vs the original draft text), writes `decisions` + `audit_log` in one transaction, sets case `decided`. Second decision on the same case → 409. |
| POST | `/study/events` | `{case_id, reviewer, ui_mode, event: view_start\|decision}` with a server-side timestamp |
| GET | `/healthz`, `/metrics` | |

Reviewer identity is just a name typed once (no auth). Say so in the README limitations.

### 2. UI (CC; you decide the layout)
- **Queue page:** table (case_id, bank, subject, max score, #alerts, status, age), filters, sort; keyboard shortcuts j/k + Enter.
- **Case page, three panes:**
  - left: the draft narrative. Each `tx_id` is a chip; clicking it highlights that edge in the graph and the row in the table. Validator badge (pass / needs review). `evidence_limitations` shown.
  - center: Cytoscape.js graph. Subject node emphasized, edge width ∝ log amount, alerted edges marked, hop rings.
  - right: model panel (score, threshold, model_version, top-5 SHAP features with values); decision form (escalate/dismiss, reason codes, editable narrative, submit).
  - bottom: evidence table (sortable).
- **Study mode:** `?mode=baseline` shows only the alert metadata + evidence table (no graph, no draft, no SHAP). `?mode=assisted` shows everything. The UI posts `view_start` on load; the decision post carries `ui_mode`.
- **Audit tab:** chronological `audit_log`.

### 3. Feedback loop (CC builds, YOU own the protocol)
**Label-delay simulation (D21).** In real AML, ground truth arrives weeks later, from investigations. Simulate that: when training "as of replay-day D", ground-truth labels exist only for transactions with day ≤ D − L (`L = 2` days default). For days in (D − L, D], the only labels are analyst decisions on reviewed alerts (escalate → 1, dismiss → 0). `training_examples.label_source` records which.

`build_training_set.py --ds hi_small --as-of 2022-09-08 --label-delay-days 2` writes `training_examples` (build_id).

**Promotion gate** (`ml/promote.py`). The challenger is promoted to `champion` only if **all** hold:
1. PR-AUC on the **fixed gate set** (the M1 validation split) ≥ champion's − 0.002
2. F1 at the bank thresholds on the gate set ≥ champion's − 0.005
3. `spec_version` and `fx_version` equal the serving ones
4. inference time for 10k rows ≤ 1.5 × champion's
5. no NaN/inf predictions on the gate set

Otherwise keep the champion and log the reasons as an MLflow tag. Test it with a deliberately worse model (e.g. 5 trees).

**DAG `train_nightly(as_of)`:** data_quality_check → build_training_set → train (MLflow run, tagged with as_of and label stats) → evaluate on the gate set → promote_or_reject → notify (log). Trigger it manually: `airflow dags trigger train_nightly -c '{"as_of":"2022-09-08"}'`.

**DAG `data_quality_daily(day)`:** row count vs expected for the replayed day, DLQ rate < 0.1%, late legs = 0, null-rate per feature within bands learned from train, alert volume per bank within ±50% of budget. Failures fail the DAG.

**Two numbers to report** (keep them separate):
- (a) the **static** model's test F1 (paper-comparable, from M2)
- (b) **prequential** F1 during the replayed test period with nightly retrains (system behavior). Don't present (b) as comparable to published numbers.

### 4. The full loop demo (YOU)
Replay 3 days of the hi_small test period. Review about 30 cases yourself across both days. Trigger `train_nightly` for each replay-day boundary. Show `champion` changing (or correctly *not* changing) and the scorer's `model_version` switching in alerts. Save `loop_run.json` and screenshots.

## Tests
- API: decision idempotency (409 on second), audit row written in the same transaction, edit-distance computed.
- `build_training_set`: no ground-truth label newer than `as_of − L` (assert on max day per `label_source`).
- `promote.py`: a better model is promoted, a worse one rejected, a spec mismatch rejected.
- UI: `npm run build` and a typecheck in CI; one Playwright smoke test (load queue, open case, submit decision) is optional.

## Revision v2 additions (+12 h): three more human-in-the-loop loops

Loop 1 (decisions → retrain → gate) is above. v2 adds three loops, so humans also govern the **LLM**, the **misses** and the **operating point**, and every change has a second pair of eyes. All four loops write to `audit_log`.

| Loop | Humans provide | System does | Guard |
|---|---|---|---|
| 1 Decisions → model | escalate/dismiss | label-delayed training set → nightly retrain | automatic promotion gate |
| 2 Draft feedback → LLM | accept / edit / reject + reason codes | regression eval set grows; prompt changes are tested on it | prompt gate **and** supervisor sign-off |
| 3 QA sample → miss rate | reviews of below-threshold samples and needs-review drafts | estimates what the threshold misses | sampling rate recorded, so the estimate is unbiased |
| 4 Operating point → threshold | analyst proposes a threshold from the curve | applies it only after approval | maker-checker (no self-approval, enforced in the database) |

### Loop 2: draft feedback → eval set → prompt gate (CC, 4 h)
- UI: under each draft, **Accept / Edit / Reject** with reason codes (`wrong_pattern`, `ungrounded_claim`, `wrong_amount`, `missing_evidence`, `unclear`, `other`). `POST /cases/{id}/draft-feedback` → `draft_feedback`.
- `eval/build_eval_set.py`: every edited or rejected draft with a substantive reason code becomes a case in `eval/sets/regression_v<N>.jsonl` (evidence snapshot, ground-truth pattern, the analyst's decision, the reason). Sets `in_eval_set = true`.
- `eval/prompt_gate.py --candidate prompts/system_v2.md`: runs the candidate and the current prompt on the fixed 200-case set plus the regression set. The candidate passes only if **all** hold (start with these numbers; tune and record them in D25):
  1. pattern accuracy ≥ current − 1 point
  2. ungrounded-entity rate before the validator ≤ current
  3. needs-review rate ≤ current + 2 points
  4. injection pass rate ≥ current
  5. $ per 1,000 cases ≤ 1.2 × current
- If it passes, the gate inserts an `approvals` row (`kind = 'prompt_change'`, one per bank, gate report in `payload`, `proposed_by` = the engineer who ran it). Prompts are approved **per bank**: each customer signs off on changes that affect its cases. After a supervisor approves, the worker uses the new `prompt_version` for that bank.

### Loop 3: QA sample of what the threshold misses (CC, 3 h)
- The alert service samples scores in the band `[0.5 × threshold, threshold)` at rate `r` (default 2%) with a **deterministic hash of `tx_id`**, so a replay picks the same transactions (and `ON CONFLICT DO NOTHING` absorbs the repeat). Rows go to `qa_samples` with `source = 'below_threshold'` and the `sampling_rate`.
- The agent worker adds every `needs_review` case with `source = 'needs_review'`.
- A QA tab in the UI: reviewer sees the transaction, features and SHAP, never the label; answers suspicious / not suspicious.
- `eval/qa_estimate.py`: share suspicious in the sample (Wilson 95% interval) × band volume per day = estimated missed suspicious transactions per day. **Because the data is synthetic, you can check the estimate against the true count in the band.** Report both: that comparison is a rare, honest number.
- Reviewing hundreds of samples yourself isn't realistic. Review about 30 yourself, then add a `--simulated-reviewer` mode that answers from the ground-truth label with a configurable error rate (e.g. 10%). Label every number from simulation as simulated.

### Loop 4: operating point with maker-checker (CC, 4 h)
- `ml/operating_points.py`: per bank, from the champion's validation scores, a table of threshold → alerts per day, precision, recall. The below-threshold recall is adjusted with Loop 3's estimate when one exists.
- UI page **Thresholds**: the curve, the current point, and a "propose" button. Proposing inserts `approvals(kind = 'threshold_change', payload = {threshold, expected_alerts_per_day, expected_precision, expected_recall, model_version})`.
- A supervisor (a different person; the database refuses self-approval) approves or rejects with a note.
- `services/alerts/apply_approvals.py` (runs as `aml_service`) turns each approved change into a `bank_thresholds` row (`threshold_version = 'tv<approval_id>'`) and marks the approval `applied`. The alert service picks it up within 30 s (M05 v2).
- When the gate promotes a new champion, regenerate the operating points and show "your current threshold now gives N alerts/day on the new model". If that differs by more than 25% from before, flag it for review. **Never change a threshold automatically.**

**Tests (v2)**
- API: an analyst's approve call → 403; self-approval → 409; another bank's approval id → 404. The database test (`make test-rls`) backs these up.
- `test_qa_sampling_idempotent`: replaying the same day twice gives the same `qa_samples` rows.
- `test_apply_approvals_idempotent`: running the applier twice creates one `bank_thresholds` row.
- `test_prompt_gate_rejects_bad_prompt`: a candidate prompt with the "cite only evidence" rule removed fails the gate.

**Numbers (v2)** in `results/M07/hitl.json`: draft accept / edit / reject rates with the reason-code distribution · regression-set size · prompt-gate decisions with reasons · QA miss-rate estimate vs the true count (real and simulated reviews kept separate) · threshold changes proposed, approved, rejected, and median time to approval.

**Definition of Done (v2)**
- [ ] Each of loops 2–4 demonstrated once end to end, with screenshots and audit-log rows
- [ ] `hitl.json` written; D25 written

**Interview questions (v2)**
- Where are humans in the loop, and what stops one person from changing the system alone?
- How do you know what your threshold is missing? (Loop 3, with its interval, checked against ground truth.)
- How do you change a prompt safely? (Regression set built from analyst feedback, gate, per-bank sign-off.)
- Why doesn't the system change thresholds when the model changes?

## Claude Code prompts
> Implement services/api per M07 §1 and web/ per §2 with Vite + React + TS + TanStack Query + Cytoscape.js. Keep components small; no UI framework beyond a minimal CSS reset. Then wire study mode exactly as §2 describes. Show me screenshots via the dev server before polishing anything.

> Implement build_training_set.py with label delay (D21), promote.py with the 5 gate checks, and the two Airflow DAGs. Write test_promote.py first with a deliberately worse challenger.

## Definition of Done
- [ ] alert → review → decision → nightly retrain → gate → new version serving (or a logged rejection), all on screen
- [ ] Gate tests green; label-delay test green
- [ ] `loop_run.json` + screenshots
- [ ] v2: loops 2–4 (see "Revision v2 additions")

## Interview questions
- How do analyst decisions get into training without poisoning it? (Label source tracked; ground truth overrides once available; gate on a fixed set.)
- Why a promotion gate, and why those checks?
- What does "nightly" mean in a replay? (Event-time days, triggered at replay-day boundaries.)
