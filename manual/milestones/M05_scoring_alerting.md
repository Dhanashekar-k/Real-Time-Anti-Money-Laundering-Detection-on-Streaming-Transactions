# M05 — Scorer, alert service, end-to-end latency

**Timebox:** 10 h fast / 14 h full · **Trigger:** > 14 h → single scorer replica locally; tune in M9. · **v2:** +4 h (section "Revision v2 additions")
**Role signal:** ML serving (model registry, hot swap, versioned predictions, explanations), SWE (idempotency, transactional outbox, latency measurement).

## Inputs
`tx.features`, MLflow `aml-xgb@champion`, `config/thresholds.yaml`, `contracts/score.avsc`, `contracts/postgres_schema.sql`.

## Outputs
| Output | Where |
|---|---|
| Scorer service | `services/scorer/` → `tx.scores` |
| Alert service | `services/alerts/` → Postgres `alerts`, `cases`, `case_jobs`, `audit_log` |
| Latency matrix (T5) | `results/M05/latency_matrix.json` + `docs/latency.md` (generated) |
| Idempotency evidence | `results/M05/idempotency.json` |

## Steps

### 1. Scorer (CC)
- Consumer group `scorer`, `isolation.level=read_committed`, `enable.auto.commit=false`.
- Loop: `consume(num_messages=500, timeout=0.05)` → Avro decode (fastavro, schemaless reader with a cached schema) → check `spec_version == model.spec_version` (on mismatch: produce to `tx.dlq` with `error_code=SPEC_MISMATCH`, never guess) → build a float32 matrix (NaN for null) → `booster.inplace_predict(X)`.
- Candidates are rows with `score ≥ min(bank thresholds)`. For those only, `booster.predict(DMatrix(X_c), pred_contribs=True)` → top-5 contributions by absolute value.
- Produce a `Score` per row (`score.avsc`). Candidates also carry the feature values and top contributions; others carry empty arrays, which keeps the topic small.
- Commit offsets after delivery callbacks succeed (at-least-once).
- **Model loading:** `mlflow.xgboost.load_model("models:/aml-xgb@champion")` → booster. Read `spec_version` from the model's tags. Every 60 s, check whether the alias points to a new version; if so, load it and swap atomically between batches. Log `model_version` on every score; expose a gauge.
- Metrics: `rows_scored_total`, `batch_seconds` histogram, `candidates_total`, `model_version` gauge, `spec_mismatch_total`.

### 2. Alert service (CC)
- Consumer group `alerts`, batch consume.
- For each score with `score ≥ thresholds[bank_id]`, run **one Postgres transaction** (psycopg 3):
  ```sql
  INSERT INTO alerts (...) VALUES (...) ON CONFLICT (tx_id) DO NOTHING RETURNING tx_id;
  -- if nothing returned → duplicate: increment duplicates_suppressed_total; continue
  INSERT INTO cases (bank_id, subject_key, case_day, status, max_score, ...)
    VALUES (...) ON CONFLICT (bank_id, subject_key, case_day)
    DO UPDATE SET max_score = GREATEST(cases.max_score, EXCLUDED.max_score), updated_at = now()
    RETURNING case_id, (xmax = 0) AS created;
  UPDATE alerts SET case_id = :case_id WHERE tx_id = :tx_id;
  -- if created: INSERT INTO case_jobs (case_id, kind) VALUES (:case_id, 'draft');
  INSERT INTO audit_log (case_id, actor, action, payload) VALUES (:case_id, 'system:alerts', 'alert_attached', ...);
  ```
- Commit the DB transaction, **then** commit Kafka offsets. A crash between the two replays the batch, and the `ON CONFLICT` makes the replay a no-op. That's the effectively-once argument.
- `latency_ms = now_ms − replay_emit_ts_ms` is stored on the alert row (for exact percentiles) and observed in a Prometheus histogram.
- The case subject is the **sender** account; one case per (bank, subject, replay-day). `(xmax = 0)` is a Postgres idiom to tell an insert from an update in an upsert ([Likely]). If you'd rather not rely on it, use a CTE that returns whether the row was inserted.

### 3. Thresholds (from M2)
`config/thresholds.yaml` per bank: `mode: budget|f1`, the value, and the val metrics that justify it. Hot-reload on file change. Record the active threshold version on each alert. **v2:** the YAML only seeds the `bank_thresholds` table; after that, thresholds change only through approved changes (see "Revision v2 additions").

### 4. Latency matrix (CC runs, YOU interpret)
Run 1 replay day per cell and record p50/p95/p99 of `alerts.latency_ms`, plus the alert count:

| Flink mode | ckpt | speedup 1440 | speedup 10000 | asap |
|---|---|---|---|---|
| EOS | 1 s | | | |
| EOS | 5 s | | | |
| EOS | 10 s | | | |
| ALO | — | | | |

Pick the default mode for T5 (D20) from the data, and write down what dominates latency at each speed: watermark OOO, checkpoint interval, scorer batching or Postgres commits.

## Tests
- `test_scorer_spec_mismatch`: a wrong `spec_version` goes to the DLQ and is never scored.
- `test_scorer_hot_swap`: changing the alias between batches changes `model_version` on subsequent scores; no batch mixes versions.
- `test_alert_idempotency`: feed the same 1,000 scores twice → 1,000 alerts, `duplicates_suppressed = 1000`.
- `test_crash_between_commit_and_offset`: inject an exception after the DB commit and before the offset commit, restart → no lost and no duplicate alerts.
- `test_case_grouping`: 3 alerts, same sender and day → 1 case, 1 draft job.

## Revision v2 additions (+4 h): tenant isolation, roles, approved thresholds

**Why:** in a multi-bank deployment, "Bank A can't see Bank B's cases" must be enforced by the database, not just by a `WHERE` clause the API might forget. This is the first thing a bank's security reviewer asks about.

**Input:** `contracts/postgres_schema.sql` section v2 (row-level security policies, `aml_api` and `aml_service` roles, `approvals`, `bank_thresholds`, `draft_feedback`, `qa_samples`) and `contracts/rls_isolation_test.sql`.

**Steps**
1. **Database logins (CC).** `infra/compose/postgres/init/10_roles.sh` gives `aml_api` and `aml_service` LOGIN with passwords from `AML_DB_API_PASSWORD` and `AML_DB_SERVICE_PASSWORD` in `.env`. The alert service, agent worker, QA sampler and exporters connect as `aml_service`. The API (M07) connects as `aml_api`. No service uses the `postgres` superuser.
2. **Tenant context helper (CC).** `services/common/db.py`:
   ```python
   @contextmanager
   def tenant_tx(pool, bank_id: str, role: str, user_id: str):
       with pool.connection() as conn, conn.transaction():
           conn.execute("SELECT set_config('app.bank_id', %s, true), set_config('app.role', %s, true), set_config('app.user_id', %s, true)",
                        (bank_id, role, user_id))
           yield conn
   ```
   Bound parameters only. `SET app.bank_id = '...'` built from a string would be an injection hole. The third argument `true` scopes the settings to the transaction, so a pooled connection can't carry one bank's context into the next request.
3. **Thresholds from the database (CC).** Replace the YAML hot-reload of §3: the alert service reads the newest `bank_thresholds` row per bank every 30 s. `make seed-thresholds` copies `config/thresholds.yaml` in once, with `approval_id = NULL`. After that, a new row appears only by applying an **approved** change (M07 v2, Loop 4). `alerts.threshold_version` already records which one was used.

**Tests**
- `make test-rls`: creates a throwaway database, applies the schema, runs `contracts/rls_isolation_test.sql` (11 checks: fails closed with no tenant set, no cross-bank reads or writes, decisions only under your own name, analysts can't approve, no self-approval, the API role can't read the outbox). It was checked against Postgres 16, including two deliberately broken policies that it caught.
- `test_threshold_reload`: insert a new `bank_thresholds` row → within 30 s, new alerts carry the new `threshold_version`.
- `test_services_not_superuser`: each service's `current_user` is `aml_service`, never `postgres`.

**Numbers:** the isolation test result (11/11) goes in `results/M05/isolation.json`.

**Definition of Done (v2)**
- [ ] `make test-rls` passes in CI
- [ ] Alert service serves thresholds from `bank_thresholds`; the YAML is used only to seed
- [ ] D28 written

**Interview questions (v2)**
- Why row-level security instead of filtering in the API? (Defense in depth: one forgotten `WHERE` can't leak another bank's data. And it fails closed.)
- Your services bypass RLS. Isn't that a hole? (They're internal, have no user input, and use a separate login. The API, which handles user requests, never does.)
- How do you stop a pooled connection from leaking one tenant's context into the next request? (`set_config(..., true)` is scoped to the transaction.)

## Claude Code prompt
> Implement services/scorer and services/alerts per M05. Write test_alert_idempotency and test_crash_between_commit_and_offset first, using a real Postgres from compose (pytest marker integration). Use psycopg 3 with a connection pool; one DB transaction per Kafka batch is fine if the tests still pass. Then run the latency matrix per §4 and render docs/latency.md from the JSON.

## Definition of Done
- [ ] Alerts appear locally; replaying the same day twice creates 0 additional cases (`idempotency.json`)
- [ ] Latency matrix filled; T5 judged against it; D20 written
- [ ] Hot swap demonstrated (two model versions in one run, visible in `alerts.model_version`)
- [ ] v2: tenant isolation and approved thresholds (see "Revision v2 additions")

## Interview questions
- Your scorer is at-least-once. Why is that acceptable? Where exactly is the duplicate absorbed?
- Why store the feature vector and SHAP contributions on the alert instead of reading the account's current profile?
- How do you roll out a new model safely? How do you roll back? (Alias flip; versioned scores; promotion gate in M7.)
- What dominated your latency, and what did you change because of it?
