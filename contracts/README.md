# contracts/

| File | Used by | Rules |
|---|---|---|
| `canonical_tx.avsc` | connectors → `tx.canonical` → Flink | Schema registry subject `tx.canonical-value`, compatibility BACKWARD. Never add label fields. |
| `feature_vector.avsc` | Flink → `tx.features` → scorer | `values` ordered by `features/feature_names_v<spec_version>.txt`. |
| `score.avsc` | scorer → `tx.scores` → alerts, reconcile | Candidates carry `values` + `top_contribs`; others empty arrays. |
| `postgres_schema.sql` | alerts, agent worker, API | Idempotency keys: `alerts.tx_id`, `cases(bank_id, subject_key, case_day)`, `case_jobs(case_id, kind)`, `decisions.case_id`, `qa_samples(tx_id, source)`, `draft_feedback(draft_id, reviewer)`. **v2:** the API runs as `aml_api` under row-level security (set `app.bank_id`, `app.role`, `app.user_id` with `set_config(…, true)` per transaction); services run as `aml_service`. Every new table with `bank_id` or `case_id` needs a policy. |
| `rls_isolation_test.sql` | CI (`make test-rls`) | Run on an empty throwaway database after the schema. Exits non-zero on the first broken isolation or maker-checker rule. Checked on Postgres 16, including two deliberately broken policies that it caught. |
| `agent_output.schema.json` | `services/agent/client.py` | Sent as `output_config.format = {"type": "json_schema", "schema": …}`. Must stay within structured-output limits: `additionalProperties: false` on every object, no numeric/length constraints, `minItems` only 0/1, scalar enums. If the API returns 400 on a keyword (e.g. `description`), strip that keyword in the client and keep this file as the documented source. Stricter rules (non-empty findings, grounding) live in `services/agent/validator.py`. |

Changing a contract = bump its version (or `spec_version`), add a decision entry, and update every producer and consumer in the same PR.
