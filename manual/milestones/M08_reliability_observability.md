# M08 — Fault injection, reconciliation, dashboards

**Timebox:** 10 h fast / 14 h full · **Trigger:** > 14 h → 10 runs instead of 20; 4 dashboard panels. · **v2:** +3 h (section "Revision v2 additions")
**Role signal:** SWE/SRE (failure testing, correctness under faults, observability), and usually your best debugging story.

## Inputs
The full local pipeline (M3–M6), a fixed champion model, fixed thresholds.

## Outputs
| Output | Where |
|---|---|
| Fault injector | `ops/fault_injection.py`, `make chaos RUNS=20` |
| Reconciliation (you own the comparison logic) | `ops/reconcile.py` |
| Per-run and summary results (T7) | `results/M08/run_<i>/{faults,reconcile}.json`, `results/M08/summary.json` |
| Dashboards as code | `infra/compose/grafana/dashboards/*.json`, screenshots in `results/M08/` |
| Bug write-ups | `docs/incidents.md` |

## Steps

### 1. Metrics everywhere (CC)
- Python services: `prometheus_client` on `/metrics`. Counters (in, out, errors by code, duplicates suppressed, DLQ), histograms (batch seconds, end-to-end latency), gauges (model_version).
- Flink: Prometheus reporter. [Likely] config: `metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory`, port 9249. Check that the reporter jar is on the classpath in the official image (it may need to be copied from `opt/` to `lib/` or `plugins/`).
- Kafka lag: Redpanda exposes metrics on its admin port. For per-group lag, add a small exporter (`ops/lag_exporter.py`: admin client → committed offsets vs high watermarks per group/partition) or use kminion.

### 2. Dashboards (CC)
One "Pipeline" dashboard: ingest rate by bank · DLQ rate · consumer lag per group · Flink records in/out per operator · Flink watermark lag (event time vs wall) · checkpoint duration and size · late legs · duplicates dropped (dedupe) · scorer rows/s and batch p99 · alerts/min per bank · end-to-end latency p50/p99 · duplicates suppressed · model_version.

### 3. Fault injection (CC)
`fault_injection.py --run i --duration 300 --faults 4 --seed i` during a replay of a fixed 2-day slice at `--speedup 20000`. Faults, chosen at random times:
- `docker kill` the Flink TaskManager (compose restarts it)
- `docker restart` Redpanda (single broker = a full outage; producers must retry and recover)
- kill the scorer / the alert service / one connector (`docker kill`, compose restarts)
- (EKS only, M9) delete one Kafka broker pod; delete a TM pod

Log the fault timeline. After the replay: drain Flink, wait for consumer lag = 0 everywhere, then reconcile.

### 4. Reconciliation (YOU write `compare()`; CC writes collection and reporting)
- **Golden run:** the same slice, same model version, same thresholds, no faults → `expected_alerts = {tx_id}` and `expected_scores = {tx_id: score}`.
- **Fault run** collects:
  - `actual_alerts` from Postgres
  - `scores_records`: every record in `tx.scores` for the slice (`read_committed`)
  - `duplicates_suppressed` from the alert service's metrics
  - from Flink: late legs, duplicates dropped, orphan halves
- Report:
  - `lost = expected − actual` (**must be 0**)
  - `extra = actual − expected` (must be 0; non-zero means non-determinism, e.g. a model swap during the run, or an ordering bug)
  - `dup_cases` = rows violating case uniqueness (0 by construction; check anyway)
  - `duplicates_produced` = tx_ids with > 1 record in `tx.scores` (expected > 0 after scorer kills: at-least-once)
  - `score_mismatches` = tx_ids whose score differs from golden (must be 0: features and model are deterministic)
- Hot swap must be **off** during chaos runs (pin the model version by number, not alias).

### 5. Run it
`make chaos RUNS=20` (about 5 min per run). Summarize with medians and maxima of each number across runs → `summary.json`.

### 6. When something breaks (it probably will) — write it up
`docs/incidents.md` entry: symptom → how you detected it (which panel/metric) → root cause → fix → test added. Common real bugs to look for:
- offsets committed before the DB transaction committed (alerts **lost** after a crash)
- scorer committing offsets before produce delivery succeeded (scores lost)
- Flink transactional id reused across deployments (fencing errors after restart)
- `transaction.timeout.ms` above the broker maximum (job fails at start)
- consumer not `read_committed` (duplicates or "ghost" records from aborted transactions)
- connector restart racing the replayer and producing duplicates (should be dropped by Flink dedupe; check the counter)

## Revision v2 additions (+3 h): drift monitor and an induced-shift experiment

**Why:** a model that was right last month can quietly go wrong. This shows you can detect that, measure how fast, and recover through the gated loop instead of by hand.

### Drift monitor (CC)
- `services/drift/`: consumer group `drift` on `tx.features` and `tx.scores`. It bins each value with the edges in `baseline_dist.json` of the **serving** `model_version` (M02 v2). At each event-time day boundary it writes `results/M08/drift/<day>.json` and exports gauges `feature_psi{bank,feature}`, `score_psi{bank}` and `alerts_per_day{bank}`.
- PSI per feature = Σ (actualᵢ − expectedᵢ) · ln(actualᵢ / expectedᵢ), with each proportion floored at 1e-4 so empty bins don't divide by zero.
- Thresholds: **0.1 = watch, 0.25 = act**. These are an industry rule of thumb ([Likely]), not a statistical test. Calibrate them in step 1 below.
- A drift alert fires (Grafana alert and a row in `audit_log`) when score PSI > 0.25, or ≥ 3 features > 0.25, or alerts per day fall outside ±50% of the baseline. It **doesn't retrain automatically**: retraining needs labels, labels arrive late, and promotion has a gate.

### Experiment (YOU design, CC runs)
1. **False-alarm check first.** Replay the unshifted hi_small test period. Record the largest daily PSI. If anything crosses 0.25 here, your bins or thresholds are wrong; fix them before going on.
2. **Induced shift.** Add a replayer option, e.g. `--shift bank=bank_b,from_day=D,kind=amount_scale,factor=3` or `kind=currency_mix`. It changes what the bank sends from day D on; labels stay as they were. Run the replay with the full pipeline and the nightly loop.
3. **Measure:**
   - detection delay = first day with a drift alert − D
   - the top drifting features (they should be the ones you shifted; if not, find out why)
   - precision, recall and alerts per day before D, after D, and after recovery (offline, by joining alerts to labels)
   - recovery: days until the gate promotes a model trained on the shifted days (the 2-day label delay applies), and the precision after that
4. **Say what it is:** a synthetic shift tests the monitor and the loop mechanics, not real-world drift.

**Output:** `results/M08/drift_experiment.json` and a Grafana drift panel (screenshot).

**Definition of Done (v2)**
- [ ] False-alarm check recorded (max PSI on unshifted data)
- [ ] One induced-shift run with delay, drop and recovery numbers
- [ ] D29 written

**Interview questions (v2)**
- How did you choose 0.25? What's your false-alarm rate?
- Your drift monitor fired. What happens next, and who decides?
- What drift would PSI miss? (A change in the relationship between features and labels while the feature distributions stay the same, i.e. concept drift. Only labels reveal it: the prequential F1 and the QA sample.)

## Definition of Done
- [ ] ≥ 20 fault runs: `lost = 0`, `extra = 0`, `dup_cases = 0`, `score_mismatches = 0`; `duplicates_produced` and `suppressed` reported, not hidden
- [ ] Dashboard JSON committed, screenshots saved
- [ ] At least one incident write-up, or an explicit note that none occurred, with the fault types covered
- [ ] v2: drift monitor and experiment (see "Revision v2 additions")

## Interview questions
- What does "0 lost" mean exactly, and how did you measure it?
- Why is it OK that the scorer produced duplicates?
- Tell me about a bug you found under fault injection. How did you find it?
- Which single metric would you page on for this system?
