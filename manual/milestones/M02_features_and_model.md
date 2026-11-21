# M02 — Feature oracle, Spark features, rules baseline, XGBoost

**Timebox:** 18 h fast / 22 h full · **Trigger:** F1 not above rules + 20 points at 22 h → record the miss, do one ablation round, move on. · **v2:** +1 h (section "Revision v2 additions")
**Role signal:** AI engineering rigor (leakage-safe evaluation, imbalance, thresholds, honest baselines, published comparison), big data (Spark on 32M rows).

## Inputs
`raw_transactions`, `patterns`, `split.json`, `fx_rates.yaml`, `features/spec.yaml`.

## Outputs
| Output | Where |
|---|---|
| Feature oracle (you write the core) | `features/reference.py` |
| Spec expansion → ordered feature names | `features/expand.py` → `features/feature_names_v1.txt` (generated, committed) |
| Golden vectors shared with Flink | `tests/golden/*.json` |
| Spark feature job | `batch/spark/features.py` → `aml_<ds>.features_tx` |
| Leakage tests | `tests/leakage/` |
| Rules baseline | `ml/rules_baseline.py` → `results/M02/<ds>/rules.json` |
| Model training + registry | `ml/train.py` → MLflow experiment `aml-<ds>`, model `aml-xgb`, alias `champion` |
| Evaluation (test touched once) | `eval/model_eval.py` → `results/M02/<ds>/metrics.json`, PR curve, per-pattern recall, feature importance |
| Thresholds | `config/thresholds.yaml` (per bank: F1-optimal on val, and alert-budget threshold) |
| Published comparison table draft | `docs/published_baselines.md` |

## Steps

### 1. Expand the spec (CC)
`features/expand.py` reads `spec.yaml` and emits the ordered name list, in this order: tx features, then `s_` account features (windows in spec order), then `r_` account features, then relational features. Write it to `feature_names_v1.txt`.
- Test: the generated file equals the committed one (so any spec change is visible in review).
- The same file is copied into the Flink module's resources in M4, with a test that both lists are identical.

### 2. The oracle (YOU write the core; Learning style)
The oracle is the definition of the features. Spark calls it directly. Flink must reproduce its golden outputs.

Signature:
```python
def compute_account_legs(legs: pd.DataFrame, spec: Spec) -> pd.DataFrame:
    """legs: all legs of ONE account, any order. Columns: tx_id, event_ts_ms, direction ('OUT'|'IN'),
    counterparty_key, amount_usd_cents, payment_currency, receiving_currency, payment_format.
    Returns one row per input leg: tx_id, role ('S' for OUT, 'R' for IN), and that account's
    feature values as of just before the transaction."""
```
Algorithm (single pass, two pointers per window):
```
sort legs by (event_ts_ms, tx_id, dir_order)          # OUT before IN within the same tx (self-transfer)
log = []                                              # appended entries, in order
first_seen = None; last_in_ts = None
for each group G of legs with the same tx_id:         # 1 leg normally, 2 for a self-transfer
    for leg in G:
        t = leg.event_ts_ms
        for W in windows:
            advance start_ptr[W] while log[start_ptr[W]].ts <= t - W   # start exclusive
            # window = log[start_ptr[W] : len(log)]  (all tx_id < current by construction)
            maintain running counts/sums with add-on-append, subtract-on-evict
            maintain counterparty multiset per (W, direction) for distinct counts
        features = read aggregates (+ null rules) + account_age (t - first_seen) + relational checks
        emit(tx_id, role, features)
    for leg in G:                                      # append AFTER all legs of this tx are computed
        hold = (t - last_in_ts)/60000 if leg.direction == 'OUT' and last_in_ts is not None else None
        log.append(entry(leg, hold)); update running aggregates for every W
        if leg.direction == 'IN': last_in_ts = t
        first_seen = first_seen or t
```
Notes:
- Eviction subtracts the evicted entry's contribution, and decrements the counterparty multiset (remove a key when its count hits 0 → `distinct` = number of keys).
- `hold_minutes` is stored on the OUT entry when it's appended, so `avg_hold_minutes_W` = mean over OUT entries in the window with non-null hold.
- Relational features for the transaction's two roles:
  - `s_new_counterparty_72h`: the receiver isn't in the sender's 72h OUT counterparty multiset.
  - `r_returned_funds_24h`: the receiver's 24h OUT multiset contains the sender.

  Each is computed by the account that owns the role, from its own state, so both can be computed in a stream.
- `s_amount_vs_out_avg_72h` needs the transaction amount and the sender's 72h out-average, both available on the sender side.
- Complexity: O(n) amortized per account per window, plus O(n) distinct-set maintenance.

### 3. Golden vectors (CC writes, YOU verify by hand)
Create 8–10 tiny scenarios in `tests/golden/`, each JSON `{transactions:[…], expected:{tx_id:{feature:value}}}`:
1. Single account, 3 OUT legs, windows exactly at the boundary (an entry at `t − 1h` is **excluded**; at `t − 1h + 1 min` it's included)
2. Same-minute ties: 3 transactions in one minute; each sees only smaller `tx_id`s
3. Self-transfer (sender = receiver): both legs see identical pre-state
4. Fan-out (1 → 5 within 1 h): `distinct_out_cp_1h` = 0, 1, 2, 3, 4 for successive legs
5. Fan-in (5 → 1)
6. 2-cycle return: A → B, then B → A within 24 h → `r_returned_funds_24h = 1` on the second transaction
7. Pass-through: IN 1000, then OUT 990 after 30 min → hold = 30, ratio = 0.99
8. Cross-currency mix and format shares
9. Empty history: null ratios, zero counts, `log_empty = 1`

Hand-compute the expected values. The bugs you find while doing that are the point.

### 4. Spark feature job (CC)
```
raw → legs = union(
   select tx_id, event_ts_ms, sender_key as account_key, 'OUT' as direction, receiver_key as counterparty_key, amount_paid_usd_cents as amount_usd_cents, …,
   select tx_id, event_ts_ms, receiver_key as account_key, 'IN' …, sender_key as counterparty_key, amount_received_usd_cents …)
→ groupBy(account_key).applyInPandas(compute_account_legs, schema=partial_schema)
→ pivot roles: join S-rows and R-rows on tx_id, prefix s_/r_
→ join tx-level features (computed in Spark SQL from raw)
→ select columns in feature_names_v1.txt order → write aml_<ds>.features_tx
```
- Watch heavy accounts. Use the per-account maximum from M1's EDA. If one group is millions of legs, raise `spark.executor.memory`/`spark.driver.memory` (local) or worker memory (EMR). A sequential computation can't be salted.
- Set `spark.sql.execution.arrow.pyspark.enabled=true`.
- Spark code must run on **AWS Glue 5.1** in Phase 5 (Spark 3.5.6, Python 3.11). Keep local PySpark on 3.5.x and Python 3.11 so the same files run in both places.

### 5. Leakage tests (CC)
- `test_no_label_in_features`: none of `is_laundering`, `pattern*`, `split` appears in `feature_names_v1.txt` or in the `features_tx` schema.
- `test_future_invariance` (the strong one): take a 50k-row slice and compute features. Then **randomly permute every column of all rows with `tx_id > s`** (or drop them) and recompute. Features for `tx_id ≤ s` must be bit-identical. This proves no feature looks forward.

### 6. Rules baseline (CC)
3–5 transparent rules, thresholds tuned on **train** (grid), evaluated on **val**, reported on **test**. For example:
- R1: cross-currency AND `amount_usd` > p99 of train
- R2: `s_pass_through_ratio_24h` ∈ [0.9, 1.1] AND `s_in_count_24h` ≥ 2
- R3: `s_distinct_out_cp_24h` ≥ 5 (fan-out)
- R4: `r_distinct_in_cp_24h` ≥ 5 (fan-in)
- R5: `r_returned_funds_24h` = 1

Report F1, precision, recall and alerts/day.

### 7. XGBoost (CC; you decide the protocol)
- Training rows: `split = train` and `event_ts ≥ data_start + 24h` (burn-in, D04).
- `tree_method=hist`, early stopping on val `aucpr`. Imbalance: try (a) `scale_pos_weight = neg/pos` and (b) unweighted. In both cases tune the decision threshold on val. **Never tune on test.**
- 5 seeds → mean ± std (the papers report mean ± std).
- MLflow: log params, `spec_version`, `fx_version`, the training data snapshot id (Iceberg snapshot), metrics, the feature-importance (gain) plot and the PR curve. Register `aml-xgb`; first good model → alias `champion`.
- Thresholds (`ml/thresholds.py`): per bank, (1) the F1-optimal threshold on val, and (2) the threshold that yields **K alerts per replay-day per bank** on val (K = 200 default, your "analyst capacity"). Write `config/thresholds.yaml`. M5 uses the budget threshold.

### 8. Evaluation (CC; test set touched once per model family)
`eval/model_eval.py` writes `metrics.json` with:
- illicit-class F1, precision, recall at the chosen threshold
- PR-AUC; ROC-AUC (secondary)
- precision@K per day (alert budget)
- **recall per pattern type** (from `patterns`)
- 95% bootstrap CI for F1 (1,000 resamples)
- the same metrics for the rules baseline, so the T2 delta is computed, not typed

### 9. Published comparison (YOU)
`docs/published_baselines.md`. Copy exact numbers from the original papers, with citations:
- GFP+LightGBM and GFP+XGBoost on HI-Small (≈ 62.9 and 63.2) and HI-Medium (≈ 59.5 and 65.7) minority F1[^mega]
- GIN baseline (28.7) and Multi-GNN adaptations on HI-Small[^egressy]

Add one sentence on why your features differ: they must be computable per event in a stream within a latency budget, while GFP mines subgraph patterns.

### 10. Optional (only if ahead)
- LightGBM comparison.
- Ablation: windows {1h, 24h} vs {1h, 24h, 72h}; with/without relational features.
- Prequential check: what F1 does a model trained on days 1–5 get on each later day? Shows drift.

## Revision v2 additions (+1 h): drift baseline

Phase 5's drift monitor (M08 v2) needs to know what "normal" looked like for the model that is serving. Save it now, while the training data is in hand.

**What to build:** `ml/drift_baseline.py`, called at the end of `ml/train.py` so **every model version gets its own baseline**. A model promoted later by M07's gate brings its own baseline with it.

**Input:** `features_tx` for the training window (after the 24 h burn-in) and the validation split; the trained booster.

**Output:** `results/M02/<ds>/baseline_dist.json`, also logged as an MLflow artifact on the model version:
```json
{
  "ds": "hi_small", "model_version": "3", "spec_version": 1,
  "window": {"from": "2022-09-02", "to": "2022-09-06"},
  "features": {
    "amount_usd_log": {"edges": [0.0, 2.1, 3.4, 4.0, 4.6, 5.2, 5.9, 6.7, 7.8], "props": [0.10, 0.10, 0.11, 0.09, 0.10, 0.10, 0.10, 0.10, 0.10, 0.10], "null_prop": 0.0},
    "...": {}
  },
  "score": {"edges": ["deciles of validation scores"], "props": ["..."]},
  "alerts_per_day": {"bank_a": 140.2, "bank_b": 96.5}
}
```
(The numbers above only show the shape.)

**Rules**
- Bins are deciles of the training window. Ties are common (many features are 0 for most rows), so drop duplicate edges; a feature can end up with fewer than 10 bins. Nulls get their own bin.
- The score baseline uses **validation** scores, not training scores; training scores are overconfident.
- `alerts_per_day` per bank uses the thresholds in `config/thresholds.yaml`, computed on validation days.

**Test:** `tests/ml/test_drift_baseline.py`: proportions sum to 1 per feature (± 1e-9); the edges are strictly increasing; recomputing on the same data gives the same file.

**Definition of Done (v2)**
- [ ] `baseline_dist.json` exists for hi_small and hi_medium and is attached to the champion's MLflow version

## Claude Code prompts
> /output-style learning — then: Scaffold features/reference.py with the signature in M02 §2, the data classes and the null rules from spec.yaml. Leave the main loop and window maintenance as TODO(human). Write tests/golden/ scenarios 1–9 with expected values as TODO comments for me to fill in, and a test runner that compares oracle output to expected.

> Implement batch/spark/features.py per M02 §4 using reference.compute_account_legs via applyInPandas. Then implement tests/leakage/test_future_invariance.py exactly as described in §5. Show me the per-account leg-count distribution before running on hi_medium.

> Implement ml/train.py, ml/thresholds.py and eval/model_eval.py per M02 §7–8. The test split must only be read inside eval/model_eval.py behind a --final flag that logs a warning and writes results/M02/<ds>/test_access_log.txt.

## Definition of Done
- [ ] Golden tests green (oracle); `feature_names_v1.txt` committed
- [ ] Leakage tests green, including future-invariance
- [ ] `features_tx` for hi_small and hi_medium
- [ ] `metrics.json` for rules and XGBoost on both datasets; T2 met, or the miss documented with an ablation
- [ ] `champion` alias set in MLflow; `config/thresholds.yaml` written
- [ ] `docs/published_baselines.md` with citations
- [ ] v2: drift baseline saved (see "Revision v2 additions")
- [ ] `/quiz M02`

## Interview questions
- Walk me through how `distinct_out_cp_24h` is computed incrementally. What's the complexity? What's in memory?
- How do you know no feature uses future data? (future-invariance test)
- Why tune the threshold on validation? How did you pick the operating point for analysts (alert budget)?
- Your F1 is below the published GFP+XGBoost number. Why, and what would you add first?

[^mega]: Table 16 in MEGA (arXiv 2412.00241), reproducing Blanuša et al. 2024. https://arxiv.org/pdf/2412.00241
[^egressy]: Egressy et al., AAAI 2024 (arXiv 2306.11586), §7.2. https://arxiv.org/pdf/2306.11586
