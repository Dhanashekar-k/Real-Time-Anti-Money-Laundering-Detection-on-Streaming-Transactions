# 05 — Schedule, timeboxes, and what to cut

> **Updated:** the same work grouped into 7 phases, with the cloud service each phase uses on your Free plan, is in `07_phases_and_cloud_plan.md`. With the v2 additions the fast track is about **174 h** (was 142 h). The week-by-week calendar below still holds for the order of work; stretch it by about one week for v2.

**Bottom line:** at 40 h/week, the fast track is about **142 focused hours ≈ 3.5–4 weeks**: start Tue Nov 17, done around **Dec 13**, or Dec 20 with slippage and Thanksgiving. The full plan is 170–200 h ≈ 5 weeks. Every number here is [Guessing]; the timeboxes exist so a bad guess costs a known amount.

---

## 1. Fast-track calendar

| Week | Dates | Milestones | Hours | Checkpoint you can show someone |
|---|---|---|---|---|
| 1 | Nov 17–22 | M0 foundation (6) · M1 data (8) · M2 features + model (18) | 32 | Model beats rules on HI-Small with a leakage-safe split; MLflow run |
| 2 | Nov 23–29 (Thanksgiving) | M3 ingestion (10) · M4 Flink (28) | 38 | Two banks streaming; Flink features match Spark; restart test passes |
| 3 | Nov 30–Dec 6 | M5 scoring/alerts (10) · M6 agent (12) · M7 UI + retrain (12) · Glue job on HI-Large (part of M9, 4) | 38 | **Full local demo**: stream → alert → draft → analyst decision → retrain |
| 4 | Dec 7–13 | M8 reliability (10) · M9 EKS sessions (10) · M10 FDE validation (6) · M11 packaging (8) | 34 | Public repo, results table, 3-min video, resume bullets |

If Thanksgiving week is light, M4 spills into week 3 and everything shifts about 3 days. That's fine. What's not fine is letting M4 eat a second full week (see the triggers below).

**Things to start in week 1 even though they're used later:**
- [ ] AWS Step 0 and Step 1 (account type, budgets, quota requests). Quotas can take days.
- [ ] Message 4–6 people for the M10 triage study and book 30-minute slots in week 4.
- [ ] Download HI-Medium overnight (used in M2).

## 2. Week 1, day by day

| Day | Plan | Done when |
|---|---|---|
| Tue | Setup (04 §1), AWS Step 0–1, M0 scaffolding | `make up` healthy; budgets exist |
| Wed | M0 finish (CI, topics, Terraform bootstrap) · M1 download + `csv_to_iceberg` | HI-Small in Iceberg; counts recorded |
| Thu | M1 EDA, FX table, patterns, split · write `reference.py` (you) | `results/M01/*.json`; golden tests drafted |
| Fri | M2 golden tests pass · Spark `features.py` · leakage tests | `features_tx` for HI-Small |
| Sat | M2 rules baseline · XGBoost · eval on val | First MLflow run with F1/PR-AUC |
| Sun | M2 HI-Medium run · test-set evaluation (once) · `/quiz M02` | T2 decision recorded |

## 3. Timeboxes and fallback triggers

| Milestone | Fast | Full | Trigger | Fallback |
|---|---|---|---|---|
| M0 | 6 | 8 | Compose not healthy after 8 h | Drop Airflow and Grafana from compose until M7/M8 |
| M1 | 8 | 12 | Spark/Iceberg config fight > 3 h | Write Parquet first, register in Iceberg later (`add_files`); keep moving |
| M2 | 18 | 22 | F1 not above rules + 20 at 22 h | Record the miss, run one feature-ablation round, move on; revisit after M11 |
| M3 | 10 | 14 | > 14 h | Bank B keeps only the name/unit differences; skip the extra quirks |
| M4 | 28 | 30 | Walking skeleton (2 features, EOS, restart test) not done by hour 12; or > 40 h total | Ship without `distinct_*` (MapState) and format shares first; then ALO-only with idempotent alerts if EOS still fights you; document it |
| M5 | 10 | 14 | > 14 h | Single scorer replica locally; tune later in M9 |
| M6 | 12 | 16 | > 16 h | Skip the tool-using agent variant; Haiku + Sonnet only (no Opus) in the eval |
| M7 | 12 | 18 | > 16 h | UI: queue + case page + decision only (no study mode until M10); retrain via `make retrain`, add Airflow last |
| M8 | 10 | 14 | > 14 h | 10 fault runs instead of 20; dashboards: 4 panels |
| M9 | 14 | 18 | Cluster not healthy in Session A after 2 h | Destroy, debug locally with kind/k3d, try again another day. **Never debug for hours on a billing cluster.** |
| M10 | 6 | 10 | Reviewers unavailable | 3 reviewers, or report T10 only and label T9 "not run" |
| M11 | 8 | 10 | — | Never cut: README results table + video + bullets |

## 4. Cut list

**Must (the project fails without these):** M1 split + counts · M2 model vs rules vs published table · M3 two banks + DLQ · M4 legs/event-time/parity/restart · M5 idempotent alerts + latency measurement · M6 validator + pattern-accuracy eval · M8 reconcile with 3 numbers · M9 HI-Large Spark + one EKS replay + teardown · M11 evidence pack.

**Should:** M7 UI with evidence graph · Airflow nightly retrain with promotion gate · EOS vs ALO latency matrix · M10 Bank C onboarding timed · three-model agent comparison.

**Could:** M10 triage study (n ≥ 3) · LLM mapping assistant for Bank C · RocksDB incremental checkpoint experiment · a few GFP-style graph features · tool-using agent variant.

**Won't (unless everything above is done):** Neo4j · Redis online store · GNN · 3-hop `short_cycle_flag` · HI-Large model training · public hosted demo.

## 5. Weekly self-check (Sunday, 15 min)
1. Which milestone DoDs are green, with result files to prove it?
2. Which trigger fired this week, and did I take the fallback or ignore it?
3. What's the riskiest thing in next week's plan, and what's the cheapest test of it on Monday?
4. Are any AWS resources running? (`/teardown-check`)
5. Can I explain every human-owned file without looking? (`/quiz` on the weakest)
