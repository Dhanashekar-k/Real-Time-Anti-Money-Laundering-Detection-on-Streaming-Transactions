# 06 — How this project maps to SDE, AI Engineer and FDE roles

## 1. What it proves, role by role

| Signal hiring teams look for | SDE | AI Eng | FDE | Where the proof lives |
|---|:-:|:-:|:-:|---|
| Correctness in distributed systems (exactly-once, idempotency, outbox) | ●●● | ● | ●● | M4 §5, M5 §2, M8 reconcile |
| Streaming at volume (Kafka, Flink event time, state) | ●●● | ● | ● | M4, M9 rate ladder |
| Big data batch (Spark, Iceberg, AWS Glue, partitioning) | ●● | ●● | ● | M1, M2, M9 Part A |
| Cloud + IaC + cost discipline (Terraform, EKS, IAM, receipts) | ●●● | ● | ●●● | M9, `03`, Cost Explorer screenshot |
| ML modeling and honest evaluation (leakage tests, time split, published comparison) | ● | ●●● | ● | M2 |
| ML in production (registry, gate, hot swap, versioned scores, label delay) | ●● | ●●● | ●● | M5, M7 |
| LLM product engineering (grounding validator, structured output, cost/quality eval) | ● | ●●● | ●●● | M6 |
| Customer data integration by config (DLQ, onboarding tool, timed) | ●● | ● | ●●● | M3, M10 |
| User workflow + measured outcome (UI, audit, triage study) | ● | ● | ●●● | M7, M10 |
| Typed languages (Java, TypeScript) | ●● | ● | ●● | Flink job, web/ |
| Testing discipline (golden vectors, parity, chaos) | ●●● | ●● | ●● | tests/, results/ |

**What this project can't fix** [Certain]:
- **Coding screens.** SDE loops (Amazon included) are gated by DSA rounds no project bypasses. Keep your DSA track running alongside, even at 5–8 h/week.
- **Customer-facing experience.** The triage sessions are the closest you'll get. Run them like customer sessions (agenda, demo, listen, write follow-ups) and you can talk about them as such.
- **Real scale.** You'll have run 180M rows for a few hours, not operated a system for months. Say exactly that.

## 2. Resume bullets (templates; fill only from `results/`)
Pick 3–4 per application and reorder for the role. Each bullet is under 2 lines.

**SDE / data-infra version**
- Built a real-time AML pipeline (Kafka, Flink/Java, Python) that sustained **[X]k tx/s** on AWS EKS over a **180M**-transaction replay, with **p99 ingest-to-alert [Y] s**; Terraform + Helm, torn down after measurement (**$[Z]** total).
- Implemented event-time per-account features in Flink (keyed state, timers, watermark buffering) and an identical Spark/Iceberg batch path; a parity test matched **[P]%** of **[N]** feature vectors exactly, eliminating training–serving skew.
- Verified **0 lost / 0 extra** alerts across **[R]** fault-injection runs (broker, TaskManager and service kills) using Kafka transactions plus an idempotent Postgres outbox; measured that exactly-once cost **[Δ] s** of p99 latency vs at-least-once.

**AI Engineer version**
- Trained XGBoost on leakage-tested streaming features over a temporal split: illicit-class F1 **[F]** vs **[R]** for a rules baseline (published GFP+XGBoost: 63.2 on HI-Small), with an MLflow promotion gate and label-delayed nightly retraining from analyst decisions.
- Built a Claude case-drafting service with structured outputs and a deterministic validator (every cited transaction ID and amount checked; **100%** of shown drafts grounded); evaluated Haiku/Sonnet/Opus on **[200]** cases: pattern accuracy **[a]/[b]/[c]%** at **$[x]/[y]/[z]** per 1k cases.

**FDE version**
- Onboarded a third bank with a different file format (locale decimals, offsets, foreign vocabularies) through YAML config only in **[M] min** with **[0]** code changes, using a dry-run onboarding tool with dead-letter diagnostics [and an LLM-proposed mapping].
- Shipped an analyst review app (React/TypeScript, FastAPI) with evidence graphs, cited drafts and an audit trail; in a timed study (n=**[k]** reviewers, **[N]** cases) median review time fell from **[t1]** to **[t2]** s at **[acc]%** decision accuracy.

## 3. Answers to rehearse (outline them out loud; don't memorize)

**Exactly-once end to end?**
Inside Flink: source offsets are in the checkpoint, and the sink commits Kafka transactions on checkpoint completion (two-phase commit); consumers read committed. Outside Flink (scorer, alerts): at-least-once plus idempotency. The alert insert and case upsert are one DB transaction keyed by `tx_id`, committed before the offset. Where a duplicate can sneak in: a consumer not using `read_committed`, a reused transactional id, or any sink that isn't idempotent. Then quote what you measured: duplicates produced vs suppressed.

**Late and out-of-order events?**
Watermark = max event time − OOO bound. Legs are buffered per account and processed in `(ts, tx_id)` order when the watermark passes, so arrival order doesn't matter. Late legs go to a side output and are counted. The OOO bound was set by measurement (late = 0 at speed S) and costs OOO/speedup in latency.

**How do you know training and serving features match?**
One spec. A Python oracle that Spark calls directly. Golden vectors the Flink operator must reproduce with shuffled input. Integer-cent money and an exact total order. Then a full-dataset parity run: [P]% identical, plus what any mismatch turned out to be.

**Why a time-based split?**
A random split puts later members of the same laundering pattern into training while earlier ones sit in test. It also lets features computed from "future" neighbors look normal. F1 goes up and means nothing in production. (If you ran the random-split comparison, quote the gap.)

**Why trees, not a GNN?**
Trees are fast, explainable per row (SHAP), and take features you can compute per event inside a latency budget. Published GNNs beat simple features on this data, partly because the simulator's patterns are graph motifs. When would you switch? When subgraph features stop closing the gap and you can afford mini-batch graph inference. Multi-GNN-style models are the reference.

**How do you stop the agent from making things up?**
Evidence is assembled by code and cut off at the alert time, with no labels. The output is schema-constrained. A validator checks every ID, account and amount, plus numbers in free text. One retry with the errors, then "needs review". Pattern accuracy is measured against ground truth. Model choice was decided by cost per correct case.

**Throughput bottleneck?**
Method: a rate ladder, watching which consumer's lag grows first, then changing one variable at a time. Then the specific finding and the before/after numbers.

**New bank tomorrow?**
Sample → YAML → `onboard-check` until 0% DLQ → register the bank and thresholds → start the connector. No code deploy. If they need a new transform, it goes in the engine generically, once.

**What would change with real data?**
Labels are delayed and partial (most laundering is never caught). PII and data residency. Model-risk governance (documentation, validation, monitoring). Explainability for investigators and regulators. Cross-bank data-sharing limits (per-bank private graphs perform much worse in Altman et al.'s experiments).

**What did Claude Code write vs you?**
Scaffolding, infrastructure, UI and tests were generated and reviewed by you. You wrote the oracle, the Flink keyed functions, the exactly-once configuration, the validator and the reconciliation logic, and you can whiteboard each one. Then offer to.

## 4. Skeptic objections and honest replies
| Objection | Reply |
|---|---|
| "It's synthetic data." | Yes, chosen because labels are complete and the patterns are known, which makes evaluation meaningful. Real-data differences are in Limitations. |
| "Your F1 is below published." | The constraint was streaming computability inside a latency budget. Here's the gap, and here's the ablation showing what graph features add. |
| "Zero duplicates is just a unique constraint." | Correct, which is why I report upstream duplicates produced and suppressed, and `lost`, which the constraint can't hide. |
| "n=4 isn't a study." | Agreed: directional evidence, and I said so in the write-up. |
| "You tore it down; how do I know?" | Results JSON from scripts, dashboard screenshots, the Cost Explorer bill, and `make demo` to reproduce locally. |

## 5. Outreach pitch (for startups and mid-size companies)
> I built a streaming fraud/AML detection system end to end: Kafka and Flink features with Spark parity, an XGBoost scorer, LLM-drafted cases with a grounding validator, an analyst UI, and an AWS scale run at [X]k tx/s for $[Z]. I also onboarded a new data source by config in [M] minutes. The repo has every number linked to the script that produced it. I'm looking for SDE / AI-engineer / forward-deployed roles where I'd build and ship systems like this with customers.
