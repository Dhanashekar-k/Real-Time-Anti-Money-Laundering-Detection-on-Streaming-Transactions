# M11 — Evidence pack: README, results, video, resume bullets

**Timebox:** 8 h fast / 10 h full · **Never cut.** A great system with a weak README reads as a weak system.
**Role signal:** communication. Recruiters spend under a minute on the repo, engineers about 10. Both need to find the proof fast.

## Outputs
| Output | Where |
|---|---|
| README (structure below) | `README.md` |
| Generated results page | `docs/results.md` via `make results` (`tools/render_results.py`) |
| Architecture diagram | Mermaid in README + `docs/architecture.png` export |
| One-command local demo | `make demo` (HI-Small, < 30 min from a clean clone, **timed**) |
| 3-minute video | YouTube/Loom (unlisted), linked from the README |
| Resume bullets (filled from results only) | `docs/resume_bullets.md` (private copy; see `manual/06_interview_and_resume.md`) |
| Limitations + future work | in README |

## README structure (keep each section short)
1. **One line + GIF** (10-second loop: alert appears → case page → draft with clickable tx → decision).
2. **Results table** (generated). Each row: target · result · how it's measured · link to the JSON.
   | | Result | Measured by |
   |---|---|---|
   | Illicit F1, HI-Small test (vs rules, vs published) | … | `eval/model_eval.py` |
   | Feature parity Spark ↔ Flink | …% of N rows identical | `tests/parity` |
   | Sustained throughput on EKS | … tx/s | rate ladder, lag flat |
   | p99 ingest→alert latency (mode) | … s | stored per alert |
   | Fault runs: lost / extra / dup cases | 0 / 0 / 0 over N runs (upstream dups absorbed: …) | `ops/reconcile.py` |
   | Draft grounding / pattern accuracy (Haiku, Sonnet, Opus) | … | `eval/agent_eval.py` |
   | Bank C onboarding | … min, … code changes | timed runbook |
   | Triage time baseline → assisted (n=…) | … → … s | study |
   | Full HI-Large cloud cost | $… | Cost Explorer |
3. **Architecture** (diagram + the "one transaction's path" paragraph from 02).
4. **Run it locally** (exact commands; tested on a clean clone).
5. **Design decisions:** the 6 most interesting entries from `docs/decisions.md`, 2 lines each (legs + event-time sort; EOS vs ALO measured; one spec two engines; evidence cutoff; validator; config-only onboarding).
6. **Published baselines and honest comparison** (from `docs/published_baselines.md`).
7. **Limitations:** synthetic data; minute-resolution timestamps with sequence tie-breaking; no auth; n of the study; single-region; scale run dismantled (results and screenshots kept); the agent drafts, it doesn't decide.
8. **What I'd do next:** GFP-style subgraph features in the stream; a GNN comparison; a synchronous scoring API with an online store; per-bank private vs shared-graph ablation (Altman et al. show sharing helps[^altman]).
9. **Credits/licenses:** dataset CDLA-Sharing-1.0 (no data in the repo), papers cited.

## Video script (3:00)
| Time | Show | Say |
|---|---|---|
| 0:00 | Title + architecture | "Real-time AML detection on 180M synthetic transactions: Kafka, Flink, XGBoost, Claude-drafted cases, analyst review." |
| 0:15 | Grafana: rate, lag, latency | "Two simulated banks with different formats streaming at X tx/s; p99 to alert Y s." |
| 0:40 | UI queue → open case | "An alert opens a case. The graph is the 2-hop neighborhood as of the alert time." |
| 1:05 | Click tx chips in the draft | "The draft cites transaction IDs. A validator rejects any ID or amount that isn't in the evidence." |
| 1:30 | Decide → audit log | "The analyst decides; everything is audited." |
| 1:45 | Airflow run → MLflow alias flip | "Decisions feed a nightly retrain behind a promotion gate." |
| 2:05 | Terminal: kill TM + broker; reconcile output | "20 fault runs: zero lost, zero extra alerts." |
| 2:30 | `bank_c.yaml` + onboard-check | "A third bank onboarded by config in Z minutes." |
| 2:45 | Results table | "Every number links to the script that produced it." |

## Steps
1. **[CC]** `tools/render_results.py`: reads `results/**/*.json` → `docs/results.md` and the README table between markers `<!-- results:start -->` and `<!-- results:end -->`. Missing → "TBD (not measured)".
2. **[YOU]** Clean-clone test: a new macOS user account (or a fresh clone in `/tmp`), follow the README literally, time it, fix every stumble.
3. **[YOU]** Record the video (two takes max; don't polish forever).
4. **[YOU]** Fill in the resume bullets from `results/` only (06 has templates).
5. **[CC]** Repo hygiene: LICENSE (MIT for code), CI badge, `CONTRIBUTING` not needed, remove dead code, the `docs/` index.
6. **[YOU]** Make the repo public. Pin it on GitHub. Add it to LinkedIn Featured.

## Definition of Done
- [ ] `make demo` works from a clean clone in < 30 min (timed)
- [ ] README results table generated, every row linked
- [ ] Video linked
- [ ] Bullets drafted with real numbers
- [ ] `/teardown-check` empty; ECR repos and S3 bucket deleted, or kept with a lifecycle rule and a known monthly cost

[^altman]: Altman et al., NeurIPS 2023 D&B, per-bank sharing results (their Figure 10). https://arxiv.org/pdf/2306.16424
