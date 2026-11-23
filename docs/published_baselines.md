# Published baselines — IBM AMLworld HI-Small / HI-Medium

Minority-class F1 (%), mean ± std, from the literature on the same datasets
(HI-Small 515,088 accounts / 5,078,345 tx; HI-Medium 2,077,023 / 31,898,238; node and edge counts from
MEGA-GNN Table 13).

| Method                          | HI-Small      | HI-Medium     | Source                              |
|---------------------------------|---------------|---------------|-------------------------------------|
| GFP + LightGBM                  | 62.86 ± 0.25  | 59.48 ± 0.15  | Egressy et al. 2024, Table 2        |
| GFP + XGBoost                   | 63.23 ± 0.17  | 65.70 ± 0.26  | Egressy et al. 2024, Table 2        |
| GIN                             | 28.70 ± 1.13  | 42.20 ± 0.44  | Egressy et al. 2024, Table 2        |
| PNA                             | 56.77 ± 2.41  | 59.71 ± 1.91  | Egressy et al. 2024, Table 2        |
| Multi-GIN+EU                    | 64.79 ± 1.22  | 58.92 ± 1.83  | Egressy et al. 2024, Table 2        |
| Multi-PNA+EU                    | 68.16 ± 2.65  | 66.48 ± 1.63  | Egressy et al. 2024, Table 2        |
| MEGA-PNA (best on HI-Medium)    | 74.01 ± 1.55  | 78.26 ± 0.11  | MEGA-GNN 2024, Table 16 (Appendix E.1) |
| Multi-FraudGT (best on HI-Small)| 75.81 ± 0.75  | 75.97 ± 0.18  | MEGA-GNN 2024, Table 16 (Appendix E.1) |

Sources:
- B. Egressy, L. von Niederhäusern, J. Blanuša, E. Altman, R. Wattenhofer,
  K. Atasu. "Provably Powerful Graph Neural Networks for Directed Multigraphs."
  AAAI 2024. arXiv:2306.11586, Table 2 (§7.2 AML Results).
- MEGA-GNN. arXiv:2412.00241, Table 16 (Appendix E.1) for the AML results and Table 13 for the dataset
  statistics. (The paper's own Table 2 is a different task, MOOC/Wikipedia/Reddit ROC-AUC.)

Transcription notes (checked 2026-11-22 against the arXiv HTML of both papers):
- The GFP rows are identical in both papers. The GNN rows are not: MEGA-GNN re-ran the baselines, so
  plain GIN is 46.50 / 58.65 and PNA 62.96 / 66.87 there, against 28.70 / 42.20 and 56.77 / 59.71 above.
  The rows above use Egressy et al. for every method it reports; MEGA-GNN Table 16 only for the last two.
- The two "+EU" rows are the Egressy et al. variants with ego-ID and port numbering and edge updates;
  plain Multi-GIN there is 57.15 / 56.23 and Multi-PNA 64.59 / 65.67.
- I did not find a sentence in either paper saying how the decision threshold for the reported F1 was chosen.
  Egressy et al. split 60/20/20; MEGA-GNN Table 13 lists 64/19/17 (HI-Small) and 61/17/22 (HI-Medium).
- Author list and title of Egressy et al. were not re-checked against the paper.

## How our model compares
Our model uses 85 hand-built features (mostly per-account temporal window features at 1h/24h/72h, plus
transaction-level and relational ones) computed by one hand-verified reference implementation (the
oracle), not GFP graph features or GNNs, so this is indicative, not head-to-head. Splits and threshold
protocols differ too: ours is the per-bank F1-optimal threshold chosen on validation, and the papers'
threshold protocol is not stated in the text checked.

On the tail-free test split (the fair comparison; our tail-inclusive numbers are inflated), our
minority-class F1 is 48.77% (HI-Small) and 61.51% (HI-Medium). On HI-Small that is below both GFP
gradient-boosting baselines (62.86, 63.23) and below every GNN row above except plain GIN (28.70). On
HI-Medium it is above GFP+LightGBM (59.48), PNA (59.71), Multi-GIN+EU (58.92) and GIN (42.20), below
GFP+XGBoost (65.70) and Multi-PNA+EU (66.48), and well below the best GNN result (78.26). Our all-test
F1-optimal numbers (63.09%, 73.72%) are inflated by the sparse tail and must not be read as
beating these baselines. At the K=200 alerts/bank/day operating point our tail-free budget F1 is
30.45% (HI-Small) and 0.00% (HI-Medium: no alerts, score saturation) — the deployment-style
metric, not comparable to the reported F1.

Figures in this section are read from `results/M02/hi_small/metrics.json` and
`results/M02/hi_medium/metrics.json` (SHA e4f43d5, not provisional).
