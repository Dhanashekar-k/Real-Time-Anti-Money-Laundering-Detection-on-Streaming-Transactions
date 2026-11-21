# M01 — Data into Iceberg, understood

**Timebox:** 8 h fast / 12 h full · **Trigger:** Spark/Iceberg config fight > 3 h → write Parquet first, register with Iceberg `add_files` later.
**Role signal:** Big data (Spark, Iceberg, partitioning), data-quality rigor, leakage-aware evaluation design.

## Inputs
- Kaggle account + API token (`~/.kaggle/kaggle.json`, `chmod 600`)
- Dataset: `ealtman2019/ibm-transactions-for-anti-money-laundering-aml` (CDLA-Sharing-1.0)
- `features/spec.yaml` (money + ordering sections), `connectors/banks/bank_a.yaml` (currency and format names)

## Outputs
| Output | Where |
|---|---|
| Raw files + your own SHA-256 and row counts | `data/raw/` (gitignored), `data/checksums.txt` (committed) |
| `aml_hi_small.raw_transactions`, `aml_hi_medium.raw_transactions` | Iceberg, partitioned by `days(event_ts)` |
| `aml_<ds>.patterns` | Iceberg |
| Frozen FX table | `features/fx_rates.yaml` (decimal strings, `fx_version`) |
| Split boundaries | `results/M01/<ds>/split.json` + entry D17 in `docs/decisions.md` |
| EDA facts | `results/M01/<ds>/eda.json` + `docs/eda_<ds>.md` (generated) |
| Money helper used everywhere | `features/money.py` (+ Spark-native equivalent in `batch/spark/common.py`) |

## Steps

1. **[YOU] Download.**
   ```bash
   mkdir -p data/raw && cd data/raw
   kaggle datasets download -d ealtman2019/ibm-transactions-for-anti-money-laundering-aml -f HI-Small_Trans.csv
   kaggle datasets download -d ealtman2019/ibm-transactions-for-anti-money-laundering-aml -f HI-Small_Patterns.txt
   # repeat for HI-Medium_*; unzip whatever comes zipped
   shasum -a 256 *.csv *.txt >> ../checksums.txt && wc -l *.csv >> ../checksums.txt
   head -3 HI-Small_Trans.csv; head -20 HI-Small_Patterns.txt
   ```
   Kaggle doesn't publish checksums, so "verify" means: record your own hashes, then compare row counts with the paper's Table 4 (HI-Small ≈ 5M transactions over 10 days; HI-Medium ≈ 32M over 16 days).[^t4]
2. **[CC] Header handling.** [Likely] the transaction CSV has 11 columns and a **duplicate `Account` header** (pandas renames the second one `Account.1`). Don't rely on header names. Read with an explicit positional schema: `ts_raw, from_bank, from_account, to_bank, to_account, amount_received, receiving_currency, amount_paid, payment_currency, payment_format, is_laundering`. Read amounts as **strings** and convert to `DecimalType(38,18)`, never float.
3. **[CC] Stable order and `tx_id`.**
   - `src_row` = exact file row index (`rdd.zipWithIndex`, not `monotonically_increasing_id`).
   - `event_ts` = parse `yyyy/MM/dd HH:mm` as UTC; `event_ts_ms` = epoch millis.
   - Sort by `(event_ts_ms, src_row)`, then `zipWithIndex` again → `seq`. **`tx_id = seq`.**
   - `sender_key = from_bank || ':' || from_account`, same for the receiver.
   - Tests: `tx_id` unique and contiguous 0..N−1; `(event_ts_ms, tx_id)` strictly increasing.
4. **[CC + YOU] FX table** (`batch/spark/fx.py` → `features/fx_rates.yaml`).
   - From rows where `payment_currency ≠ receiving_currency`, compute the implied rate `amount_received / amount_paid` for each currency pair.
   - Check variance per pair. [Unclear] whether the generator used constant rates. If the coefficient of variation is near 0, freeze one rate per currency to USD. If rates drift by day, freeze a **per-day** table and say so in decisions.
   - Triangulate currencies with no direct USD pair through EUR.
   - Write the rates as **decimal strings** with `fx_version: 1`.
5. **[CC] Money conversion** (`features/money.py`, the single implementation that connectors also import):
   - `to_minor(amount_str, currency)` = `Decimal(amount).quantize(10**-exp, ROUND_HALF_EVEN) × 10**exp` as int. Exponents: JPY 0, BTC 8, others 2.
   - `minor_to_usd_cents(minor, currency)` = `(Decimal(minor) / 10**exp × fx[currency] × 100).quantize(1, ROUND_HALF_EVEN)` as int.
   - The Spark version uses `bround` (HALF_EVEN) on decimals. Test: 1M random rows through both give **identical** integers.
6. **[CC] `csv_to_iceberg.py`.** Write `raw_transactions` with the columns in `02_architecture_and_stack.md` §4, plus:
   - `owner_bank_id`: `A` if `stable_hash(sender_bank) % 2 == 0`, else `B`. Use xxhash or md5, not Python's `hash()`.
   - `split`: train, val or test (step 8).

   Partition by `days(event_ts)`; sort within partitions by `tx_id`.
7. **[CC] Patterns** (`batch/spark/patterns.py`).
   - Parse blocks `BEGIN LAUNDERING ATTEMPT - <TYPE>…` / `END LAUNDERING ATTEMPT`. [Likely] the types are FAN-OUT, FAN-IN, CYCLE, GATHER-SCATTER, SCATTER-GATHER, STACK, BIPARTITE, RANDOM; read them from the file rather than trusting this list.
   - Inside a block, each line has the same CSV layout as the transactions file. Join on **all** columns to get `tx_id`, and record ambiguous matches.
   - Report coverage: what share of `is_laundering = 1` rows sit in some attempt. [Likely] not 100%.
8. **[CC] Split** (`batch/spark/split.py`), matching the published protocol: a 60/20/20 temporal split by transaction order, where validation and test can see earlier history.[^egressy]
   - `t1` = `event_ts` at index ⌊0.6N⌋ in `(event_ts, tx_id)` order; `t2` = `event_ts` at index ⌊0.8N⌋.
   - train = `ts < t1`, val = `t1 ≤ ts < t2`, test = `ts ≥ t2`. Using the **timestamp** boundary keeps same-minute transactions together; note the small difference from a pure index split in decisions.
   - Write `results/M01/<ds>/split.json`: t1, t2, row counts and laundering counts per split.
9. **[CC] EDA → `results/M01/<ds>/eda.json`** (script, not just a notebook):
   - totals, laundering count and rate (the paper says HI-Small is 1 in 981 per Table 4, but also lists "3.6K" laundering transactions, which doesn't match 5M/981 ≈ 5.2K; [Unclear], **your count is the truth**, record it)[^t4]
   - **transactions per day** (look for a sparse tail of extra days after the main period, and decide whether to keep it)
   - distinct accounts; transactions-per-account distribution with the **max** (matters for Spark `applyInPandas` memory and Flink state)
   - self-transfer count (sender = receiver)
   - laundering rate by payment format, by currency, cross-currency vs not
   - attempts per pattern type, and laundering transactions per type
   - share of transactions per owner bank (A/B)
10. **[CC] HI-Medium** through the same pipeline (Spark local; give the driver 8–12 GB: `spark.driver.memory`).

## Claude Code prompt
> Implement M01 per manual/milestones/M01_data_lakehouse.md for hi_small first. Tests first: tests/unit/test_money.py (half-even cases, JPY, BTC, Spark-vs-Python equality on generated rows), tests/integration/test_raw_table.py (tx_id contiguous, order strictly increasing, no nulls in key columns, split counts add up). Do not load data/raw files into context; inspect with head/wc/duckdb. When the FX variance check runs, stop and show me the table before freezing fx_rates.yaml.

## Definition of Done
- [ ] Row counts recorded and within 1% of the published sizes (5M / 32M); laundering counts recorded
- [ ] `fx_rates.yaml` frozen with a stated method; money tests green
- [ ] Split boundaries in `split.json` and in decision D17
- [ ] Patterns table with coverage %
- [ ] `eda.json` for hi_small and hi_medium; the max transactions per account noted in decisions (it sizes M2 and M4)

## Interview questions this milestone prepares you for
- Why did you sort by `(timestamp, file row)` and assign your own sequence?
- Why decimals and integer cents instead of floats?
- Why a timestamp-based split instead of a random one? What would a random split do to F1, and why is that number wrong?
- What fraction of laundering transactions aren't in any labeled pattern, and how does that affect your per-pattern metrics?

[^t4]: Altman et al., NeurIPS 2023 D&B, Table 4. https://papers.neurips.cc/paper_files/paper/2023/file/5f38404edff6f3f642d6fa5892479c42-Supplemental-Datasets_and_Benchmarks.pdf
[^egressy]: Egressy et al., AAAI 2024, Appendix E.1 (60-20-20 temporal split). https://arxiv.org/pdf/2306.11586
