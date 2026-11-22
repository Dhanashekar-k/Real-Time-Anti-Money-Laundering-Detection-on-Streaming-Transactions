# Golden values — hand computation log

Filled 2026-11-22, written before `make golden` was run. Sources used: `features/spec.yaml`,
`features/feature_names_v1.txt`, `manual/milestones/M02_features_and_model.md` §2–3, `docs/decisions.md`
(D36, D40–D43), and each scenario's `transactions`. `features/reference.py` and
`tests/unit/test_reference_bruteforce.py` were not opened. `tests/golden/test_golden.py` was not opened either.

Notation: times are minutes from the 12:00Z origin; `[t-W, t)` means "ts > t−W, tx_id < current"
(start exclusive). "A-log" = account A's entries that the leg can see. 1h=60, 24h=1440, 72h=4320 min.
Floats are IEEE doubles (one correctly rounded division); ints stay ints; null = JSON null.

## s01 window boundary (A→B ×5 at 0, 1, 61, 62, 121; 10000 c each). s_* = A's OUT, r_* = B's IN
| tx | t | 1h window = ts > t−60 | s_out_count_1h / s_out_sum_1h | s_out_count_24h | r_in_count_1h |
|---|---|---|---|---|---|
| 1 | 0 | empty log | 0 / 0 | 0 | 0 |
| 2 | 1 | {0} (0 > −59) | 1 / 10000 | 1 | 1 |
| 3 | 61 | ts > 1: 0 out, 1 out (exactly 1h old) | 0 / 0 | 2 (0,1 > −1379) | 0 |
| 4 | 62 | ts > 2: only 61 | 1 / 10000 | 3 | 1 |
| 5 | 121 | ts > 61: 61 out (exactly 1h), 62 in | 1 / 10000 | 4 | 1 |

## s02 same-minute ties (all at minute 0; A→B 100, 200, 400; then A→C 800)
Same ts ⇒ every earlier entry is inside 1h; visibility is by tx_id only.
- tx1: A-log empty → out_count 0, out_sum 0, distinct_out 0, new_cp(B ∉ ∅)=1. B-log empty → r_in 0 / 0.
- tx2: A-log {tx1→B 100} → 1, 100, distinct {B}=1, new_cp(B ∈ {B})=0. B-log {tx1 100} → 1 / 100.
- tx3: A-log {tx1, tx2} → 2, 100+200=300, {B}=1, new_cp 0. B-log → 2 / 300.
- tx4: A-log {tx1..tx3} → 3, 100+200+400=700, {B}=1, new_cp(C ∉ {B})=1. r_ is C's, C-log empty → 0 / 0.

## s03 self-transfer (X→A 1000 @0; A→A 500 @10; A→B 300 @20)
- tx1: r_ = A, empty → r_log_empty 1, r_in_count_1h 0. s_ = X, empty → s_log_empty 1.
- tx2 (both legs see A-log = {IN tx1 from X, 1000, @0}): s_out_count_1h 0, r_out_count_1h 0;
  s_in_count_1h 1, r_in_count_1h 1; s_in_sum_1h 1000, r_in_sum_1h 1000; s_new_cp 0 and r_new_cp 0 (D43:
  self-transfer leg); s_avg_hold_minutes_1h: no hold-bearing OUT entry in window → null;
  self_transfer 1 (001:A = 001:A); same_bank 1 (001 = 001).
- Append after tx2: OUT entry hold_ms = 600000 − 0 = 600000 (last IN with smaller tx_id is tx1; tx2's own IN
  has the same tx_id, so it does not count), then IN entry (last_in_ts := @10). Neither enters a cp multiset.
- tx3 (t=20, 1h window ts > −40 → all of A-log): OUT {tx2 500} → s_out_count_1h 1; IN {tx1 1000, tx2 500}
  → s_in_count_1h 2; distinct_out = {} (tx2 OUT is self) → 0; distinct_in = {X} → 1;
  pass_through = 500 / 1500 = 0.3333333333333333; avg_hold = 600000 / (1·60000) = 10.0.

## s04 fan-out (A→B1..B5 @0,5,10,15,20; A→B1 @25; 1000 c each; all within 1h)
tx k sees the k−1 earlier OUT entries: out_count = k−1, out_sum = 1000(k−1), distinct_out = k−1
(B1..B_{k−1} all distinct for k ≤ 6). new_cp = 1 for tx1..tx5 (B_k not yet seen); tx6: B1 ∈ {B1..B5} → 0.
| tx | 1 | 2 | 3 | 4 | 5 | 6 |
|---|---|---|---|---|---|---|
| count / sum / distinct / new | 0/0/0/1 | 1/1000/1/1 | 2/2000/2/1 | 3/3000/3/1 | 4/4000/4/1 | 5/5000/5/0 |

## s05 fan-in (B1..B5→A @0..20; B1→A @25; 1000 c each) — mirror of s04 on A's IN side
| tx | 1 | 2 | 3 | 4 | 5 | 6 |
|---|---|---|---|---|---|---|
| r_in_count / r_in_sum / r_distinct_in / r_new | 0/0/0/1 | 1/1000/1/1 | 2/2000/2/1 | 3/3000/3/1 | 4/4000/4/1 | 5/5000/5/0 |

## s06 2-cycle return (A→B 5000 @0; B→A 4000 @360; C→A 100 @420; B→A 100 @1800)
- tx2 (B→A, t=360): A's 24h window (ts > −1080) = {OUT tx1→B} → OUT cp B = sender → r_returned_funds_24h 1.
  A has no IN entries → B new → r_new_cp 1. B-log = {IN tx1 from A}, no OUT → A ∉ ∅ → s_new_cp 1.
- tx3 (C→A, t=420): A 24h = {OUT→B, IN←B}; no OUT with cp C → r_returned 0. A's 72h IN cps {B}; C ∉ → r_new_cp 1.
- tx4 (B→A, t=1800): A 24h is ts > 360: tx1 @0 out, tx2 @360 out (exactly 24h), tx3 @420 in.
  No OUT entry in 24h → r_returned 0. A 72h (ts > −2520) IN cps {B, C}; B ∈ → r_new_cp 0.
  B 72h OUT cps {A} (tx2); receiver A ∈ → s_new_cp 0.

## s07 pass-through (X→A 100000 @0; A→Y 99000 @30; A→Z 100 @40)
- tx2 (t=30): A-log {IN 100000}. s_in_sum_1h 100000, s_out_sum_1h 0, pass_through 0 / 100000 = 0.0
  (in_sum ≠ 0, so not null), avg_hold: no OUT entry → null. Append OUT hold_ms = 1800000 − 0 = 1800000.
- tx3 (t=40, window ts > −20): s_in_sum_1h 100000, s_out_sum_1h 99000, pass_through 99000 / 100000 = 0.99,
  avg_hold = 1800000 / (1·60000) = 30.0. s_amount_vs_out_avg_72h: avg = 99000 / 1 = 99000.0, then
  100 / 99000.0 = 0.00101010101010101.

## s08 currency and formats
- tx1: A-log empty → r_cross_currency_share_72h null. USD ≠ EUR → is_cross_currency 1. USD index 0 → payment_currency_code 0.
- tx3: EUR ≠ USD → 1. WIRE is index 6 in [ACH, BITCOIN, CASH, CHEQUE, CREDIT_CARD, REINVESTMENT, WIRE] → 6.
  EUR index 1 → payment_currency_code 1. USD → receiving_currency_code 0.
- tx5 (t=40; 1h and 72h windows both hold all four earlier entries):
  entries = IN tx1 (USD/EUR, CREDIT_CARD), OUT tx2 (USD/USD, ACH), OUT tx3 (EUR/USD, WIRE), OUT tx4 (USD/USD, WIRE).
  cross-currency = {tx1, tx3} = 2 of 4 → s_cross_currency_share_1h 0.5, _72h 0.5.
  Format shares over the 3 OUT entries only: ACH 1/3 = 0.3333333333333333, WIRE 2/3 = 0.6666666666666666,
  CASH 0/3 = 0.0 (tx5's own CASH is not visible), CREDIT_CARD 0/3 = 0.0 (tx1 is IN, not counted).

## s09 empty history (A→B 1000 @0; A→C 500 @90)
- tx1: A and B both empty. Counts/sums 0 (s_out_count_1h, s_in_count_72h, s_out_sum_24h, s_distinct_out_cp_1h);
  ratios/averages/shares null (pass_through_1h, avg_hold_72h, cross_currency_share_1h, format_share_ACH_72h);
  account_age null (no first_seen); s_log_empty 1; s_new_cp 1 (B ∉ ∅; booleans never null);
  s_amount_vs_out_avg_72h null (count 0). r_log_empty 1, r_account_age null, r_new_cp 1, r_returned_funds_24h 0.
- tx2 (t=90): A-log {OUT tx1→B 1000 @0, hold null — no earlier IN}. account_age = 5400000 / 60000 = 90.0;
  log_empty 0. 1h window (ts > 30) is empty → in_sum 0 → pass_through_1h null. 72h window has tx1 but its
  hold is null → hold count 0 → avg_hold_72h null. vs_avg: 1000 / 1 = 1000.0; 500 / 1000.0 = 0.5.

## s10 eviction keeps first_seen and last_in_ts (X→A 1000 @0; A→B 500 @4800; A→C 300 @4810)
- tx2 (t=4800, 72h window ts > 480): IN tx1 @0 evicted → s_in_count_72h 0, s_in_sum_72h 0,
  pass_through_72h null (in_sum 0). first_seen kept: account_age = 288000000 / 60000 = 4800.0.
  s_log_empty 0 — spec: "1 if the account has NO prior entries AT ALL"; A has tx1, merely out of window.
  Append OUT: hold_ms = 288000000 − 0 = 288000000 (last_in_ts kept outside window state).
- tx3 (t=4810, window ts > 490): only OUT tx2 → s_out_count_72h 1; avg_hold = 288000000 / (1·60000) = 4800.0;
  account_age = 288600000 / 60000 = 4810.0.

## Counts
s01 20 · s02 24 · s03 20 · s04 24 · s05 24 · s06 8 · s07 9 · s08 13 · s09 21 · s10 8 = 171.
