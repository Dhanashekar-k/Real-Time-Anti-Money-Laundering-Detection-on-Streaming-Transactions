# tests/fixtures/aml/

A synthetic 16-row file in the layout the manual describes for the IBM AML CSV, and a matching
patterns file. **Written by hand for tests; it is not IBM data**, and the layout is the manual's
description (checked against the real file in M01 step 1), not a copy of it.

What the rows exercise: a duplicate `Account` header; same-minute ties; a row out of file order
(`2022/08/31 23:59`); a self-transfer; an exact duplicate row (1 and 8: ambiguous pattern match);
JPY, BTC and cross-currency rows; half-even rounding cases (`0.125`, `99.995`); a laundering row that
no pattern block contains (row 12).
