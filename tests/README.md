# tests/

| Dir | What | Marker |
|---|---|---|
| `unit/` | fast tests, no services | `unit` (default) |
| `golden/` | feature vectors that Spark and Flink must both reproduce | `unit` |
| `leakage/` | label/split/pattern never reach topics, features, evidence or prompts (must pass before any milestone closes) | `unit` |
| `parity/` | Spark vs Flink feature parity | `integration` |
| `integration/` | need `make up` | `integration` |

`make test` runs everything except `integration`; `make itest` runs the integration tests.
