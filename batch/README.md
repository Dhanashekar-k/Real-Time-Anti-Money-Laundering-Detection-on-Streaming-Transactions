# batch/

Spark and Iceberg batch side (`batch/spark/`): CSV to Iceberg, pattern tables, feature computation and the training-set build. It implements `features/spec.yaml` with the same semantics as the Flink job. Parity is checked against `tests/golden/`.

Runs locally on Spark 3.5.x and in the cloud on AWS Glue 5.1 (Spark 3.5.6, Python 3.11) with the same job code. Added from M1.
