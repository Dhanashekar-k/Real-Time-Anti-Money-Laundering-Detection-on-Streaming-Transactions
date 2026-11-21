# The official MLflow image ships without a Postgres driver (checked on v3.18.0: no psycopg2, no
# curl), so the backend store on Postgres needs one extra package. Everything else is the
# official image. Keep the MLflow tag in step with the `mlflow` pin in pyproject.toml / uv.lock.
FROM ghcr.io/mlflow/mlflow:v3.18.0

RUN pip install --no-cache-dir psycopg2-binary==2.9.13
