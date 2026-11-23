# Image for the M3 connectors; later app services (scorer, alerts) are added with their own groups.
# Built for this machine's architecture locally. Cloud images must be linux/amd64 (CLAUDE.md rule 10,
# M9): `docker buildx build --platform linux/amd64`; both base tags below publish amd64 and arm64.
# Python 3.11.15 is the version in uv.lock's venv. Dependencies come from the `connector` group of
# pyproject.toml, pinned by uv.lock.
FROM python:3.11.15-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.10.8 /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1
WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --only-group connector

# Only what a connector imports: the money code and its tables, the bank YAMLs, the Avro contract.
COPY features/ features/
COPY connectors/ connectors/
COPY contracts/canonical_tx.avsc contracts/canonical_tx.avsc
COPY stream/__init__.py stream/settings.py stream/
COPY stream/connectors/ stream/connectors/

# Files keep their host mode (spec.yaml is 0600), which the non-root user below could not read.
RUN chmod -R a+rX /app/features /app/connectors /app/contracts /app/stream
ENV PATH="/app/.venv/bin:$PATH"
RUN useradd --system --uid 10001 aml
USER aml
