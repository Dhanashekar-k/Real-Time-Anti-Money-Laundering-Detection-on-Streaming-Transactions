"""Which registered model is a dataset's champion (D50).

All models live in one MLflow registered model, `aml-xgb`. Each dataset has its own alias,
`champion_<ds>`, so training one dataset cannot take the champion away from another. The bare
`champion` alias is kept for the scorer (M5) and follows HI-Small only, the dataset whose
thresholds are in `config/thresholds.yaml`. Evaluation and analysis scripts must resolve a model
through this module, never through the bare alias (`tests/ml/test_registry.py` scans for it).
"""

from typing import Any

MODEL_NAME = "aml-xgb"
SCORER_ALIAS = "champion"  # follows hi_small; for the scorer, not for evaluation
SCORER_DATASET = "hi_small"


def champion_alias(ds: str) -> str:
    return f"champion_{ds}"


def aliases_for(ds: str) -> list[str]:
    """Aliases to point at a dataset's newly registered champion."""
    out = [champion_alias(ds)]
    if ds == SCORER_DATASET:
        out.append(SCORER_ALIAS)
    return out


def model_uri(ds: str) -> str:
    return f"models:/{MODEL_NAME}@{champion_alias(ds)}"


def resolve_version(client: Any, ds: str) -> str:
    """The registered version of the dataset's champion; fails loudly if the alias is missing."""
    alias = champion_alias(ds)
    try:
        return str(client.get_model_version_by_alias(MODEL_NAME, alias).version)
    except Exception as exc:  # MLflow raises its own exception type
        raise RuntimeError(
            f"{MODEL_NAME} has no alias {alias}: train {ds} first, or re-point the alias"
        ) from exc


def check_matches(ds: str, version: str, expected: str, source: str) -> None:
    """The champion alias must name the version that `source` (a results/config file) recorded."""
    if str(version) != str(expected):
        raise RuntimeError(
            f"{champion_alias(ds)} points at version {version}, but {source} was written for "
            f"version {expected}"
        )
