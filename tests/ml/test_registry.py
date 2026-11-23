"""ml/registry.py: one champion per dataset in the shared `aml-xgb` registry (D50).

HI-Medium's training took the single `champion` alias away from HI-Small's model, which broke the
HI-Small evaluation and made `eval.pattern_gap` load the wrong model without saying so. Each
dataset now has its own alias, `champion_<ds>`; the bare `champion` alias follows HI-Small only
(the dataset whose thresholds the scorer reads, `config/thresholds.yaml`).
"""

import re
from pathlib import Path

import pytest

from ml import registry

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]


class FakeClient:
    """The one MLflow call the registry helper makes."""

    def __init__(self, aliases: dict[str, str]) -> None:
        self.aliases = aliases
        self.asked: list[tuple[str, str]] = []

    def get_model_version_by_alias(self, name: str, alias: str):
        self.asked.append((name, alias))
        if alias not in self.aliases:
            raise LookupError(f"no alias {alias}")  # MLflow raises its own exception type
        return type("V", (), {"version": self.aliases[alias]})()


def test_each_dataset_has_its_own_alias() -> None:
    assert registry.champion_alias("hi_small") == "champion_hi_small"
    assert registry.champion_alias("hi_medium") == "champion_hi_medium"
    assert registry.MODEL_NAME == "aml-xgb"


def test_a_dataset_resolves_to_its_own_version_not_another_datasets() -> None:
    client = FakeClient({"champion_hi_small": "3", "champion_hi_medium": "5", "champion": "3"})
    assert registry.resolve_version(client, "hi_small") == "3"
    assert registry.resolve_version(client, "hi_medium") == "5"
    assert ("aml-xgb", "champion_hi_medium") in client.asked  # asked for its own alias


def test_the_shared_alias_is_never_what_a_dataset_resolves_through() -> None:
    client = FakeClient({"champion": "5"})  # only the old shared alias exists
    with pytest.raises(RuntimeError, match="champion_hi_small"):
        registry.resolve_version(client, "hi_small")


def test_the_model_uri_names_the_dataset_alias() -> None:
    assert registry.model_uri("hi_medium") == "models:/aml-xgb@champion_hi_medium"


def test_a_version_that_differs_from_the_thresholds_file_is_refused() -> None:
    registry.check_matches("hi_small", version="3", expected="3", source="thresholds_hi_small.yaml")
    with pytest.raises(RuntimeError, match=r"version 5.*version 3|3.*5"):
        registry.check_matches(
            "hi_small", version="5", expected="3", source="thresholds_hi_small.yaml"
        )


def test_aliases_to_set_after_training() -> None:
    assert registry.aliases_for("hi_small") == ["champion_hi_small", "champion"]
    assert registry.aliases_for("hi_medium") == ["champion_hi_medium"]


def test_no_code_resolves_the_shared_alias_directly() -> None:
    """eval/ and ml/ go through ml.registry; a literal `@champion` or alias "champion" is a bug."""
    offenders = []
    for folder in ("eval", "ml"):
        for path in (ROOT / folder).glob("*.py"):
            if path.name == "registry.py":
                continue
            text = path.read_text()
            if re.search(r"@champion[\"'\s]", text) or re.search(
                r"by_alias\([^)]*[\"']champion[\"']", text
            ):
                offenders.append(str(path.relative_to(ROOT)))
    assert offenders == []
