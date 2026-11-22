"""Typed view of `features/spec.yaml`, the single source of truth for the feature semantics.

Everything that needs the windows, the vocabularies or the feature templates (the name expansion in
`features/expand.py`, the oracle in `features/reference.py`, the Spark job) goes through here, so a
spec change is read in one place. This module only parses and validates; it computes no features.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

SPEC_PATH = Path(__file__).resolve().parent / "spec.yaml"

_NO_WINDOWS = "none"  # spec.yaml spells "this template has no window" as `windows: none`


@dataclass(frozen=True)
class Template:
    """One entry of `tx_features`, `account_features` or `relational_features`.

    `windows` is already resolved: the template's own list, the default windows when the key is
    absent, or empty for `windows: none`. `for_each` is the vocabulary name (for example
    `payment_formats`) when the name carries a `{fmt}` placeholder. `role` is `OUT`/`IN` for the
    relational features and None otherwise.
    """

    name: str
    windows: tuple[str, ...]
    for_each: str | None
    role: str | None


@dataclass(frozen=True)
class Spec:
    spec_version: int
    fx_version_required: int
    default_windows: tuple[str, ...]
    window_ms: Mapping[str, int]
    state_retention_ms: int
    training_burn_in_ms: int
    currencies: tuple[str, ...]
    payment_formats: tuple[str, ...]
    dir_order: Mapping[str, int]
    tx_features: tuple[Template, ...]
    account_features: tuple[Template, ...]
    relational_features: tuple[Template, ...]
    forbidden_inputs: tuple[str, ...]

    def vocabulary(self, name: str) -> tuple[str, ...]:
        """`currencies` or `payment_formats`."""
        if name == "currencies":
            return self.currencies
        if name == "payment_formats":
            return self.payment_formats
        raise ValueError(f"unknown vocabulary {name!r}")


def load_raw(path: Path = SPEC_PATH) -> dict[str, Any]:
    """The spec file as a plain dict."""
    doc = yaml.safe_load(path.read_text())
    if not isinstance(doc, dict):
        raise ValueError(f"{path} is not a mapping")
    return doc


def load_spec(path: Path = SPEC_PATH) -> Spec:
    return parse_spec(load_raw(path))


def _template(
    entry: Mapping[str, Any], defaults: tuple[str, ...], known: Mapping[str, int]
) -> Template:
    name = str(entry["name"])
    raw_windows = entry.get("windows")
    if raw_windows is None:
        windows = defaults
    elif raw_windows == _NO_WINDOWS:
        windows = ()
    elif isinstance(raw_windows, list):
        windows = tuple(str(w) for w in raw_windows)
    else:
        raise ValueError(f"{name}: windows must be a list or 'none', got {raw_windows!r}")
    for w in windows:
        if w not in known:
            raise ValueError(f"{name}: unknown window {w!r} (known: {sorted(known)})")

    for_each = entry.get("for_each")
    if for_each is not None:
        prefix = "vocabularies."
        if not str(for_each).startswith(prefix):
            raise ValueError(f"{name}: for_each must name a vocabulary, got {for_each!r}")
        for_each = str(for_each)[len(prefix) :]

    if "{w}" in name and not windows:
        raise ValueError(f"{name}: has {{w}} but the template has no windows")
    if "{w}" not in name and len(windows) > 1:
        # several windows and nothing to tell their names apart: the expansion would repeat a name
        raise ValueError(f"{name}: {len(windows)} windows but no {{w}} in the name")
    if "{fmt}" in name and for_each is None:
        raise ValueError(f"{name}: has {{fmt}} but no for_each")
    if "{fmt}" not in name and for_each is not None:
        raise ValueError(f"{name}: for_each but no {{fmt}} in the name")
    role = entry.get("role")
    if role is not None and role not in ("OUT", "IN"):
        raise ValueError(f"{name}: role must be OUT or IN, got {role!r}")
    return Template(name=name, windows=windows, for_each=for_each, role=role)


def parse_spec(doc: Mapping[str, Any]) -> Spec:
    """Validate a spec document and return the typed view. Raises ValueError on a bad template."""
    windows = doc["windows"]
    default_windows = tuple(str(w) for w in windows["default"])
    window_ms = {str(k): int(v) for k, v in windows["durations_ms"].items()}
    for w in default_windows:
        if w not in window_ms:
            raise ValueError(f"default window {w!r} has no duration in windows.durations_ms")

    def templates(key: str, defaults: tuple[str, ...] = ()) -> tuple[Template, ...]:
        # Only account features are "expanded for every window in windows.default unless
        # 'windows' is given" (spec.yaml); tx and relational features have no window by default.
        return tuple(_template(e, defaults, window_ms) for e in doc.get(key, []))

    vocab = doc["vocabularies"]
    return Spec(
        spec_version=int(doc["spec_version"]),
        fx_version_required=int(doc["fx_version_required"]),
        default_windows=default_windows,
        window_ms=window_ms,
        state_retention_ms=int(windows["state_retention_ms"]),
        training_burn_in_ms=int(windows["training_burn_in_ms"]),
        currencies=tuple(vocab["currencies"]),
        payment_formats=tuple(vocab["payment_formats"]),
        dir_order={str(k): int(v) for k, v in doc["ordering"]["dir_order"].items()},
        tx_features=templates("tx_features"),
        account_features=templates("account_features", default_windows),
        relational_features=templates("relational_features"),
        forbidden_inputs=tuple(str(x) for x in doc["forbidden_inputs"]),
    )
