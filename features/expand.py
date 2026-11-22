"""Expand `features/spec.yaml` into the ordered feature name list (M02 step 1).

    uv run python -m features.expand            # write features/feature_names_v<N>.txt
    uv run python -m features.expand --check    # exit 1 if the committed file differs

Order (spec.yaml `vector_order.rule`): tx features, then `s_` account features, then `r_` account
features, then relational features. Inside an account block the templates keep their listed order
and each one expands over its windows in `windows.default` order (feature-major), and a `{fmt}`
template expands over its vocabulary in vocabulary order. The vocabulary strings go into the name
as written (`s_format_share_CREDIT_CARD_72h`), so Flink must use the same strings.

The generated file is committed: Flink copies it in M4 and the scorer reads it, so a spec change
has to show up as a diff there.
"""

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from features.spec import SPEC_PATH, Spec, Template, load_spec

FEATURES_DIR = Path(__file__).resolve().parent
ROLE_PREFIXES = ("s_", "r_")  # OUT leg (sender side) first, then IN leg (receiver side)


def _expand(template: Template, spec: Spec) -> list[str]:
    values: list[str | None] = [None]
    if template.for_each is not None:
        values = list(spec.vocabulary(template.for_each))
    windows: list[str | None] = list(template.windows) if "{w}" in template.name else [None]
    out = []
    for fmt in values:
        for w in windows:
            name = template.name
            if fmt is not None:
                name = name.replace("{fmt}", fmt)
            if w is not None:
                name = name.replace("{w}", w)
            out.append(name)
    return out


def feature_names(spec: Spec) -> list[str]:
    """All feature names in vector order. Raises ValueError on a duplicate."""
    names: list[str] = []
    for t in spec.tx_features:
        names += _expand(t, spec)
    for prefix in ROLE_PREFIXES:
        for t in spec.account_features:
            names += [prefix + n for n in _expand(t, spec)]
    for t in spec.relational_features:
        names += _expand(t, spec)  # already carry their s_/r_ prefix in the spec

    seen: set[str] = set()
    for n in names:
        if n in seen:
            raise ValueError(f"duplicate feature name {n!r}")
        seen.add(n)
    return names


def names_path(spec: Spec) -> Path:
    return FEATURES_DIR / f"feature_names_v{spec.spec_version}.txt"


def render(spec: Spec) -> str:
    """The file content: one name per line, trailing newline."""
    return "\n".join(feature_names(spec)) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="compare, do not write")
    parser.add_argument("--spec", type=Path, default=SPEC_PATH)
    parser.add_argument(
        "--out", type=Path, default=None, help="default: features/feature_names_v<N>.txt"
    )
    args = parser.parse_args(argv)

    spec = load_spec(args.spec)
    out = args.out or names_path(spec)
    text = render(spec)
    if args.check:
        current = out.read_text() if out.is_file() else None
        if current != text:
            print(f"{out} is out of date: run `uv run python -m features.expand`", file=sys.stderr)
            return 1
        return 0
    out.write_text(text)
    print(f"wrote {out} ({len(text.splitlines())} names)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
