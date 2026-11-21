"""CLAUDE.md rule 5 (cloud safety), checked statically over everything under infra/.

These are text checks, not a Terraform parser: they exist so a future change that breaks a cost or
safety rule fails in CI instead of in a bill. `cost-guard` still reviews every infra/ change.
"""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
TF_FILES = sorted((ROOT / "infra" / "terraform").rglob("*.tf"))
INFRA_YAML = sorted(
    p for ext in ("*.yaml", "*.yml", "*.tpl") for p in (ROOT / "infra").rglob(ext)
)  # compose, helm values, k8s manifests
# Where an apply could be scripted: -auto-approve must appear in none of them (rule 5).
SCRIPTED = [ROOT / "Makefile", *sorted((ROOT / ".github").rglob("*.yml")), *TF_FILES]


def _blocks(text: str, header: re.Pattern[str]) -> list[str]:
    """Bodies of every `header { ... }` block, found by brace matching."""
    out = []
    for m in header.finditer(text):
        depth, i = 1, m.end()
        while i < len(text) and depth:
            depth += {"{": 1, "}": -1}.get(text[i], 0)
            i += 1
        out.append(text[m.end() : i - 1])
    return out


def _all_blocks(header: str) -> list[tuple[Path, str]]:
    pattern = re.compile(header + r"\s*\{")
    return [(f, b) for f in TF_FILES for b in _blocks(f.read_text(), pattern)]


def test_there_is_at_least_one_aws_provider() -> None:
    # Guards the other tests: with no providers found they would pass vacuously.
    assert _all_blocks(r'provider\s+"aws"'), "no provider block found under infra/terraform"


def test_every_aws_provider_sets_default_tags_with_project() -> None:
    for path, body in _all_blocks(r'provider\s+"aws"'):
        assert "default_tags" in body, f"{path}: provider has no default_tags"
        assert re.search(r"\bproject\s*=", body), f"{path}: default_tags has no `project` tag"


def test_no_nat_gateways() -> None:
    offenders = [f for f in TF_FILES if re.search(r'resource\s+"aws_nat_gateway"', f.read_text())]
    assert not offenders, f"NAT gateways are billed hourly and banned: {offenders}"


def test_s3_backends_use_lockfile_not_dynamodb() -> None:
    for path, body in _all_blocks(r'backend\s+"s3"'):
        assert re.search(r"\buse_lockfile\s*=\s*true\b", body), (
            f"{path}: backend lacks use_lockfile"
        )
        assert "dynamodb_table" not in body, f"{path}: DynamoDB locking is deprecated, use lockfile"


def _code_lines(path: Path) -> list[str]:
    """Lines that are not whole-line comments (a comment may explain the rule it obeys)."""
    return [ln for ln in path.read_text().splitlines() if not ln.lstrip().startswith(("#", "//"))]


def test_no_auto_approve_in_scripted_paths() -> None:
    offenders = [
        p for p in SCRIPTED if p.is_file() and any("-auto-approve" in ln for ln in _code_lines(p))
    ]
    assert not offenders, f"-auto-approve is forbidden (CLAUDE.md rule 5): {offenders}"


def test_auto_approve_check_catches_a_real_use(tmp_path: Path) -> None:
    """Negative control: a code line with the flag is flagged, a comment about it is not."""
    f = tmp_path / "Makefile"
    f.write_text("# never use -auto-approve\napply:\n\tterraform apply -auto-approve\n")
    assert [ln for ln in _code_lines(f) if "-auto-approve" in ln] == [
        "\tterraform apply -auto-approve"
    ]


def test_cloud_bootstrap_cannot_be_answered_by_env_or_pipe() -> None:
    """The apply must wait for a human: scrub TF_CLI_ARGS* and require a terminal on stdin."""
    makefile = (ROOT / "Makefile").read_text()
    match = re.search(r"^cloud-bootstrap:.*?(?=\n\n|\Z)", makefile, re.DOTALL | re.MULTILINE)
    assert match, "cloud-bootstrap target not found"
    recipe = match.group(0)
    assert "env -u TF_CLI_ARGS " in recipe and "-u TF_CLI_ARGS_apply" in recipe
    assert "[ -t 0 ]" in recipe
    assert "-auto-approve" not in recipe


def test_no_load_balancer_services() -> None:
    offenders = [
        p for p in INFRA_YAML if re.search(r"type:\s*LoadBalancer", p.read_text(), re.IGNORECASE)
    ]
    assert not offenders, f"Service type LoadBalancer creates billable ELBs: {offenders}"
