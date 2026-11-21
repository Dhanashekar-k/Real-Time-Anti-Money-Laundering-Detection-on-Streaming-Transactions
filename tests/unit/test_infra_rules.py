"""CLAUDE.md rule 5 (cloud safety), checked statically over everything under infra/.

These are text checks, not a Terraform parser. They catch the usual ways to break a cost or
safety rule, so a future change fails in CI instead of in a bill; they do not prove a config is
safe. `cost-guard` still reviews every infra/ change. Each pattern has a positive control below
(a snippet it must flag) so none of them can silently stop matching.
"""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]


def _not_cached(p: Path) -> bool:
    return ".terraform" not in p.parts


TF_FILES = sorted(p for p in (ROOT / "infra" / "terraform").rglob("*.tf") if _not_cached(p))
INFRA_FILES = sorted(
    p
    for ext in ("*.yaml", "*.yml", "*.tpl", "*.tf")
    for p in (ROOT / "infra").rglob(ext)
    if _not_cached(p)
)
# Where an apply or a Helm install could be scripted. -auto-approve must appear in none of them.
SCRIPTED = [ROOT / "Makefile", *sorted((ROOT / ".github").rglob("*.yml")), *TF_FILES]
# Where a billable load balancer or NAT gateway could be introduced.
COST_SCANNED = [*INFRA_FILES, ROOT / "Makefile"]

NAT_PATTERNS = [
    r'resource\s+"aws_nat_gateway"',
    r"\benable_nat_gateway\s*=\s*true\b",  # terraform-aws-modules/vpc
    r"\bsingle_nat_gateway\s*=\s*true\b",
    r"\bone_nat_gateway_per_az\s*=\s*true\b",
]
LB_PATTERNS = [
    r"""\btype\s*[:=]\s*["']?LoadBalancer\b""",  # YAML `type: LoadBalancer`, HCL `type = "..."`
    r"service\.type=LoadBalancer",  # helm --set
    r'resource\s+"aws_(?:lb|alb|elb)"',
]


def _violations(text: str, patterns: list[str]) -> list[str]:
    return [p for p in patterns if re.search(p, text, re.IGNORECASE)]


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


def _code_lines(path: Path) -> list[str]:
    """Lines that are not whole-line comments (a comment may explain the rule it obeys)."""
    return [ln for ln in path.read_text().splitlines() if not ln.lstrip().startswith(("#", "//"))]


def _strimzi_violation(text: str) -> bool:
    """A Strimzi Kafka resource must set deleteClaim: true (rule 5: no PVCs outliving teardown)."""
    if not re.search(r"^\s*kind:\s*Kafka\s*$", text, re.MULTILINE):
        return False
    return not re.search(r"deleteClaim:\s*true\b", text) or bool(
        re.search(r"deleteClaim:\s*false\b", text)
    )


def _glue_violations(text: str) -> list[str]:
    """Each aws_glue_job needs a timeout and zero retries (rule 5)."""
    out = []
    for body in _blocks(text, re.compile(r'resource\s+"aws_glue_job"\s+"[^"]+"\s*\{')):
        if not re.search(r"^\s*timeout\s*=", body, re.MULTILINE):
            out.append("no timeout")
        if not re.search(r"^\s*max_retries\s*=\s*0\s*$", body, re.MULTILINE):
            out.append("max_retries is not 0")
    return out


# ------------------------------------------------------------------ the checks on the repo
def test_there_is_at_least_one_aws_provider() -> None:
    # Guards the other tests: with no providers found they would pass vacuously.
    assert _all_blocks(r'provider\s+"aws"'), "no provider block found under infra/terraform"


def test_every_aws_provider_sets_default_tags_with_project() -> None:
    for path, body in _all_blocks(r'provider\s+"aws"'):
        assert "default_tags" in body, f"{path}: provider has no default_tags"
        assert re.search(r"\bproject\s*=", body), f"{path}: default_tags has no `project` tag"


def test_no_nat_gateways() -> None:
    offenders = {str(f): _violations(f.read_text(), NAT_PATTERNS) for f in COST_SCANNED}
    offenders = {f: v for f, v in offenders.items() if v}
    assert not offenders, f"NAT gateways are billed hourly and banned: {offenders}"


def test_no_load_balancers() -> None:
    offenders = {str(f): _violations(f.read_text(), LB_PATTERNS) for f in COST_SCANNED}
    offenders = {f: v for f, v in offenders.items() if v}
    assert not offenders, f"load balancers create billable ELBs and are banned: {offenders}"


def test_s3_backends_use_lockfile_not_dynamodb() -> None:
    for path, body in _all_blocks(r'backend\s+"s3"'):
        assert re.search(r"\buse_lockfile\s*=\s*true\b", body), (
            f"{path}: backend lacks use_lockfile"
        )
        assert "dynamodb_table" not in body, f"{path}: DynamoDB locking is deprecated, use lockfile"


def test_every_root_except_bootstrap_has_an_s3_backend() -> None:
    """A new root with no backend would silently keep its state in a local file."""
    roots: dict[Path, str] = {}
    for f in TF_FILES:
        roots[f.parent] = roots.get(f.parent, "") + f.read_text()
    for directory, text in roots.items():
        if (
            not re.search(r"^\s*terraform\s*\{", text, re.MULTILINE)
            or directory.name == "bootstrap"
        ):
            continue  # not a root, or the root that creates the bucket (local state by design)
        assert re.search(r'backend\s+"s3"', text), f'{directory}: root has no backend "s3" block'


def test_strimzi_kafka_deletes_its_volumes() -> None:
    offenders = [str(p) for p in INFRA_FILES if _strimzi_violation(p.read_text())]
    assert not offenders, (
        f"Strimzi Kafka needs `deleteClaim: true` (PVCs outlive teardown): {offenders}"
    )


def test_glue_jobs_have_timeout_and_zero_retries() -> None:
    offenders = {str(f): v for f in TF_FILES if (v := _glue_violations(f.read_text()))}
    assert not offenders, f"Glue jobs need a timeout and max_retries = 0: {offenders}"


def test_no_auto_approve_in_scripted_paths() -> None:
    offenders = [
        p for p in SCRIPTED if p.is_file() and any("-auto-approve" in ln for ln in _code_lines(p))
    ]
    assert not offenders, f"-auto-approve is forbidden (CLAUDE.md rule 5): {offenders}"


def test_cloud_bootstrap_guards_are_present() -> None:
    """Guards against accidents (a pipe, TF_CLI_ARGS); not a security boundary, see the Makefile."""
    makefile = (ROOT / "Makefile").read_text()
    match = re.search(r"^cloud-bootstrap:.*?(?=\n\n|\Z)", makefile, re.DOTALL | re.MULTILINE)
    assert match, "cloud-bootstrap target not found"
    recipe = match.group(0)
    assert "env -u TF_CLI_ARGS " in recipe and "-u TF_CLI_ARGS_apply" in recipe
    assert "[ -t 0 ]" in recipe
    assert "-auto-approve" not in recipe


# ----------------------------------------- positive controls: each pattern must be able to fail
@pytest.mark.parametrize(
    "snippet",
    [
        'resource "aws_nat_gateway" "x" {}',
        "enable_nat_gateway = true",
        "single_nat_gateway    = true",
        "one_nat_gateway_per_az = true",
    ],
)
def test_nat_patterns_catch(snippet: str) -> None:
    assert _violations(snippet, NAT_PATTERNS)


@pytest.mark.parametrize(
    "snippet",
    [
        "  type: LoadBalancer",
        '  type: "LoadBalancer"',
        "type = 'LoadBalancer'",
        "helm upgrade x --set service.type=LoadBalancer",
        'resource "aws_lb" "x" {}',
        'resource "aws_elb" "x" {}',
    ],
)
def test_load_balancer_patterns_catch(snippet: str) -> None:
    assert _violations(snippet, LB_PATTERNS)


@pytest.mark.parametrize("snippet", ["  type: ClusterIP", "enable_nat_gateway = false"])
def test_cost_patterns_do_not_flag_safe_config(snippet: str) -> None:
    assert not _violations(snippet, NAT_PATTERNS + LB_PATTERNS)


def test_strimzi_check_catches_missing_and_false_delete_claim() -> None:
    assert _strimzi_violation("kind: Kafka\nspec:\n  storage:\n    type: persistent-claim\n")
    assert _strimzi_violation("kind: Kafka\n    deleteClaim: false\n")
    assert not _strimzi_violation("kind: Kafka\n    deleteClaim: true\n")
    assert not _strimzi_violation("kind: ConfigMap\n")


def test_glue_check_catches_missing_timeout_and_retries() -> None:
    bad = 'resource "aws_glue_job" "j" {\n  name = "j"\n  max_retries = 1\n}\n'
    good = 'resource "aws_glue_job" "j" {\n  timeout = 120\n  max_retries = 0\n}\n'
    assert _glue_violations(bad) == ["no timeout", "max_retries is not 0"]
    assert not _glue_violations(good)


def test_auto_approve_check_catches_a_real_use(tmp_path: Path) -> None:
    """A code line with the flag is flagged; a comment about it is not."""
    f = tmp_path / "Makefile"
    f.write_text("# never use -auto-approve\napply:\n\tterraform apply -auto-approve\n")
    assert [ln for ln in _code_lines(f) if "-auto-approve" in ln] == [
        "\tterraform apply -auto-approve"
    ]
