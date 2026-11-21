# 03 — AWS on your account: rules, guardrails, cost model, teardown

**Updated for your account:** the project-based "Sign up for AWS (new)" experience, Free plan, no payment method. The phase-by-phase view of which service is used when is in `07_phases_and_cloud_plan.md`. Facts there carry sources.

**Most useful fact first:** almost all of this project runs free on your laptop (Phases 0–4). AWS is used for guardrails in Phase 0 and for one batch run plus two cluster sessions in Phase 5, at about $25–30 of credits [Guessing]. On the Free plan you can't overspend: usage stops when credits run out. The risk is no longer a surprise bill. It's **credits or the 6-month window running out before Phase 5 is done.**

---

## Step 0 — Know your account's limits (do it today)

| Rule | What to do |
|---|---|
| Credits: $100 at sign-up, plus up to $100 for completing starter activities | Complete the activities now; the extra credit is free buffer for Phase 5 |
| Free plan lasts until credits run out or the account is 6 months old, whichever comes first; then you must upgrade (add a payment method) | Note "days remaining" from the Projects page in decision D16. Schedule Phase 5 inside the window |
| Services: EKS, EC2, Glue, Athena, S3, ECR, Bedrock, Budgets are allowed; **EMR is Paid-plan only; EMR Serverless isn't on the list** | Spark at scale = **AWS Glue**, not EMR |
| EC2 instance types are limited to t3.micro/small, t4g.micro/small, **c7i-flex.large, m7i-flex.large** | EKS nodes use these types; images must be **amd64** |
| IAM Identity Center unsupported | Use the CLI sign-in your account provides; no Identity Center setup |
| Paid plan (if you ever upgrade): spend limit per project from **$20/month** | If Phase 5 needs it, upgrade with a $30 limit and record decision D22 |
| [Unclear] Spot instances and the EC2 vCPU quota on your plan | Check in Phase 0. The plan needs 24 vCPUs (6-node fallback: 12 vCPUs) |

Commands to run once (read-only):
```bash
aws sts get-caller-identity
aws ec2 describe-instance-types --filters Name=free-tier-eligible,Values=true \
  --query "InstanceTypes[*].[InstanceType]" --output text | sort
```

---

## Step 1 — Guardrails (30 min, before any Terraform)

1. **Sign-in:** enable MFA on the identity you signed up with (Google, GitHub or email). Use the CLI sign-in flow your new account offers instead of creating long-lived access keys. [Likely] The new experience supports keyless local authentication; prefer it.
2. **Profile:** create a named CLI profile `stream-aml` pointing at the project's account, region `us-east-2` (changed from us-east-1, see D35). Export `AWS_PROFILE=stream-aml` only while working on Phase 5.
3. **Budgets:** an actual-cost budget at $5 (early warning), $20 and $50, with email alerts. Budgets are allowed on the Free plan. The budgets warn you; the Free plan's credit limit is the hard stop.
4. **Tagging:** Terraform `default_tags = { project = "stream-aml", owner = "shekar", ttl = "session" }`. Activate the `project` cost-allocation tag if your console offers it.
5. **Region:** `us-east-2` for everything (changed from us-east-1, see D35).

---

## Step 2 — Cost-safe infrastructure rules (enforced by the `cost-guard` agent)

| Trap | Rule in this project |
|---|---|
| Instance types not allowed on your plan | Node groups use **only** m7i-flex.large / c7i-flex.large (helper box: c7i-flex.large). `cost-guard` flags anything else |
| NAT gateway: hourly plus per-GB | No NAT. Public subnets with security groups; S3 gateway VPC endpoint (free) |
| `Service type: LoadBalancer` creates billable ELBs that survive sloppy teardowns | ClusterIP only; reach UIs with `kubectl port-forward` |
| PVC volumes outliving the cluster | Strimzi `deleteClaim: true`; uninstall Helm releases before `terraform destroy`; teardown check lists `available` volumes |
| EKS extended support costs more per hour | Create the cluster on the newest version in standard support |
| EKS control-plane logging | Off |
| Glue jobs or sessions left running | Jobs have a timeout and 0 retries; no interactive sessions or notebooks left open; no crawlers (Iceberg writes the catalog directly) |
| Orphaned Terraform state | S3 backend with `use_lockfile = true`, set up in Phase 0 |
| Forgetting | `make cloud-down` → `make teardown-check` → Cost Explorer next day |

---

## Step 3 — Cost model (credits)

### 3a. Glue: Spark over 180M rows (T1)
Glue bills **$0.44 per DPU-hour**, per second with a 1-minute minimum; one DPU is 4 vCPUs and 16 GB.[^glue]
- Rehearsal on HI-Small (end of Phase 1): about 6 DPU × 0.25 h ≈ **$0.66**
- HI-Large `csv_to_iceberg` + `features`: about 10 DPU × 2 h ≈ **$9** [Guessing]; read the real number from each run's DPU-seconds
- Glue 6.0 is 30% cheaper but runs Spark 4.1. Stay on Glue 5.1 to match local Spark 3.5 unless cost forces a change

### 3b. EKS sessions (T4, T5, fault at scale)
[Guessing] Check current on-demand prices for the two flex types on the day you run.

| Item | Size | ≈ $/h |
|---|---|---|
| EKS control plane | 1 cluster | 0.10 |
| 9 × m7i-flex.large (system 2, kafka 3, flink 4) | on-demand | ~0.85 |
| 3 × c7i-flex.large (apps) | on-demand | ~0.25 |
| EBS gp3 + public IPv4 | ~400 GB, ~12 IPs | ~0.10 |
| **Total** | | **≈ $1.3/h** |

Session A (≈ 2.5 h) ≈ $3–4. Session B (≈ 5 h) ≈ $7. Kaggle helper box and S3/Athena/ECR: under $2.
**Phase 5 total ≈ $20–25**, leaving most of your $100–200 in credits unused.

### 3c. Claude API (your Anthropic credits, not AWS)
Prices per million tokens:[^price]

| Model | Input | Output | Batch input / output |
|---|---|---|---|
| `claude-haiku-5-5` (prompts ≤ 100k tokens) | $0.10 | $0.50 | $0.05 / $0.25 |
| `claude-sonnet-5-5` | $2 | $10 | $1 / $5 |
| `claude-opus-5-5` | $4 | $20 | $2 / $10 |

At about 6k input and 700 output tokens per case: Haiku ≈ $0.001, Sonnet ≈ $0.019, Opus ≈ $0.038. With the v2 cascade (Haiku first, Sonnet only when validation fails), the blended cost stays close to Haiku's. Expect about **$20 total** across development, evals and re-runs.

Never set `ANTHROPIC_API_KEY` in the shell where Claude Code runs; the app uses `AML_ANTHROPIC_API_KEY`.

---

## Step 4 — Session runbook (print this)

**Before:**
- [ ] `aws sts get-caller-identity --profile stream-aml` shows the project account
- [ ] Credits balance and days remaining checked (enough for the session plus a buffer)
- [ ] `make teardown-check` comes back CLEAN
- [ ] Three measurements this session must produce, written at the top of `results/M09/session_<n>.md`

**During:**
- [ ] `make cloud-up` (you approve). Read the plan's resource count; instance types must be m7i-flex.large / c7i-flex.large only
- [ ] Timer on; hard stop at plan + 1 h
- [ ] Save screenshots and result JSON as you go

**After (same sitting):**
- [ ] `make cloud-down`
- [ ] `make teardown-check` comes back CLEAN: no EKS cluster, instances, NAT gateways, load balancers, unattached volumes or Elastic IPs, and no Glue job runs or sessions in progress
- [ ] Next day: Cost Explorer filtered on `project=stream-aml` → screenshot

---

## Step 5 — HI-Large into S3 without uploading 20 GB from home
1. Temporary EC2 instance: c7i-flex.large, Amazon Linux 2023, 100 GB gp3, with a role that can write `s3://<bucket>/landing/`.
2. `pip install kaggle` → token in `~/.kaggle/kaggle.json` (`chmod 600`) → download HI-Large transactions and patterns only → unzip → `aws s3 cp` to `landing/hi_large/`.
3. Terminate the instance. Delete `landing/` after the Glue conversion's counts match (or let the 7-day lifecycle rule do it).

[^glue]: https://aws.amazon.com/glue/pricing ; https://aws.amazon.com/tw/glue/pricing/
[^price]: https://platform.claude.com/docs/en/about-claude/pricing
