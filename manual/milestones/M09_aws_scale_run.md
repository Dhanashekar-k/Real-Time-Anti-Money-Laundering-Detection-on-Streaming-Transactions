# M09 — AWS: Glue Spark on HI-Large, EKS scale replay, tear down (Phase 5b/5c)

**Updated for your account:** the new project-based sign-up, Free plan, no payment method. See `07_phases_and_cloud_plan.md` for the facts and sources. Two changes from the first version:
1. Spark runs on **AWS Glue 5.1**, not EMR Serverless. EMR is Paid-plan only and EMR Serverless needs advanced features.
2. EKS nodes use only **free-tier-eligible instance types** (m7i-flex.large, c7i-flex.large), not larger Graviton or Spot nodes. Rebuild images for **amd64**.

**Timebox:** 14 h fast / 18 h full · **Trigger:** cluster unhealthy after 2 h in Session A → destroy, debug locally (kind/k3d), retry another day. Never debug for hours on a billing cluster.
**Role signal:** Cloud (Terraform, EKS, S3, Glue, Athena, IAM), big data at real volume, cost discipline with receipts.

## Preconditions
- [ ] Phase 0 AWS steps done: credits recorded, days remaining recorded (Phase 5 must finish inside the 6-month window), allowed instance types confirmed with `aws ec2 describe-instance-types --filters Name=free-tier-eligible,Values=true`
- [ ] vCPU quota known. The planned cluster needs 24 vCPUs. If the quota is lower, use the 6-node fallback (§B1).
- [ ] Local pipeline passes M8
- [ ] `make teardown-check` comes back CLEAN
- [ ] (Recommended) Glue rehearsal on HI-Small done at the end of Phase 1

## Outputs
| Output | Where |
|---|---|
| Terraform | `infra/terraform/envs/scale/` (vpc.tf, eks.tf, nodegroups.tf, s3.tf, ecr.tf, iam.tf, glue.tf, outputs.tf) |
| Glue job scripts + params | `batch/glue/` (thin wrappers that import the same `batch/spark/*` code) |
| Helm values / manifests | `infra/helm/{strimzi,kafka,schema-registry,flink-operator,flink-job,apps,postgres,monitoring}/` |
| Images (linux/amd64) | ECR: `replayer`, `connector`, `flink-features`, `scorer`, `alerts` |
| T1 | `results/M09/glue_csv_to_iceberg.json`, `results/M09/glue_features.json` (runtime, DPU-hours, $) |
| T4/T5 | `results/M09/scale_run.json` (rate ladder, sustained rate, p50/p99, bottleneck, fix) |
| Fault at scale | `results/M09/chaos_scale.json` |
| T11 | `results/M09/cost.json` + Cost Explorer screenshot (dated) |

## Part A — Spark on HI-Large with AWS Glue

1. **[CC] Terraform** (`s3.tf`, `glue.tf`, `iam.tf`):
   - S3 bucket `stream-aml-<acct>-use1`: block public access, SSE-S3, lifecycle (`landing/` 7 days, `checkpoints/` 3 days)
   - Glue database `aml_hi_large`
   - Glue job role: read/write the bucket, Glue catalog access, CloudWatch Logs write
   - Glue jobs `aml-csv-to-iceberg` and `aml-features`, both Glue version 5.1 (Spark 3.5.6, Python 3.11), worker type G.1X, **10 workers to start** (raise to 20 only if runtime is painful), timeout 120 min, max retries 0, job bookmarks off
2. **[YOU] Land the data:**
   - Launch a temporary EC2 instance: c7i-flex.large, Amazon Linux 2023, 100 GB gp3, with an instance role that can write to `landing/`.
   - On it: `pip install kaggle`, add your token, then `kaggle datasets download -d ealtman2019/ibm-transactions-for-anti-money-laundering-aml -f HI-Large_Trans.csv` and `-f HI-Large_Patterns.txt`.
   - Unzip, `aws s3 cp` to `landing/hi_large/`, then **terminate the instance**.
3. **[CC] Job code:** reuse `batch/spark/csv_to_iceberg.py` and `features.py` unchanged. Glue wrappers only build the SparkSession with the Glue catalog. Parameters ([Likely]; check the Glue 5.1 docs):
   - `--datalake-formats iceberg`
   - `--conf` with the `spark.sql.extensions` and `spark.sql.catalog.lake` settings (GlueCatalog, warehouse `s3://…/warehouse`, `S3FileIO`)
   - `--additional-python-modules pyyaml` (or a `requirements.txt`, which Glue 5.x supports)
   - ship `features/` as a zip with `--extra-py-files`
4. **[CC] `make cloud-glue-run JOB=csv_to_iceberg DS=hi_large`, then `JOB=features`** (user-approved). Afterwards, read the run's DPU-seconds from `aws glue get-job-run` and compute $ = DPU-hours × $0.44. Write `glue_*.json`.
5. **[YOU] Validate with Athena:** `SELECT count(*), sum(is_laundering) FROM aml_hi_large.raw_transactions` should return about 180M rows; record the laundering count. Spot-check 1,000 random `features_tx` rows against the oracle run locally on those accounts' legs.
6. Delete `landing/` once the counts match.

**If a Glue run is refused** (worker limits on the Free plan, [Unclear]): drop to 5 workers and accept a longer runtime. If that also fails, upgrade to Paid with a $30 spend limit and record decision D22.

## Part B — EKS scale replay

### B1. Terraform `envs/scale` (CC writes, `cost-guard` reviews, YOU approve)
- **VPC:** 2 AZs, public subnets only, no NAT, S3 gateway endpoint
- **EKS:** newest version in standard support; addons vpc-cni, coredns, kube-proxy, `aws-ebs-csi-driver`, `eks-pod-identity-agent`; control-plane logging off
- **Managed node groups, allowed types only, on-demand** (try `capacity_type = SPOT` for flink and apps once; if refused, stay on-demand):

| Group | Count × type | vCPU / RAM each | Runs |
|---|---|---|---|
| system | 2 × m7i-flex.large | 2 / 8 GiB | operators, cert-manager, Postgres, schema registry, kube-prometheus-stack |
| kafka | 3 × m7i-flex.large (taint `dedicated=kafka`) | 2 / 8 GiB | Strimzi brokers (KRaft, dual-role), heap about 3 GB |
| flink | 4 × m7i-flex.large (taint `dedicated=flink`) | 2 / 8 GiB | 1 TaskManager per node, 2 slots, about 6 GB process memory |
| apps | 3 × c7i-flex.large | 2 / 4 GiB | replayer Jobs, connectors, scorer, alerts |

  Total: 12 nodes, 24 vCPU.
  **6-node fallback** (low quota): system 1, kafka 1 (replication factor 1, stated as a limitation), flink 2, apps 2.
- **IAM via Pod Identity:** Flink → `checkpoints/`; replayer → `warehouse/` read + Glue read; scorer → `models/` read
- **ECR** with `force_delete = true`; `default_tags` everywhere

### B2. Platform (CC; all Services ClusterIP)
1. **Strimzi** (1.3.x supports Kafka 4.2.2): KRaft `KafkaNodePool` of 3 dual-role nodes, gp3 PVC 100 Gi each, `deleteClaim: true`. Config: RF=3, `min.insync.replicas=2`, zstd, retention tuned for the replay size. Hot topics get **12 partitions** (24 vCPUs can't use more).
2. **Schema registry:** one Deployment (record which image you used).
3. **cert-manager + Flink Kubernetes Operator:** a `FlinkDeployment` in application mode, parallelism 8, checkpoints to `s3://…/checkpoints/<run>`, HashMap state backend.
4. **Postgres** StatefulSet + an init Job that applies the schema, including the v2 RLS policies.
5. **Apps:** connectors (one Deployment per bank, replicas up to the partition count), scorer (start with 3 replicas), alerts (2). Replayers run as Kubernetes Jobs, one per bank. The agent is off except a 100-case sample at the end.
6. **kube-prometheus-stack** with your dashboards; reach it with `kubectl port-forward`.

### B3. Session A: bring-up and smoke test (≈ 2.5 h, ≈ $3–4)
`make cloud-up` → `make cloud-images` (buildx `--platform linux/amd64`) → `make cloud-deploy` → replay **hi_small** at `--speedup 20000` → alert count matches the local run for the same model and thresholds → screenshots → `make cloud-down` → `make teardown-check`.

### B4. Session B: scale (≈ 5 h, ≈ $7)
1. Bring up as in Session A. Copy the champion model artifact to `s3://…/models/`.
2. **Rate ladder:** 5k → 10k → 20k → 30k → 40k tx/s, about 10 minutes per step. At each step record the achieved ingest rate, consumer-lag trend (flat means sustainable), Flink backpressure, checkpoint duration, and p50/p99 latency. [Guessing] On 2-vCPU nodes expect a ceiling of 15–30k tx/s.
3. **Bottleneck:** find the stage whose lag grows first. Change one thing (scorer replicas, batch size, connector replicas, Flink parallelism), re-run that step, and record before and after.
4. **Volume:** at ≥ 30k tx/s sustained, replay all of HI-Large (≈ 180M rows, under 2 h). Otherwise replay a 20-day slice (≈ 37M rows) and say so in the README.
5. **Fault at scale:** pick a 1-day slice and run it twice, once clean and once deleting a broker pod and a TaskManager pod. Reconcile the two.
6. Agent: enable 100 drafts, then disable.
7. Save all results → `make cloud-down` → `make teardown-check` → next day, Cost Explorer filtered on `project=stream-aml` → `cost.json` + screenshot.

## Claude Code prompts
> Write infra/terraform/envs/scale per M09 B1. Node groups may use only these instance types: m7i-flex.large, c7i-flex.large (verify against `aws ec2 describe-instance-types --filters Name=free-tier-eligible,Values=true` output that I paste). No NAT, S3 gateway endpoint, Pod Identity, EBS CSI, taints, default_tags. Include the 6-node fallback as a variable. Run the cost-guard subagent, fix findings, output `terraform plan -out` for me. Do not apply.

> Write batch/glue wrappers and glue.tf for Glue 5.1 jobs that run batch/spark/csv_to_iceberg.py and features.py unchanged against the Glue Data Catalog. Verify every job parameter name against the Glue 5.1 documentation and list what you checked.

## Definition of Done
- [ ] T1: HI-Large in Iceberg on S3 via Glue; runtime and $ from the job's DPU-seconds; counts validated in Athena
- [ ] T4/T5: rate-ladder table, sustained throughput, p99 at that rate, bottleneck + fix
- [ ] One reconciled fault run at scale
- [ ] T11: Cost Explorer total and screenshot; teardown check CLEAN

## Gotchas
- **Arch mismatch:** an image built natively on Apple Silicon (arm64) won't run on c7i-flex/m7i-flex nodes. Use `buildx --platform linux/amd64`, or build in GitHub Actions on amd64 runners.
- **`cloud-down` order:** uninstall Helm releases and wait for PVC deletion **before** `terraform destroy`, or EBS volumes are orphaned.
- **The 6-month clock:** if your Free plan ends before Phase 5, you must upgrade (payment method), and you can set a spend limit from $20/month. Plan Phase 5 inside the window.
- **Credits are the hard stop:** on the Free plan, running out of credits stops usage rather than billing you. That's safe, but it would kill a session mid-run. Check the balance before Session B.
