---
name: teardown-check
description: List AWS resources that may still be billing for stream-aml, using read-only AWS CLI commands only. Use after every cloud session, before starting one, or whenever the user asks whether anything is still running.
disable-model-invocation: true
allowed-tools: Bash(aws sts get-caller-identity *) Bash(aws eks list-clusters *) Bash(aws ec2 describe-instances *) Bash(aws ec2 describe-nat-gateways *) Bash(aws ec2 describe-volumes *) Bash(aws ec2 describe-addresses *) Bash(aws elbv2 describe-load-balancers *) Bash(aws elb describe-load-balancers *) Bash(aws glue get-job-runs *) Bash(aws glue list-sessions *) Bash(aws ecr describe-repositories *) Bash(aws s3api list-buckets *) Bash(aws resourcegroupstaggingapi get-resources *)
---

Check for leftover billable resources of **stream-aml** in `us-east-2` for profile `stream-aml`. Read-only commands only. Never delete anything; if something is found, tell the user the exact resource and the `make cloud-down` (or console) step to remove it.

**The AWS account is shared with the edgarqa project (D16).** Classify every resource you list, and never report another project's resources as stream-aml leftovers:

- **ours**: tag `project=stream-aml`, or a name starting `aml-` or `stream-aml-` (EKS clusters, Glue jobs, ECR repositories, load balancers, buckets).
- **other project (ignored)**: has a `project` tag with a different value (for example `edgarqa`). List it under "ignored", do not count it.
- **UNATTRIBUTED**: no `project` tag and no `aml-` name. Resources created by in-cluster controllers (EBS volumes for Strimzi PVCs, load balancers) often lack the tag, so a tag-only scope would miss exactly the leftovers that cost money. Never ignore these: list them and ask the user whether they belong to edgarqa.

Run (with `--region us-east-2 --profile stream-aml --output json`, use `--query` to keep output short, and include the project tag in the output, e.g. `--query "Reservations[].Instances[].[InstanceId,InstanceType,State.Name,Tags[?Key=='project']|[0].Value]"`):
1. `aws sts get-caller-identity` (confirm the account; remember it is shared)
2. `aws eks list-clusters` (classify by name)
3. `aws ec2 describe-instances --filters Name=instance-state-name,Values=pending,running,stopping,stopped` (include the `project` tag value)
4. `aws ec2 describe-nat-gateways --filter Name=state,Values=pending,available` (include the tag value)
5. `aws ec2 describe-volumes` (include the tag value; report `available` volumes separately: those are orphaned)
6. `aws ec2 describe-addresses` (Elastic IPs; include the tag value)
7. `aws elbv2 describe-load-balancers` and `aws elb describe-load-balancers` (no tags in this output: classify by name, anything else is UNATTRIBUTED)
8. `aws glue get-job-runs --job-name aml-csv-to-iceberg` and `--job-name aml-features` (report any run in `STARTING`/`RUNNING`/`STOPPING`), and `aws glue list-sessions` (report any interactive session; none should exist)
9. `aws ecr describe-repositories` and `aws s3api list-buckets` (informational: small monthly cost; classify by name)
10. `aws resourcegroupstaggingapi get-resources --tag-filters Key=project,Values=stream-aml` (everything tagged ours)

Expected and NOT leftovers: the Terraform state bucket `stream-aml-tfstate-*` and the budgets from `infra/terraform/bootstrap` (tagged `ttl=permanent`).

Output a table: resource type · id/name · state · project (ours / other / UNATTRIBUTED) · billing risk (running hourly / storage only / none) · how to remove. End with one line:
- "CLEAN" if no hourly resource is ours or UNATTRIBUTED;
- "NOT CLEAN — N hourly stream-aml resources" if any hourly resource is ours (add the UNATTRIBUTED count if there are some);
- "UNCLEAR — N hourly resources have no project tag" if the only hourly resources are UNATTRIBUTED (ask the user whether they belong to edgarqa).
