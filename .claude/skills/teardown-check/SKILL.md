---
name: teardown-check
description: List AWS resources that may still be billing for stream-aml, using read-only AWS CLI commands only. Use after every cloud session, before starting one, or whenever the user asks whether anything is still running.
disable-model-invocation: true
allowed-tools: Bash(aws sts get-caller-identity *) Bash(aws eks list-clusters *) Bash(aws ec2 describe-instances *) Bash(aws ec2 describe-nat-gateways *) Bash(aws ec2 describe-volumes *) Bash(aws ec2 describe-addresses *) Bash(aws elbv2 describe-load-balancers *) Bash(aws elb describe-load-balancers *) Bash(aws glue get-job-runs *) Bash(aws glue list-sessions *) Bash(aws ecr describe-repositories *) Bash(aws s3api list-buckets *) Bash(aws resourcegroupstaggingapi get-resources *)
---

Check for leftover billable resources in `us-east-2` for profile `stream-aml`. Read-only commands only. Never delete anything; if something is found, tell the user the exact resource and the `make cloud-down` (or console) step to remove it.

Run (with `--region us-east-2 --profile stream-aml --output json`, use `--query` to keep output short):
1. `aws sts get-caller-identity` (confirm the account)
2. `aws eks list-clusters`
3. `aws ec2 describe-instances --filters Name=instance-state-name,Values=pending,running,stopping,stopped`
4. `aws ec2 describe-nat-gateways --filter Name=state,Values=pending,available`
5. `aws ec2 describe-volumes` (report `available` volumes separately: those are orphaned)
6. `aws ec2 describe-addresses` (Elastic IPs)
7. `aws elbv2 describe-load-balancers` and `aws elb describe-load-balancers`
8. `aws glue get-job-runs --job-name aml-csv-to-iceberg` and `--job-name aml-features` (report any run in `STARTING`/`RUNNING`/`STOPPING`), and `aws glue list-sessions` (report any interactive session; none should exist)
9. `aws ecr describe-repositories` and `aws s3api list-buckets` (informational: small monthly cost)
10. `aws resourcegroupstaggingapi get-resources --tag-filters Key=project,Values=stream-aml`

Output a table: resource type · id/name · state · billing risk (running hourly / storage only / none) · how to remove. End with one line: "CLEAN" if nothing hourly is running, otherwise "NOT CLEAN — N hourly resources".
