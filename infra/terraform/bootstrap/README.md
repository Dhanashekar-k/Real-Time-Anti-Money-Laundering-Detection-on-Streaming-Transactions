# infra/terraform/bootstrap

Creates, once per AWS account: the **Terraform state bucket** (versioned, encrypted, public access
blocked, TLS-only) and **three monthly cost budgets** ($5 / $20 / $50, email alerts at 80% and 100%
of actual cost). Everything else in `infra/terraform/` stores its state in that bucket.

## Run it (you, not Claude Code)

1. Create the `stream-aml` AWS CLI profile (region `us-east-2`, see D35).
2. `cp terraform.tfvars.example terraform.tfvars` and fill in `expected_account_id` and `alert_email`.
   The file is gitignored. The provider refuses to run against any other account ID.
3. From the repo root, **in your own terminal**: `make cloud-bootstrap`. It shows the plan and asks
   you to type `yes`. It refuses to run when stdin is not a terminal (which stops a plain
   `yes | make …`) and clears `TF_CLI_ARGS*` (so an environment variable cannot add the
   auto-approve flag). These guard against accidents; they are not a security boundary.
4. `terraform -chdir=infra/terraform/bootstrap output backend_config` prints the `backend "s3"` block
   for the other roots. It deliberately has **no bucket**: the bucket name contains the account ID,
   which should not be committed. Initialise each root with
   `terraform init -backend-config="bucket=$(terraform -chdir=infra/terraform/bootstrap output -raw state_bucket)"`.

## Things to know

- **State of this root is local** (`terraform.tfstate`, gitignored): it creates the bucket the other
  roots use, so it cannot live in it. It also holds the account ID and alert email in plaintext.
  **Back it up outside the repo** (or, after the first apply, add a partial `backend "s3"` block with
  `key = "bootstrap/terraform.tfstate"` and run `terraform init -migrate-state`). If you lose it,
  re-applying fails with `BucketAlreadyOwnedByYou` and you must `terraform import` the bucket and its
  sub-resources.
- **End-of-project teardown:** `prevent_destroy` and `force_destroy = false` stop `terraform destroy`
  on purpose. To remove everything: empty the bucket including all versions and delete markers,
  remove the `lifecycle` block, then destroy. `teardown-check` should treat `ttl=permanent` resources
  as expected until then.
- **Bucket policy:** TLS only, and TLS 1.2 or newer. Encryption is SSE-S3 (free); KMS would add cost
  for no benefit here.
- **Shared account, tag-filtered budgets.** stream-aml and edgarqa are the same AWS account (D16), so
  the budgets count only costs tagged `project=stream-aml` (`filter_budgets_by_tag`, default true).
  That requires `project` to be an **active cost-allocation tag** in Billing. The key shows up there
  only after tagged resources exist (so after this root is applied) and can take up to 24 h. On
  2026-11-21 listing cost-allocation tags from this account was refused ("Linked account doesn't
  have access to cost allocation tags"), so activation may need the Organization's management
  account. **A filtered budget whose tag is not active stays at $0 and never warns.** Before Phase 5,
  verify that the tag shows as Active and that a tagged, billed resource moves the budget's actual
  spend. If the first apply is rejected, set `filter_budgets_by_tag = false` in `terraform.tfvars`,
  apply, and flip it back once the tag is active. Existing account-wide budgets (`project 1` at $5,
  `edgarqa-monthly-cost` at $80) are not managed here and keep covering the whole account.
- **Budgets exclude credits and refunds.** With $100 of sign-up credits, a default "actual cost"
  budget stays near $0 until the credits run out, so it would never warn you. These show what the
  usage costs before credits. They warn only; the Free plan's credit limit is the hard stop.
- **Tags:** `project=stream-aml`, `owner`, and `ttl=permanent` (the other roots use `ttl=session`).
  Teardown checks should expect these resources to remain.
- **Cost:** the bucket holds a few KB of state; budgets without actions are not billed. Not
  measured here: check Cost Explorer after the first day.
- No NAT gateways, no load balancers, no DynamoDB (locking is S3-native, `use_lockfile`, Terraform
  1.11+).
