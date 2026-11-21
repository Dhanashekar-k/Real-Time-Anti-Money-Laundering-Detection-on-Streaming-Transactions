output "state_bucket" {
  description = "Name of the Terraform state bucket."
  value       = aws_s3_bucket.state.id
}

output "region" {
  value = var.region
}

output "budget_names" {
  description = "Names of the budgets created."
  value       = sort([for b in aws_budgets_budget.monthly : b.name])
}

# Partial backend config: the bucket name contains the account ID, and a backend block cannot use
# variables, so the bucket is NOT written into the other roots' .tf files. Commit this block, and
# pass the bucket when initialising:
#   terraform init -backend-config="bucket=$(terraform -chdir=infra/terraform/bootstrap output -raw state_bucket)"
output "backend_config" {
  description = "Backend block for the other Terraform roots (bucket is passed at init time)."
  value       = <<-EOT
    backend "s3" {
      key          = "<root-name>/terraform.tfstate"
      region       = "${var.region}"
      encrypt      = true
      use_lockfile = true
    }
  EOT
}
