provider "aws" {
  region  = var.region
  profile = var.aws_profile

  # Refuse to run against any other account. This machine has other AWS profiles.
  allowed_account_ids = [var.expected_account_id]

  default_tags {
    tags = {
      project = "stream-aml"
      owner   = var.owner
      # The state bucket and budgets must outlive any session, unlike the "session" resources
      # in the cloud-up roots. Teardown checks should expect these to remain.
      ttl = "permanent"
    }
  }
}
