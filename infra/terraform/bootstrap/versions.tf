terraform {
  # use_lockfile (S3-native state locking, used by every other root) is GA from Terraform 1.11.
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.73"
    }
  }

  # No backend block on purpose: this root creates the state bucket, so its own state is local
  # (terraform.tfstate, gitignored). Keep that file; see README.md.
}
