# Monthly cost budgets with email alerts. They warn; they do not stop anything. The Free plan's
# credit limit is the hard stop.
resource "aws_budgets_budget" "monthly" {
  for_each = { for limit in var.budget_limits_usd : tostring(limit) => limit }

  name         = "stream-aml-monthly-${each.key}-usd"
  budget_type  = "COST"
  limit_amount = tostring(each.value)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # Credits and refunds are NOT netted out. With $100 of sign-up credits a default budget would
  # read about $0 until the credits run out, so the early warning would never fire. These budgets
  # track what the usage costs before credits.
  cost_types {
    include_credit = false
    include_refund = false
  }

  # SHARED ACCOUNT: stream-aml and edgarqa live in the same AWS account (D16), so these budgets
  # count only costs tagged project=stream-aml (the provider's format for a user tag is
  # "user:<key>$<value>", per the aws_budgets_budget docs for 6.73).
  #
  # This depends on `project` being an ACTIVE cost-allocation tag in Billing:
  #  - the key appears there only after resources carrying it exist, i.e. after this root has been
  #    applied (default_tags puts it on the bucket), and can take up to 24 h to show up;
  #  - on 2026-11-21 `aws ce list-cost-allocation-tags` from this account was refused with "Linked
  #    account doesn't have access to cost allocation tags", so activation may only be possible
  #    from an Organization management account. If it cannot be activated, a filtered budget
  #    stays at $0 and NEVER warns.
  # VERIFY BEFORE PHASE 5 (see README): the tag shows as Active, and a tagged resource that costs
  # money moves the budget's actual spend. If the first apply is rejected because the tag is not
  # known yet, set filter_budgets_by_tag = false, apply, then set it back to true after activation.
  dynamic "cost_filter" {
    for_each = var.filter_budgets_by_tag ? [1] : []

    content {
      name   = "TagKeyValue"
      values = ["user:project$stream-aml"]
    }
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }
}
