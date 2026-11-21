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
