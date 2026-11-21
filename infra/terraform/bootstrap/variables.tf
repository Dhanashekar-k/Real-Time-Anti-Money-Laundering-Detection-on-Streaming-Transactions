variable "region" {
  description = "AWS region for everything in this project."
  type        = string
  default     = "us-east-2"
}

variable "aws_profile" {
  description = "Named AWS CLI profile for the stream-aml account."
  type        = string
  default     = "stream-aml"
}

variable "expected_account_id" {
  description = "12-digit ID of the stream-aml AWS account. The provider refuses any other account."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.expected_account_id))
    error_message = "expected_account_id must be a 12-digit AWS account ID."
  }
}

variable "owner" {
  description = "Value of the `owner` tag."
  type        = string
  default     = "shekar"
}

variable "alert_email" {
  description = "Where budget alerts go. Required, no default, so no address is committed."
  type        = string

  validation {
    condition     = can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.alert_email))
    error_message = "alert_email must be an email address."
  }
}

variable "budget_limits_usd" {
  description = "Monthly cost budgets in USD. $5 is the early warning; the Free plan's credit limit is the hard stop."
  type        = list(number)
  default     = [5, 20, 50]
}
