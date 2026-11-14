variable "aws_region" {
  description = "AWS region for the S3 bucket and IAM user."
  type        = string
  default     = "us-east-1"
}

variable "bucket_suffix" {
  description = "Unique suffix for the gamepulse-<suffix> bucket name."
  type        = string
}

variable "budget_email" {
  description = "Email address for AWS budget alerts."
  type        = string
}

variable "budget_limit_usd" {
  description = "Monthly cost budget in USD."
  type        = number
  default     = 10
}