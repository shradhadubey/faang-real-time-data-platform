variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name prefix for all resources"
  type        = string
  default     = "faang-data-platform"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "staging", "production"], var.environment)
    error_message = "Environment must be dev, staging, or production."
  }
}

variable "kinesis_shard_count" {
  description = "Number of Kinesis shards (1 shard = 1 MB/s write, 2 MB/s read)"
  type        = number
  default     = 2
}

variable "alarm_sns_arn" {
  description = "SNS topic ARN for CloudWatch alarms (leave empty to skip)"
  type        = string
  default     = ""
}
