# ── Project metadata ──────────────────────────────────────────────────────────
variable "project_name" {
  description = "Short slug used in all resource names"
  type        = string
  default     = "ecom-lakehouse"
}

variable "environment" {
  description = "Deployment environment: dev | staging | prod"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "team_owner" {
  description = "Team or individual owning this infrastructure"
  type        = string
  default     = "data-engineering"
}

# ── S3 ────────────────────────────────────────────────────────────────────────
variable "raw_data_prefix" {
  description = "S3 prefix where source CSV files land"
  type        = string
  default     = "raw/"
}

variable "processed_data_prefix" {
  description = "S3 prefix for Delta Lake processed tables"
  type        = string
  default     = "lakehouse-dwh/"
}

variable "archived_data_prefix" {
  description = "S3 prefix for successfully ingested raw files"
  type        = string
  default     = "archived/"
}

variable "rejected_data_prefix" {
  description = "S3 prefix for records that failed validation"
  type        = string
  default     = "rejected/"
}

variable "flagged_data_prefix" {
  description = "S3 prefix for soft-flagged records that pass but require analyst review"
  type        = string
  default     = "flagged/"
}

variable "log_retention_days" {
  description = "Days to retain S3 access logs"
  type        = number
  default     = 90
}

variable "noncurrent_version_expiry_days" {
  description = "Days before old object versions are expired"
  type        = number
  default     = 30
}

# ── Glue ──────────────────────────────────────────────────────────────────────
variable "glue_version" {
  description = "AWS Glue version"
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Glue worker type: G.1X | G.2X | G.025X"
  type        = string
  default     = "G.1X"
}

variable "glue_num_workers" {
  description = "Number of Glue workers per job"
  type        = number
  default     = 2
}

variable "glue_max_retries" {
  description = "Max automatic retries for a failed Glue job run"
  type        = number
  default     = 1
}

variable "glue_timeout_minutes" {
  description = "Glue job timeout in minutes"
  type        = number
  default     = 60
}

# ── Glue Catalog ──────────────────────────────────────────────────────────────
variable "glue_database_name" {
  description = "Glue Data Catalog database name"
  type        = string
  default     = "ecom_lakehouse_db"
}

variable "crawler_schedule" {
  description = "Cron schedule for Glue crawlers (empty = on-demand only)"
  type        = string
  default     = ""
}

# ── Athena ────────────────────────────────────────────────────────────────────
variable "athena_workgroup_name" {
  description = "Athena workgroup name"
  type        = string
  default     = "ecom-lakehouse-wg"
}

variable "athena_bytes_scanned_cutoff" {
  description = "Per-query data scan limit in bytes (cost control)"
  type        = number
  default     = 1073741824 # 1 GB
}

# ── Step Functions ────────────────────────────────────────────────────────────
variable "sfn_timeout_seconds" {
  description = "Step Functions execution timeout in seconds"
  type        = number
  default     = 7200 # 2 hours
}

# ── Notifications ─────────────────────────────────────────────────────────────
variable "alert_email" {
  description = "Email address for pipeline alerts"
  type        = string
  default     = "jeremiah.coblah@amalitechtraining.org"
}

variable "slack_webhook_url" {
  description = "Slack incoming-webhook URL for pipeline alerts (optional)"
  type        = string
  sensitive   = true
  default     = ""
}