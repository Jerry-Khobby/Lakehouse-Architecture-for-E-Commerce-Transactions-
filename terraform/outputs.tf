# ── S3 ────────────────────────────────────────────────────────────────────────
output "data_bucket_name" {
  description = "S3 bucket for raw, processed, archived and rejected data"
  value       = aws_s3_bucket.data.id
}

output "data_bucket_arn" {
  description = "ARN of the data bucket"
  value       = aws_s3_bucket.data.arn
}

output "scripts_bucket_name" {
  description = "S3 bucket where Glue scripts are deployed"
  value       = aws_s3_bucket.scripts.id
}

output "athena_results_bucket_name" {
  description = "S3 bucket for Athena query results"
  value       = aws_s3_bucket.athena_results.id
}

# ── Glue ──────────────────────────────────────────────────────────────────────
output "glue_role_arn" {
  description = "IAM role ARN used by all Glue jobs"
  value       = aws_iam_role.glue_role.arn
}

output "glue_database_name" {
  description = "Glue Data Catalog database name"
  value       = aws_glue_catalog_database.lakehouse.name
}

output "glue_job_products" {
  description = "Name of the products Glue ETL job"
  value       = aws_glue_job.products.name
}

output "glue_job_orders" {
  description = "Name of the orders Glue ETL job"
  value       = aws_glue_job.orders.name
}

output "glue_job_order_items" {
  description = "Name of the order_items Glue ETL job"
  value       = aws_glue_job.order_items.name
}

output "crawler_products" {
  description = "Name of the products Glue crawler"
  value       = aws_glue_crawler.products.name
}

output "crawler_orders" {
  description = "Name of the orders Glue crawler"
  value       = aws_glue_crawler.orders.name
}

output "crawler_order_items" {
  description = "Name of the order_items Glue crawler"
  value       = aws_glue_crawler.order_items.name
}

# ── Athena ────────────────────────────────────────────────────────────────────
output "athena_workgroup" {
  description = "Athena workgroup name"
  value       = aws_athena_workgroup.lakehouse.name
}

# ── Step Functions ────────────────────────────────────────────────────────────
output "sfn_state_machine_arn" {
  description = "ARN of the ETL Step Functions state machine"
  value       = aws_sfn_state_machine.etl_pipeline.arn
}

output "sfn_state_machine_name" {
  description = "Name of the ETL Step Functions state machine"
  value       = aws_sfn_state_machine.etl_pipeline.name
}

# ── IAM ───────────────────────────────────────────────────────────────────────
output "sfn_role_arn" {
  description = "IAM role ARN used by Step Functions"
  value       = aws_iam_role.sfn_role.arn
}

# ── Useful deploy commands ────────────────────────────────────────────────────
output "deploy_scripts_command" {
  description = "AWS CLI command to sync glue_jobs/ scripts to S3 after terraform apply"
  value       = "aws s3 sync ./glue_jobs s3://${aws_s3_bucket.scripts.id}/glue_jobs/ --delete"
}

output "manual_sfn_trigger_command" {
  description = "AWS CLI command to manually trigger the pipeline for testing"
  value       = "aws stepfunctions start-execution --state-machine-arn ${aws_sfn_state_machine.etl_pipeline.arn} --input '{\"bucket\":\"${aws_s3_bucket.data.id}\",\"key\":\"raw/test.csv\"}'"
}