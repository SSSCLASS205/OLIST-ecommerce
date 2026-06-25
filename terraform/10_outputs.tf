output "rds_endpoint" {
  value = aws_db_instance.olist_oltp.address
}

output "s3_staging_bucket" {
  value = aws_s3_bucket.staging.bucket
}

output "mwaa_data_bucket" {
  value = aws_s3_bucket.mwaa_data.bucket
}

output "sns_alert_topic_arn" {
  value = aws_sns_topic.pipeline_alerts.arn
}

output "airbyte_instance_private_ip" {
  value = aws_instance.airbyte.private_ip
}

output "snowflake_s3_role_arn" {
  description = "Use this ARN as STORAGE_AWS_ROLE_ARN when creating the Snowflake storage integration"
  value       = aws_iam_role.snowflake_s3_role.arn
}

output "s3_staging_dev_bucket" {
  value       = aws_s3_bucket.staging_dev.bucket
  description = "DEV/CI staging bucket — Airbyte syncs sample data here on every PR"
}

output "snowflake_s3_role_dev_arn" {
  description = "Use this ARN as STORAGE_AWS_ROLE_ARN when creating the Snowflake DEV storage integration"
  value       = aws_iam_role.snowflake_s3_role_dev.arn
}