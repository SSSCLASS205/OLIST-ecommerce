output "rds_endpoint" {
  value       = aws_db_instance.olist_oltp.address
  description = "RDS OLTP endpoint for Olist warehouse data"
}

output "s3_staging_bucket" {
  value       = aws_s3_bucket.staging.bucket
  description = "S3 bucket for Airbyte staging data"
}

output "mwaa_data_bucket" {
  value       = aws_s3_bucket.mwaa_data.bucket
  description = "S3 bucket for Airbyte logs, state, and workspace output"
}

output "sns_alert_topic_arn" {
  value       = aws_sns_topic.pipeline_alerts.arn
  description = "SNS topic for pipeline alerts"
}

# ==========================================
# AIRBYTE ASG OUTPUTS
# ==========================================
# Replaces the broken aws_instance.airbyte.private_ip reference
# Now correctly references the Auto Scaling Group

output "airbyte_asg_name" {
  value       = aws_autoscaling_group.airbyte_asg.name
  description = "Name of the Airbyte Auto Scaling Group"
}

output "airbyte_launch_template_id" {
  value       = aws_launch_template.airbyte_asg_lt.id
  description = "Launch template ID for Airbyte instances"
}

# Get all running instances from the ASG
output "airbyte_instance_ids" {
  value       = data.aws_instances.airbyte_asg.ids
  description = "Instance IDs of all running Airbyte instances in the ASG"
}

output "airbyte_instance_private_ips" {
  value       = data.aws_instances.airbyte_asg.private_ips
  description = "Private IP addresses of all running Airbyte instances"
}

# For quick access to first/only instance
output "airbyte_first_instance_id" {
  value = try(
    data.aws_instances.airbyte_asg.ids[0],
    null
  )
  description = "First Airbyte instance ID (useful for SSM access)"
}

output "airbyte_first_instance_ip" {
  value = try(
    data.aws_instances.airbyte_asg.private_ips[0],
    null
  )
  description = "Private IP of first Airbyte instance"
}

output "airbyte_access_instructions" {
  value = <<-EOT
    ╔════════════════════════════════════════════════════════════════╗
    ║         🔗 HOW TO ACCESS AIRBYTE                               ║
    ╚════════════════════════════════════════════════════════════════╝
    
    Step 1: Get Instance ID
    ─────────────────────────
    aws ec2 describe-instances \
      --filters "Name=tag:aws:autoscaling:groupName,Values=${aws_autoscaling_group.airbyte_asg.name}" \
      --query 'Reservations[].Instances[].[InstanceId,PrivateIpAddress,State.Name]' \
      --output table
    
    Step 2: Port-forward via SSM Session Manager
    ──────────────────────────────────────────────
    aws ssm start-session \
      --target <INSTANCE_ID> \
      --document-name AWS-StartPortForwardingSession \
      --parameters "localPortNumber=8000,portNumber=8000"
    
    Step 3: Open in Browser
    ───────────────────────
    http://localhost:8000
    
    💡 NOTES:
    • No SSH keys needed — uses AWS Systems Manager Session Manager
    • Connection stays open until you press Ctrl+C
    • If instance fails, ASG automatically launches a replacement
    • All config is stored in shared RDS database (survives failures)
    • All logs are stored in S3 (survives failures)
  EOT
  description = "Instructions for accessing Airbyte UI via SSM Session Manager"
}

# ==========================================
# SNOWFLAKE STORAGE INTEGRATION OUTPUTS
# ==========================================

output "snowflake_s3_role_arn" {
  description = "Use this ARN as STORAGE_AWS_ROLE_ARN when creating the Snowflake storage integration"
  value       = aws_iam_role.snowflake_s3_role.arn
}

# ==========================================
# DEV/CI ENVIRONMENT OUTPUTS
# ==========================================

output "s3_staging_dev_bucket" {
  value       = aws_s3_bucket.staging_dev.bucket
  description = "DEV/CI staging bucket — Airbyte syncs sample data here on every PR"
}

output "snowflake_s3_role_dev_arn" {
  description = "Use this ARN as STORAGE_AWS_ROLE_ARN when creating the Snowflake DEV storage integration"
  value       = aws_iam_role.snowflake_s3_role_dev.arn
}