resource "aws_secretsmanager_secret" "rds_creds" {
  name = "${var.project}/rds-oltp-credentials"
}

resource "aws_secretsmanager_secret_version" "rds_creds" {
  secret_id = aws_secretsmanager_secret.rds_creds.id
  secret_string = jsonencode({
    username = var.db_username
    password = var.db_password
    host     = aws_db_instance.olist_oltp.address
    port     = 5432
    dbname   = "olist"
  })
}

resource "aws_secretsmanager_secret" "github_deploy_key" {
  name = "${var.project}/github-dbt-deploy-key"
}

# Populate out-of-band: generate a read-only deploy key in the dbt repo's
# GitHub settings (Settings > Deploy keys, do NOT check "Allow write access"),
# then store the private key here. Terraform never sees the key material.
resource "aws_secretsmanager_secret_version" "github_deploy_key" {
  secret_id     = aws_secretsmanager_secret.github_deploy_key.id
  secret_string = jsonencode({
    private_key = "REPLACE_OUT_OF_BAND_VIA_AWS_CLI_OR_CONSOLE"
    repo_url    = "git@github.com:your-org/olist_ecommerce.git"
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}
  name = "${var.project}/snowflake-credentials"
}

# Populate the actual secret value out-of-band (terraform should not store
# Snowflake passwords in state in plaintext long-term); placeholder here.
resource "aws_secretsmanager_secret_version" "snowflake_creds" {
  secret_id     = aws_secretsmanager_secret.snowflake_creds.id
  secret_string = jsonencode({
    account   = "REPLACE_ME"
    user      = "REPLACE_ME"
    password  = "REPLACE_ME"
    role      = "REPLACE_ME"
    warehouse = "REPLACE_ME"
    database  = "REPLACE_ME"
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}
resource "aws_secretsmanager_secret" "airbyte_config" {
  name = "${var.project}/airbyte-config"
}

# Populate after first `terraform apply` once the EC2 is up:
#   aws secretsmanager put-secret-value \
#     --secret-id olist/airbyte-config \
#     --secret-string '{"private_ip": "<AIRBYTE_EC2_PRIVATE_IP>", "workspace_id": "<AIRBYTE_WORKSPACE_UUID>"}'
#
# Get private IP from: terraform output airbyte_instance_private_ip
# Get workspace ID from: Airbyte UI → Settings → Workspace → Workspace ID
resource "aws_secretsmanager_secret_version" "airbyte_config" {
  secret_id     = aws_secretsmanager_secret.airbyte_config.id
  secret_string = jsonencode({
    private_ip   = "REPLACE_AFTER_TERRAFORM_APPLY"
    workspace_id = "REPLACE_FROM_AIRBYTE_UI"
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}