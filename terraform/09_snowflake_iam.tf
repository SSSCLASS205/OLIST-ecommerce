# This role is assumed by Snowflake (via its storage integration) to read
# from the S3 staging bucket for COPY INTO / external tables.
# After creating the storage integration in Snowflake, run:
#   DESC STORAGE INTEGRATION s3_olist_int;
# and update trust_relationship_external_id + snowflake_aws_iam_user below
# to match the STORAGE_AWS_IAM_USER_ARN / STORAGE_AWS_EXTERNAL_ID returned,
# then re-apply (chicken-and-egg step, common with Snowflake+AWS integrations).

variable "snowflake_aws_iam_user_arn" {
  description = "STORAGE_AWS_IAM_USER_ARN from Snowflake DESC STORAGE INTEGRATION (placeholder until first apply)"
  type        = string
  default     = "arn:aws:iam::000000000000:user/placeholder"
}

variable "snowflake_external_id" {
  description = "STORAGE_AWS_EXTERNAL_ID from Snowflake DESC STORAGE INTEGRATION"
  type        = string
  default     = "placeholder_external_id"
}

resource "aws_iam_role" "snowflake_s3_role" {
  name = "${var.project}-snowflake-s3-access"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = var.snowflake_aws_iam_user_arn }
      Action    = "sts:AssumeRole"
      Condition = {
        StringEquals = { "sts:ExternalId" = var.snowflake_external_id }
      }
    }]
  })
}

resource "aws_iam_role_policy" "snowflake_s3_policy" {
  name = "${var.project}-snowflake-s3-policy"
  role = aws_iam_role.snowflake_s3_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:GetObjectVersion"]
      Resource = "${aws_s3_bucket.staging.arn}/*"
      }, {
      Effect   = "Allow"
      Action   = ["s3:ListBucket"]
      Resource = aws_s3_bucket.staging.arn
    }]
  })
}

# ---------------------------------------------------------------------------
# Snowflake storage integration role for DEV bucket (CI only)
# Same chicken-and-egg flow as prod:
#   1. terraform apply → get role ARN → create Snowflake storage integration
#   2. DESC STORAGE INTEGRATION → fill snowflake_dev_aws_iam_user_arn + snowflake_dev_external_id
#   3. terraform apply again to lock the trust policy
# ---------------------------------------------------------------------------
variable "snowflake_dev_aws_iam_user_arn" {
  description = "STORAGE_AWS_IAM_USER_ARN from Snowflake DESC STORAGE INTEGRATION for dev"
  type        = string
  default     = "arn:aws:iam::000000000000:user/placeholder-dev"
}

variable "snowflake_dev_external_id" {
  description = "STORAGE_AWS_EXTERNAL_ID from Snowflake DESC STORAGE INTEGRATION for dev"
  type        = string
  default     = "placeholder_dev_external_id"
}

resource "aws_iam_role" "snowflake_s3_role_dev" {
  name = "${var.project}-snowflake-s3-access-dev"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = var.snowflake_dev_aws_iam_user_arn }
      Action    = "sts:AssumeRole"
      Condition = {
        StringEquals = { "sts:ExternalId" = var.snowflake_dev_external_id }
      }
    }]
  })
}

resource "aws_iam_role_policy" "snowflake_s3_policy_dev" {
  name = "${var.project}-snowflake-s3-policy-dev"
  role = aws_iam_role.snowflake_s3_role_dev.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Dev Snowflake role can ONLY read from dev bucket — never prod
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = "${aws_s3_bucket.staging_dev.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.staging_dev.arn
      }
    ]
  })
}