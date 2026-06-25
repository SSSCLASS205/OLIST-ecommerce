resource "aws_s3_bucket" "staging" {
  bucket = "${var.project}-raw-staging-${data.aws_caller_identity.current.account_id}"
  tags   = { Name = "${var.project}-raw-staging" }
}

data "aws_caller_identity" "current" {}

resource "aws_s3_bucket_versioning" "staging" {
  bucket = aws_s3_bucket.staging.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "staging" {
  bucket = aws_s3_bucket.staging.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "staging" {
  bucket                  = aws_s3_bucket.staging.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle: raw landing files expire after N days once loaded into Snowflake Bronze,
# keeps replay capability without unbounded storage growth
resource "aws_s3_bucket_lifecycle_configuration" "staging" {
  bucket = aws_s3_bucket.staging.id
  rule {
    id     = "expire-raw-landing"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    expiration {
      days = 30
    }
  }
}

# Folder structure convention (created implicitly by Airbyte writes):
#   raw/orders/...
#   raw/order_items/...
#   raw/customers/...
# MWAA triggers Snowflake COPY INTO pointed at these prefixes per sync.

# Also used by MWAA / Airbyte for DAG/log artifacts if needed
resource "aws_s3_bucket" "mwaa_data" {
  bucket = "${var.project}-mwaa-data-${data.aws_caller_identity.current.account_id}"
  tags   = { Name = "${var.project}-mwaa-data" }
}

resource "aws_s3_bucket_versioning" "mwaa_data" {
  bucket = aws_s3_bucket.mwaa_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "mwaa_data" {
  bucket                  = aws_s3_bucket.mwaa_data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------------
# DEV / CI staging bucket
# Airbyte syncs a small sample of RDS data here on every PR.
# dbt CI runs dbt build against this bucket — never touches prod data.
# Short lifecycle (7 days) since CI data is throwaway.
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "staging_dev" {
  bucket = "${var.project}-raw-staging-dev-${data.aws_caller_identity.current.account_id}"
  tags   = { Name = "${var.project}-raw-staging-dev", Env = "dev" }
}

resource "aws_s3_bucket_versioning" "staging_dev" {
  bucket = aws_s3_bucket.staging_dev.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "staging_dev" {
  bucket = aws_s3_bucket.staging_dev.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "staging_dev" {
  bucket                  = aws_s3_bucket.staging_dev.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# CI data is throwaway — expire after 7 days to keep costs near zero
resource "aws_s3_bucket_lifecycle_configuration" "staging_dev" {
  bucket = aws_s3_bucket.staging_dev.id
  rule {
    id     = "expire-ci-raw-landing"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    expiration {
      days = 7
    }
  }
}