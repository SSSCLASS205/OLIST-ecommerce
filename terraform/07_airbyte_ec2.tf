data "aws_region" "current" {}

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
}

resource "aws_iam_role" "airbyte_role" {
  name = "${var.project}-airbyte-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "airbyte_policy" {
  name = "${var.project}-airbyte-policy"
  role = aws_iam_role.airbyte_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:GetObject", "s3:ListBucket", "s3:DeleteObject"]
        Resource = [
          aws_s3_bucket.staging.arn, "${aws_s3_bucket.staging.arn}/*",
          aws_s3_bucket.mwaa_data.arn, "${aws_s3_bucket.mwaa_data.arn}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = [aws_secretsmanager_secret.rds_creds.arn]
      }
    ]
  })
}

# Lets the airbyte instance act as an SSM-managed node, so we can
# port-forward to RDS via `aws ssm start-session` without opening any
# inbound SSH port or managing key pairs for tunneling purposes.
resource "aws_iam_role_policy_attachment" "airbyte_ssm" {
  role       = aws_iam_role.airbyte_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "airbyte_profile" {
  name = "${var.project}-airbyte-profile"
  role = aws_iam_role.airbyte_role.name
}


# Standby / autoscaling group for Airbyte, matching diagram's HA intent.
resource "aws_launch_template" "airbyte_asg_lt" {
  name_prefix   = "${var.project}-airbyte-"
  image_id      = data.aws_ami.amazon_linux.id
  instance_type = "t3.large"
  key_name      = var.key_pair_name

  iam_instance_profile {
    name = aws_iam_instance_profile.airbyte_profile.name
  }

  network_interfaces {
    security_groups = [aws_security_group.airbyte_sg.id]
  }

  user_data = base64encode(<<-EOF
    #!/bin/bash
    yum update -y
    yum install -y docker git jq awscli postgresql15
    systemctl enable --now docker

    mkdir -p /opt/airbyte
    cd /opt/airbyte

    # Creds + endpoint already live in the rds_creds secret (05_secrets.tf)
    SECRET_JSON=$(aws secretsmanager get-secret-value \
      --secret-id "${aws_secretsmanager_secret.rds_creds.id}" \
      --region "${data.aws_region.current.name}" \
      --query SecretString --output text)

    DB_USER=$(echo "$SECRET_JSON" | jq -r .username)
    DB_PASS=$(echo "$SECRET_JSON" | jq -r .password)
    DB_HOST=$(echo "$SECRET_JSON" | jq -r .host)
    DB_PORT=$(echo "$SECRET_JSON" | jq -r .port)

    # olist_oltp's default db is "olist" (the warehouse OLTP data) — give
    # Airbyte its own database on the same Multi-AZ instance so config/
    # metadata doesn't mix with app data.
    PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres \
      -tc "SELECT 1 FROM pg_database WHERE datname = 'airbyte'" | grep -q 1 || \
      PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres \
      -c "CREATE DATABASE airbyte"

    curl -L https://raw.githubusercontent.com/airbytehq/airbyte-platform/main/run-ab-platform.sh -o run-ab-platform.sh
    chmod +x run-ab-platform.sh

    # Externalize Airbyte's config DB to RDS (Multi-AZ) and its logs/state/
    # workspace output to S3, so every ASG node shares the same backing
    # stores instead of each holding its own local Postgres + local disk.
    cat > .env <<EOT
    DATABASE_USER=$DB_USER
    DATABASE_PASSWORD=$DB_PASS
    DATABASE_HOST=$DB_HOST
    DATABASE_PORT=$DB_PORT
    DATABASE_DB=airbyte

    STORAGE_TYPE=S3
    STORAGE_BUCKET_LOG=${aws_s3_bucket.mwaa_data.bucket}
    STORAGE_BUCKET_STATE=${aws_s3_bucket.mwaa_data.bucket}
    STORAGE_BUCKET_WORKSPACE_OUTPUT=${aws_s3_bucket.mwaa_data.bucket}
    AWS_DEFAULT_REGION=${data.aws_region.current.name}
    EOT

    ./run-ab-platform.sh -b
  EOF
  )
}

resource "aws_autoscaling_group" "airbyte_asg" {
  desired_capacity    = 1
  min_size            = 1
  max_size            = 2
  vpc_zone_identifier = [aws_subnet.private_az1.id, aws_subnet.private_az2.id]

  launch_template {
    id      = aws_launch_template.airbyte_asg_lt.id
    version = "$Latest"
  }

  tag {
    key                 = "Name"
    value               = "${var.project}-airbyte-asg"
    propagate_at_launch = true
  }
}