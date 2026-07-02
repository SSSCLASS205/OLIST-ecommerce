data "aws_region" "current" {}

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    # "al2023-ami-*-x86_64" also matches "al2023-ami-minimal-*-x86_64", which
    # does NOT ship amazon-ssm-agent preinstalled — that caused this instance
    # to never register with SSM. Pinning to the "2023." version prefix
    # excludes the minimal variant, since its name inserts "minimal-" right
    # after "al2023-ami-".
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
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
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:PutSecretValue"]
        Resource = aws_secretsmanager_secret.airbyte_admin_creds.arn
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

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 3
  }
  
  iam_instance_profile {
    name = aws_iam_instance_profile.airbyte_profile.name
  }

  network_interfaces {
    security_groups = [aws_security_group.airbyte_sg.id]
  }

  # Default AMI root volume (8-20GB depending on the AMI) isn't enough headroom
  # for a kind cluster pulling ~10 Airbyte platform images (server, worker,
  # workload-launcher, temporal, etc.) plus Docker's own layer cache. Ran out
  # of disk mid-pull with the default size -> ImagePullBackOff / "no space
  # left on device" from containerd.
  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size           = 50
      volume_type            = "gp3"
      delete_on_termination = true
    }
  }

  user_data = base64encode(<<-EOF
#!/bin/bash
set -euo pipefail
exec > >(tee /var/log/airbyte-init.log | logger -t airbyte-init) 2>&1

yum update -y
yum install -y docker git jq awscli postgresql15
systemctl enable --now docker

# abctl runs as ec2-user (see note below) but docker is only installed/started
# as root, and ec2-user is never added to the docker group — so the very
# first `docker` call abctl makes fails with a socket permission error.
# Add the group membership, then wait for the socket to actually exist
# before handing off, since `systemctl enable --now` can return slightly
# before dockerd finishes creating /var/run/docker.sock.
usermod -aG docker ec2-user

for i in $(seq 1 15); do
  [ -S /var/run/docker.sock ] && break
  echo "Waiting for docker socket... ($i/15)"
  sleep 2
done
[ -S /var/run/docker.sock ] || { echo "docker socket never appeared"; exit 1; }

# Install abctl — the current official Airbyte CLI (replaces run-ab-platform.sh)
curl -LsfS https://get.airbyte.com | bash -

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

# Write abctl values.yaml:
#   - Disable the bundled Postgres pod; point at our Multi-AZ RDS instead.
#   - Externalize logs/state/workspace output to S3 so every ASG node shares
#     the same backing stores rather than each holding its own local disk.
cat > /opt/airbyte/values.yaml <<EOT
postgresql:
  enabled: false

global:
  database:
    type: "external"
    host: "$DB_HOST"
    port: "$DB_PORT"
    # NOTE: Helm chart V2 (used by abctl >=0.30) renamed this key from
    # "database" to "name". The old key is silently ignored rather than
    # erroring, so the chart falls back to its default db name "db-airbyte"
    # which doesn't exist on RDS -> bootloader pod fails pre-install.
    name: "airbyte"
    user: "$DB_USER"
    password: "$DB_PASS"

  storage:
    type: "s3"
    # NOTE: chart V2 nests bucket names under a separate "bucket:" block
    # using the keys log/state/workloadOutput. The old V1-style flat keys
    # (logBucketName/stateBucketName/workspaceBucketName) directly under
    # "s3:" aren't recognized -> the whole storage/S3 client bean fails to
    # build, which surfaces as "region must not be blank or empty" even
    # though region is set, because the malformed block breaks the client
    # construction before region is ever read correctly.
    bucket:
      log: "${aws_s3_bucket.mwaa_data.bucket}"
      state: "${aws_s3_bucket.mwaa_data.bucket}"
      workloadOutput: "${aws_s3_bucket.mwaa_data.bucket}"
    s3:
      region: "${data.aws_region.current.name}"
      # We're relying on the EC2 instance profile (aws_iam_instance_profile.airbyte_profile)
      # for S3 permissions — no access keys exist anywhere in this setup.
      authenticationType: "instanceProfile"
EOT

# abctl stores its kubeconfig under ~/.airbyte/abctl/ so it must run as a
# real user with a home directory, not root. We use `sg docker -c` (not
# just `sudo -u ec2-user`) to force the ec2-user shell to pick up the
# docker group we just added it to, rather than relying on sudo re-reading
# group membership, which isn't guaranteed on every distro/PAM config.
# --insecure-cookies is required for plain-HTTP access on the private subnet;
# without it abctl refuses to set the session cookie on login.
sudo -u ec2-user sg docker -c "/usr/local/bin/abctl local install \
  --values /opt/airbyte/values.yaml \
  --port 8000 \
  --insecure-cookies"

CREDS_OUTPUT=$(sudo -u ec2-user sg docker -c "/usr/local/bin/abctl local credentials" 2>&1)
CREDS_OUTPUT=$(sudo -u ec2-user abctl local credentials --email sssclass205@gmail.com)
ADMIN_EMAIL=$(echo "$CREDS_OUTPUT" | grep -oP '(?<=Email: ).*')
ADMIN_PASS=$(echo "$CREDS_OUTPUT" | grep -oP '(?<=Password: ).*')

aws secretsmanager put-secret-value \
  --secret-id "${aws_secretsmanager_secret.airbyte_admin_creds.id}" \
  --region "${data.aws_region.current.name}" \
  --secret-string "$(jq -n --arg e "$ADMIN_EMAIL" --arg p "$ADMIN_PASS" '{email:$e, password:$p}')"

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

  instance_refresh {
    strategy = "Rolling"
    preferences {
      min_healthy_percentage = 0  
    }
  }

  tag {
    key                 = "Name"
    value               = "${var.project}-airbyte-asg"
    propagate_at_launch = true
  }
}
data "aws_instances" "airbyte_asg" {
  filter {
    name   = "tag:aws:autoscaling:groupName"
    values = [aws_autoscaling_group.airbyte_asg.name]
  }

  filter {
    name   = "instance-state-name"
    values = ["running"]
  }

  # Ensure ASG is created before querying instances
  depends_on = [aws_autoscaling_group.airbyte_asg]
}