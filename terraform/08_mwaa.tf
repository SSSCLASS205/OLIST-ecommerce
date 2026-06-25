resource "aws_iam_role" "mwaa_role" {
  name = "${var.project}-mwaa-execution-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = [
          "airflow.amazonaws.com",
          "airflow-env.amazonaws.com"
        ]
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "mwaa_policy" {
  name = "${var.project}-mwaa-policy"
  role = aws_iam_role.mwaa_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["airflow:PublishMetrics"]
        Resource = "arn:aws:airflow:${var.aws_region}:${data.aws_caller_identity.current.account_id}:environment/${var.project}-mwaa"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject*", "s3:GetBucket*", "s3:List*", "s3:PutObject*"]
        Resource = [aws_s3_bucket.mwaa_data.arn, "${aws_s3_bucket.mwaa_data.arn}/*", aws_s3_bucket.staging.arn, "${aws_s3_bucket.staging.arn}/*", aws_s3_bucket.staging_dev.arn, "${aws_s3_bucket.staging_dev.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:CreateLogGroup", "logs:PutLogEvents", "logs:GetLogEvents", "logs:GetLogRecord", "logs:GetLogGroupFields", "logs:GetQueryResults", "logs:DescribeLogGroups"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:ChangeMessageVisibility", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl", "sqs:ReceiveMessage", "sqs:SendMessage"]
        Resource = "arn:aws:sqs:${var.aws_region}:*:airflow-celery-*"
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = [aws_secretsmanager_secret.rds_creds.arn, aws_secretsmanager_secret.snowflake_creds.arn, aws_secretsmanager_secret.github_deploy_key.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = aws_sns_topic.pipeline_alerts.arn
      }
    ]
  })
}

resource "aws_mwaa_environment" "olist_mwaa" {
  name              = "${var.project}-mwaa"
  airflow_version   = "2.9.2"
  environment_class  = "mw1.small"
  execution_role_arn = aws_iam_role.mwaa_role.arn

  source_bucket_arn = aws_s3_bucket.mwaa_data.arn
  dag_s3_path        = "dags/"

  network_configuration {
    security_group_ids = [aws_security_group.mwaa_sg.id]
    subnet_ids          = [aws_subnet.private_az1.id, aws_subnet.private_az2.id]
  }

logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }

    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }

    task_logs {
      enabled   = true
      log_level = "INFO"
    }

    webserver_logs {
      enabled   = true
      log_level = "INFO"
    }

    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  webserver_access_mode = "PRIVATE_ONLY"

  tags = { Name = "${var.project}-mwaa" }
}