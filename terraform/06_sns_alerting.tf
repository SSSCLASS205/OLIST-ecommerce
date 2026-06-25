resource "aws_sns_topic" "pipeline_alerts" {
  name = "${var.project}-pipeline-alerts"
}

resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_sqs_queue" "alert_dlq" {
  name = "${var.project}-alert-dlq"
}

resource "aws_sns_topic_subscription" "sqs_alert" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.alert_dlq.arn
}

resource "aws_sqs_queue_policy" "allow_sns" {
  queue_url = aws_sqs_queue.alert_dlq.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "sns.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.alert_dlq.arn
      Condition = { ArnEquals = { "aws:SourceArn" = aws_sns_topic.pipeline_alerts.arn } }
    }]
  })
}

# CloudWatch alarm on MWAA scheduler health -> feeds SNS.
# DAG-level failure -> use an on_failure_callback in the DAG to sns.publish(),
# this alarm catches infra-level scheduler/worker failures.
resource "aws_cloudwatch_metric_alarm" "mwaa_scheduler_health" {
  alarm_name          = "${var.project}-mwaa-scheduler-unhealthy"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 1
  metric_name         = "SchedulerHeartbeat"
  namespace           = "AmazonMWAA"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]
  dimensions = {
    Function = "Scheduler"
  }
}
