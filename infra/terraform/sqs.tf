# Device command queue. The control plane's dispatcher long-polls it and moves
# commands into per-device Redis lists; poison messages land in the DLQ after
# 5 receives.
resource "aws_sqs_queue" "commands_dlq" {
  name                      = "${local.name}-commands-dlq"
  message_retention_seconds = 1209600 # 14 days
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "commands" {
  name                       = "${local.name}-commands"
  visibility_timeout_seconds = 30
  message_retention_seconds  = 345600 # 4 days: covers devices offline over a weekend
  receive_wait_time_seconds  = 10
  sqs_managed_sse_enabled    = true
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.commands_dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "${local.name}-commands-dlq-not-empty"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.commands_dlq.name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
}
