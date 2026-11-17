#!/bin/sh
awslocal sqs create-queue --queue-name edgefleet-commands-dlq
awslocal sqs create-queue --queue-name edgefleet-commands \
  --attributes '{"ReceiveMessageWaitTimeSeconds":"10","RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:edgefleet-commands-dlq\",\"maxReceiveCount\":\"5\"}"}'
