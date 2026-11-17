package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/sakshat-patil/edgefleet/internal/model"
)

// SQS is a Queue backed by an AWS SQS standard queue. Credentials and the
// endpoint come from the default AWS config chain, so the same binary works
// against LocalStack (AWS_ENDPOINT_URL) and against real AWS via IRSA on EKS.
type SQS struct {
	client *sqs.Client
	url    string
}

// NewSQS builds an SQS queue client for queueURL.
func NewSQS(ctx context.Context, queueURL string) (*SQS, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return &SQS{client: sqs.NewFromConfig(cfg), url: queueURL}, nil
}

// Send batches commands 10 at a time (the SQS batch limit).
func (q *SQS) Send(ctx context.Context, cmds ...model.Command) error {
	for i := 0; i < len(cmds); i += 10 {
		end := min(i+10, len(cmds))
		entries := make([]types.SendMessageBatchRequestEntry, 0, end-i)
		for j, c := range cmds[i:end] {
			b, err := json.Marshal(c)
			if err != nil {
				return err
			}
			entries = append(entries, types.SendMessageBatchRequestEntry{
				Id:          aws.String(strconv.Itoa(j)),
				MessageBody: aws.String(string(b)),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"device_id": {DataType: aws.String("String"), StringValue: aws.String(c.DeviceID)},
					"type":      {DataType: aws.String("String"), StringValue: aws.String(c.Type)},
				},
			})
		}
		out, err := q.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: &q.url, Entries: entries})
		if err != nil {
			return fmt.Errorf("sqs send batch: %w", err)
		}
		if len(out.Failed) > 0 {
			return fmt.Errorf("sqs send batch: %d entries failed, first: %s", len(out.Failed), aws.ToString(out.Failed[0].Message))
		}
	}
	return nil
}

// Receive long-polls for up to max (<=10) messages.
func (q *SQS) Receive(ctx context.Context, max int) ([]Message, error) {
	out, err := q.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &q.url,
		MaxNumberOfMessages: int32(min(max, 10)),
		WaitTimeSeconds:     10,
	})
	if err != nil {
		return nil, fmt.Errorf("sqs receive: %w", err)
	}
	msgs := make([]Message, 0, len(out.Messages))
	for _, m := range out.Messages {
		var c model.Command
		if err := json.Unmarshal([]byte(aws.ToString(m.Body)), &c); err != nil {
			// Leave it on the queue; the redrive policy moves it to the DLQ.
			continue
		}
		msgs = append(msgs, Message{Command: c, Receipt: aws.ToString(m.ReceiptHandle)})
	}
	return msgs, nil
}

// Ack deletes processed messages.
func (q *SQS) Ack(ctx context.Context, receipts ...string) error {
	for i := 0; i < len(receipts); i += 10 {
		end := min(i+10, len(receipts))
		entries := make([]types.DeleteMessageBatchRequestEntry, 0, end-i)
		for j, r := range receipts[i:end] {
			entries = append(entries, types.DeleteMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(j)), ReceiptHandle: aws.String(r)})
		}
		if _, err := q.client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{QueueUrl: &q.url, Entries: entries}); err != nil {
			return fmt.Errorf("sqs delete batch: %w", err)
		}
	}
	return nil
}
