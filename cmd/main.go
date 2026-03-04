package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// SqsActions encapsulates the Amazon Simple Queue Service (Amazon SQS) actions
// used in the examples.
type SqsActions struct {
	SqsClient *sqs.Client
}

type Config struct {
	url         string
	maxMessages int32
	waitTime    int32
}

// GetMessages uses the ReceiveMessage action to get messages from an Amazon SQS queue.
func (actor SqsActions) GetMessages(ctx context.Context, queueUrl string, maxMessages int32, waitTime int32) ([]types.Message, error) {
	var messages []types.Message
	result, err := actor.SqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueUrl),
		MaxNumberOfMessages: maxMessages,
		WaitTimeSeconds:     waitTime,
	})
	if err != nil {
		log.Printf("Couldn't get messages from queue %v. Here's why: %v\n", queueUrl, err)
	} else {
		messages = result.Messages
	}
	return messages, err
}

// main uses the AWS SDK for Go V2 to create an Amazon Simple Queue Service
// (Amazon SQS) client and list the queues in your account.
// This example uses the default settings specified in your shared credentials
// and config files.
func main() {
	ctx := context.Background()
	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		fmt.Println("Couldn't load default configuration. Have you set up your AWS account?")
		fmt.Println(err)
		return
	}

	sqsActions := &SqsActions{
		SqsClient: sqs.NewFromConfig(sdkConfig),
	}

	config := &Config{
		url:         "https://sqs.us-east-1.amazonaws.com/137110796336/notification-blueberry",
		maxMessages: 1,
		waitTime:    20,
	}

	for {
		messages, err := sqsActions.GetMessages(ctx, config.url, config.maxMessages, config.waitTime)
		if err != nil {
			continue
		}
		if messages == nil {
			log.Printf("queue is empty, no messages yet!")
			continue
		}

		msg := messages[0]
		log.Printf("%s", *msg.Body)
	}
}
