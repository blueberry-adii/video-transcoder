package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/blueberry-adii/video-transcoder/internal/queue"
	"github.com/blueberry-adii/video-transcoder/internal/storage"
)

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

	sqsActions := &queue.SqsActions{
		SqsClient: sqs.NewFromConfig(sdkConfig),
	}

	config := &queue.Config{
		Url:         "https://sqs.us-east-1.amazonaws.com/137110796336/notification-blueberry",
		MaxMessages: 1,
		WaitTime:    20,
	}

	ecsConfig := ecs.NewFromConfig(sdkConfig)

	for {
		messages, err := sqsActions.GetMessages(ctx, config.Url, config.MaxMessages, config.WaitTime)
		if err != nil {
			continue
		}
		if messages == nil {
			log.Printf("queue is empty, no messages yet!")
			continue
		}

		msg := messages[0]
		log.Printf("%s", *msg.Body)
		err = sqsActions.DeleteMessage(ctx, config.Url, *msg.ReceiptHandle)

		if err != nil {
			continue
		}

		storage.ProcessMessage(*msg.Body, ecsConfig)
	}
}
