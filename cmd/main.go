package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecsTypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/blueberry-adii/video-transcoder/internal/queue"
	"github.com/blueberry-adii/video-transcoder/internal/storage"
)

func processMessage(msgBody string, ecsConfig *ecs.Client) {
	var event storage.S3Event
	json.Unmarshal([]byte(msgBody), &event)

	if event.Event == "s3:TestEvent" {
		fmt.Println("Received S3 Test Event. Ignoring.")
		return
	}

	if len(event.Records) > 0 {
		videoKey := event.Records[0].S3.Object.Key
		key, _ := url.QueryUnescape(videoKey)
		fmt.Printf("Processing video: %s\n", key)
		// Spin ECS Container for ffmpeg transcoding
		envVars := []ecsTypes.KeyValuePair{
			{
				Name:  aws.String("BUCKET"),
				Value: aws.String(event.Records[0].S3.Bucket.Name),
			},
			{
				Name:  aws.String("KEY"),
				Value: aws.String(key),
			},
		}

		input := &ecs.RunTaskInput{
			Cluster:        aws.String("transcoder"),
			TaskDefinition: aws.String("arn:aws:ecs:us-east-1:137110796336:task-definition/transcoder:2"),
			LaunchType:     ecsTypes.LaunchTypeFargate,
			Overrides: &ecsTypes.TaskOverride{
				ContainerOverrides: []ecsTypes.ContainerOverride{
					{
						Name:        aws.String("my-container"),
						Environment: envVars,
					},
				},
			},
			NetworkConfiguration: &ecsTypes.NetworkConfiguration{
				AwsvpcConfiguration: &ecsTypes.AwsVpcConfiguration{
					Subnets:        []string{"subnet-0223e64311264a87d", "subnet-0dd59d6f622945c0e", "subnet-092ae77daa0fceecb"},
					AssignPublicIp: ecsTypes.AssignPublicIpEnabled,
				},
			},
			Count: aws.Int32(1),
		}

		result, err := ecsConfig.RunTask(context.TODO(), input)
		if err != nil {
			log.Fatalf("failed to run task: %v", err)
		}

		log.Printf("Task started: %s", *result.Tasks[0].TaskArn)
	}
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

		processMessage(*msg.Body, ecsConfig)
	}
}
