package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
)

func ProcessMessage(msgBody string, ecsConfig *ecs.Client) {
	var event S3Event
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
		envVars := []types.KeyValuePair{
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
			LaunchType:     types.LaunchTypeFargate,
			Overrides: &types.TaskOverride{
				ContainerOverrides: []types.ContainerOverride{
					{
						Name:        aws.String("my-container"),
						Environment: envVars,
					},
				},
			},
			NetworkConfiguration: &types.NetworkConfiguration{
				AwsvpcConfiguration: &types.AwsVpcConfiguration{
					Subnets:        []string{"subnet-0223e64311264a87d", "subnet-0dd59d6f622945c0e", "subnet-092ae77daa0fceecb"},
					AssignPublicIp: types.AssignPublicIpEnabled,
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
