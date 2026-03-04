package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	ctx := context.TODO()
	bucket := os.Getenv("BUCKET")
	key := os.Getenv("KEY")

	cfg, _ := config.LoadDefaultConfig(ctx)
	s3Client := s3.NewFromConfig(cfg)
	presignClient := s3.NewPresignClient(s3Client)

	presignedReq, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(time.Hour))
	if err != nil {
		log.Fatal("failed to sign request:", err)
	}

	args := []string{
		"-i", presignedReq.URL,

		"-vf", "scale=-2:720", "-c:v", "libx264", "vid_720p.mp4",

		"-vf", "scale=-2:480", "-c:v", "libx264", "vid_480p.mp4",

		"-vf", "scale=-2:360", "-c:v", "libx264", "vid_360p.mp4",
	}

	cmd := exec.Command("ffmpeg", args...)
	err = cmd.Run()

	outputFiles := []string{"vid_720p.mp4", "vid_480p.mp4", "vid_360p.mp4"}
	var wg sync.WaitGroup
	destBucket := "s3bucket"

	for _, fileName := range outputFiles {
		wg.Add(1)

		go func(fName string) {
			defer wg.Done()

			file, err := os.Open(fName)
			if err != nil {
				log.Printf("Error opening %s: %v", fName, err)
				return
			}
			defer file.Close()

			_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(destBucket),
				Key:    aws.String(key),
				Body:   file,
			})

			if err != nil {
				log.Printf("Failed to upload %s: %v", fName, err)
			} else {
				fmt.Printf("Successfully uploaded %s\n", fName)
			}
		}(fileName)
	}

	wg.Wait()
	fmt.Println("All uploads complete. Cleaning up...")

	for _, f := range outputFiles {
		os.Remove(f)
	}
}
