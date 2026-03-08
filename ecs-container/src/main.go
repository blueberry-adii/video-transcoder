package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
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
		"-threads", "1",
		"-filter_complex", "[0:v]split=3[v1][v2][v3]; [v1]scale=-2:720,setsar=1[v720]; [v2]scale=-2:480,setsar=1[v480]; [v3]scale=-2:360,setsar=1[v360]",
		"-map", "[v720]", "-map", "a:0",
		"-map", "[v480]", "-map", "a:0",
		"-map", "[v360]", "-map", "a:0",
		"-c:v", "libx264", "-preset", "ultrafast",
		"-f", "hls",
		"-hls_time", "6",
		"-hls_playlist_type", "vod",
		"-master_pl_name", "master.m3u8",
		"-var_stream_map", "v:0,a:0,name:720p v:1,a:1,name:480p v:2,a:2,name:360p",
		"stream_%v.m3u8",
	}

	cmd := exec.Command("ffmpeg", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err != nil {
		log.Fatalf("FFmpeg failed: %v\nStderr: %s", err, stderr.String())
	}
	log.Println("FFmpeg finished successfully. Starting uploads...")

	destBucket := "result-blueberry"
	videoID := key[7 : len(key)-4]
	s3Folder := fmt.Sprintf("processed/%s", videoID)
	files, err := os.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	for _, f := range files {
		if f.IsDir() || (!strings.HasSuffix(f.Name(), ".m3u8") && !strings.HasSuffix(f.Name(), ".ts")) {
			continue
		}

		wg.Add(1)
		go func(fileName string) {
			defer wg.Done()

			file, err := os.Open(fileName)
			if err != nil {
				log.Printf("Error opening %s: %v", fileName, err)
				return
			}
			defer file.Close()

			var contentType string
			if strings.HasSuffix(fileName, ".m3u8") {
				contentType = "application/x-mpegURL"
				if fileName == "master.m3u8" {
					content, _ := os.ReadFile(fileName)
					newContent := strings.ReplaceAll(string(content), "RESOLUTION=1280x720", "RESOLUTION=1280x720,NAME=\"720p\"")
					newContent = strings.ReplaceAll(newContent, "RESOLUTION=854x480", "RESOLUTION=854x480,NAME=\"480p\"")
					newContent = strings.ReplaceAll(newContent, "RESOLUTION=640x360", "RESOLUTION=640x360,NAME=\"360p\"")
					os.WriteFile(fileName, []byte(newContent), 0644)
				}
			} else if strings.HasSuffix(fileName, ".ts") {
				contentType = "video/MP2T"
			}

			_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      aws.String(destBucket),
				Key:         aws.String(fmt.Sprintf("%s/%s", s3Folder, fileName)),
				Body:        file,
				ContentType: aws.String(contentType),
			})

			if err != nil {
				log.Printf("Failed to upload %s: %v", fileName, err)
			} else {
				os.Remove(fileName)
			}
		}(f.Name())
	}

	wg.Wait()

	_, err = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Printf("failed to delete source object %s: %v", key, err)
	}

	fmt.Println("All uploads and cleanup complete.")
}
