package skbn

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// GetClientToS3 checks the connection to S3 and returns the tested client
func GetClientToS3() (*session.Session, error) {
	s, err := getNewSession()
	if err != nil {
		return nil, err
	}

	_, err = s3.New(s).ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}
	return s, nil
}

// GetListOfFilesFromS3 gets list of files in path from S3 (recursive)
func GetListOfFilesFromS3(s *session.Session, path string) ([]string, error) {
	pSplit := strings.Split(path, "/")

	if len(pSplit) < 2 {
		return nil, fmt.Errorf("illegal path")
	}

	bucket := pSplit[0]
	pathToCopy := filepath.Join(pSplit[1:]...)

	objectOutput, err := s3.New(s).ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(pathToCopy),
	})
	if err != nil {
		return nil, err
	}

	var outLines []string
	for _, content := range objectOutput.Contents {
		line := *content.Key
		outLines = append(outLines, strings.Replace(line, pathToCopy, "", 1))
	}

	return outLines, nil
}

// DownloadFromS3 downloads a single file from S3
func DownloadFromS3(s *session.Session, path string) ([]byte, error) {
	pSplit := strings.Split(path, "/")
	bucket := pSplit[0]
	s3Path := filepath.Join(pSplit[1:]...)

	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++
		objectOutput, err := s3.New(s).GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(s3Path),
		})
		if err != nil {
			if attempt == attempts {
				return nil, err
			}
			continue
		}

		buffer := make([]byte, int(*objectOutput.ContentLength))
		_, err = objectOutput.Body.Read(buffer)

		if attempt == attempts && err != nil {
			return nil, err
		}
		if err == nil {
			return buffer, nil
		}
		time.Sleep(1 * time.Second)
	}

	return nil, nil
}

// UploadToS3 uploads a single file to S3
func UploadToS3(s *session.Session, path string, buffer []byte) error {
	pSplit := strings.Split(path, "/")
	bucket := pSplit[0]
	s3Path := filepath.Join(pSplit[1:]...)

	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++
		_, err := s3.New(s).PutObject(&s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(s3Path),
			Body:   bytes.NewReader(buffer),
		})

		if attempt == attempts && err != nil {
			return err
		}
		if err == nil {
			return nil
		}
		time.Sleep(1 * time.Second)
	}

	return nil
}

func getNewSession() (*session.Session, error) {
	region := "eu-central-1" // Not really important for S3
	s, err := session.NewSession(&aws.Config{Region: aws.String(region)})

	return s, err
}
