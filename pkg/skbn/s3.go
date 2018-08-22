package skbn

import (
	"bytes"
	"path/filepath"
	"strings"

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

// DownloadFromS3 downloads a single file from S3
func DownloadFromS3(s *session.Session, path string) ([]byte, error) {
	pSplit := strings.Split(path, "/")
	bucket := pSplit[0]
	s3Path := filepath.Join(pSplit[1:]...)

	objectOutput, err := s3.New(s).GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s3Path),
	})

	buffer := make([]byte, int(*objectOutput.ContentLength))
	objectOutput.Body.Read(buffer)

	return buffer, err
}

// UploadToS3 uploads a single file to S3
func UploadToS3(s *session.Session, path string, buffer []byte) error {
	pSplit := strings.Split(path, "/")
	bucket := pSplit[0]
	s3Path := filepath.Join(pSplit[1:]...)

	_, err := s3.New(s).PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s3Path),
		Body:   bytes.NewReader(buffer),
	})

	return err
}

func getNewSession() (*session.Session, error) {
	region := "eu-central-1" // Not really important for S3
	s, err := session.NewSession(&aws.Config{Region: aws.String(region)})

	return s, err
}
