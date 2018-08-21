package c2s3

import (
	"bytes"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// UploadToS3 uploads a single file to S3
func UploadToS3(s3Region, s3Bucket, s3Path string, buffer []byte) error {
	s, err := getNewSession(s3Region)
	if err != nil {
		return err
	}

	_, err = s3.New(s).PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(s3Path),
		Body:   bytes.NewReader(buffer),
	})

	return err
}

// DownloadFromS3 downloads a single file from S3
func DownloadFromS3(s3Region, s3Bucket, s3Path string) ([]byte, error) {
	s, err := getNewSession(s3Region)
	if err != nil {
		return nil, err
	}

	objectOutput, err := s3.New(s).GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(s3Path),
	})

	buffer := make([]byte, int(*objectOutput.ContentLength))
	objectOutput.Body.Read(buffer)

	return buffer, err
}

func getNewSession(s3Region string) (*session.Session, error) {
	s, err := session.NewSession(&aws.Config{Region: aws.String(s3Region)})

	return s, err
}
