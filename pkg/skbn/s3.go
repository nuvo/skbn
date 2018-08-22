package skbn

import (
	"bytes"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// UploadToS3 uploads a single file to S3
func UploadToS3(region, bucket, path string, buffer []byte) error {
	s, err := getNewSession(region)
	if err != nil {
		return err
	}

	_, err = s3.New(s).PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
		Body:   bytes.NewReader(buffer),
	})

	return err
}

// DownloadFromS3 downloads a single file from S3
func DownloadFromS3(region, bucket, path string) ([]byte, error) {
	s, err := getNewSession(region)
	if err != nil {
		return nil, err
	}

	objectOutput, err := s3.New(s).GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
	})

	buffer := make([]byte, int(*objectOutput.ContentLength))
	objectOutput.Body.Read(buffer)

	return buffer, err
}

func getNewSession(region string) (*session.Session, error) {
	s, err := session.NewSession(&aws.Config{Region: aws.String(region)})

	return s, err
}
