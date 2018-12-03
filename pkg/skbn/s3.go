package skbn

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/nuvo/skbn/pkg/utils"
	nio "gopkg.in/djherbis/nio.v2"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// GetClientToS3 checks the connection to S3 and returns the tested client
func GetClientToS3(path string) (*session.Session, error) {
	pSplit := strings.Split(path, "/")
	bucket, _ := initS3Variables(pSplit)
	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++

		s, err := getNewSession()
		if err != nil {
			if attempt == attempts {
				return nil, err
			}
			utils.Sleep(attempt)
			continue
		}

		_, err = s3.New(s).ListObjects(&s3.ListObjectsInput{
			Bucket:  aws.String(bucket),
			MaxKeys: aws.Int64(0),
		})
		if attempt == attempts {
			if err != nil {
				return nil, err
			}
		}
		if err == nil {
			return s, nil
		}
		utils.Sleep(attempt)
	}

	return nil, nil
}

// GetListOfFilesFromS3 gets list of files in path from S3 (recursive)
func GetListOfFilesFromS3(iClient interface{}, path string) ([]string, error) {
	s := iClient.(*session.Session)
	pSplit := strings.Split(path, "/")
	if err := validateS3Path(pSplit); err != nil {
		return nil, err
	}
	bucket, s3Path := initS3Variables(pSplit)

	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++

		objectOutput, err := s3.New(s).ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(bucket),
			Prefix: aws.String(s3Path),
		})
		if err != nil {
			if attempt == attempts {
				return nil, err
			}
			utils.Sleep(attempt)
			continue
		}

		var outLines []string
		for _, content := range objectOutput.Contents {
			line := *content.Key
			outLines = append(outLines, strings.Replace(line, s3Path, "", 1))
		}

		return outLines, nil
	}

	return nil, nil
}

// DownloadFromS3 downloads a single file from S3
func DownloadFromS3(iClient interface{}, path string, pw *nio.PipeWriter) error {
	s := iClient.(*session.Session)
	pSplit := strings.Split(path, "/")
	if err := validateS3Path(pSplit); err != nil {
		return err
	}
	bucket, s3Path := initS3Variables(pSplit)

	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++

		downloader := s3manager.NewDownloader(s)
		downloader.Concurrency = 1 // support PipeWriterWrapper

		_, err := downloader.Download(pipeWriterWrapper{pw},
			&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(s3Path),
			})
		if err != nil {
			if attempt == attempts {
				return err
			}
			utils.Sleep(attempt)
			continue
		}
		return nil
	}

	return nil
}

type pipeWriterWrapper struct {
	pw *nio.PipeWriter
}

func (pww pipeWriterWrapper) WriteAt(p []byte, off int64) (n int, err error) {
	return pww.pw.Write(p)
}

// UploadToS3 uploads a single file to S3
func UploadToS3(iClient interface{}, toPath, fromPath string, pr *nio.PipeReader) error {
	s := iClient.(*session.Session)
	pSplit := strings.Split(toPath, "/")
	if err := validateS3Path(pSplit); err != nil {
		return err
	}
	if len(pSplit) == 1 {
		_, fileName := filepath.Split(fromPath)
		pSplit = append(pSplit, fileName)
	}
	bucket, s3Path := initS3Variables(pSplit)

	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++

		uploader := s3manager.NewUploader(s)

		_, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(s3Path),
			Body:   pr,
		})
		if err != nil {
			if attempt == attempts {
				return err
			}
			utils.Sleep(attempt)
			continue
		}
		return nil
	}

	return nil
}

func getNewSession() (*session.Session, error) {
	s, err := session.NewSession(&aws.Config{
		Region: aws.String(os.Getenv("AWS_REGION")),
	})

	return s, err
}

func validateS3Path(pathSplit []string) error {
	if len(pathSplit) >= 1 {
		return nil
	}
	return fmt.Errorf("illegal path: %s", filepath.Join(pathSplit...))
}

func initS3Variables(split []string) (string, string) {
	bucket := split[0]
	path := filepath.Join(split[1:]...)

	return bucket, path
}
