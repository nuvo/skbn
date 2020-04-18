package skbn

import (
	"cloud.google.com/go/storage"
	"fmt"
	"github.com/nuvo/skbn/pkg/utils"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"io"
	"log"
	"path/filepath"
	"strings"
)

// GetClientToGcs checks the connection to GCS and returns the tested client
// Set environment variable GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
// This environment variable will be automatically picked up by the client
func GetClientToGcs(ctx context.Context, path string) (*storage.Client, error) {
	pSplit := strings.Split(path, "/")
	bucketName, _ := initGcsVariables(pSplit)
	attempts := 4
	for attempt := 1; attempt < attempts; attempt++ {

		client, err := storage.NewClient(ctx)
		if err != nil {
			if attempt == attempts {
				return nil, err
			}
			utils.Sleep(attempt)
			continue
		}

		bucket := client.Bucket(bucketName)
		_, err = bucket.Attrs(ctx)
		if err != nil {
			if attempt == attempts {
				return nil, err
			}
		}
		if err == nil {
			return client, nil
		}

		utils.Sleep(attempt)
	}

	log.Println("Could not get client to GCS")
	return nil, nil
}

// GetListOfFilesFromGcs gets list of files in path from GCS (recursive)
func GetListOfFilesFromGcs(ctx context.Context, iClient interface{}, path string) ([]string, error) {
	client := iClient.(*storage.Client)
	pSplit := strings.Split(path, "/")
	if err := validateGcsPath(pSplit); err != nil {
		return nil, err
	}
	bucketName, gcsPath := initGcsVariables(pSplit)

	var outLines []string
	bucket := client.Bucket(bucketName)
	objectIterator := bucket.Objects(ctx, &storage.Query{Prefix: gcsPath}) // gets all files and directories recursively
	for objectAttrs, err := objectIterator.Next(); err != iterator.Done; objectAttrs, err = objectIterator.Next() {
		if err != nil {
			return nil, err
		}
		fileName := objectAttrs.Name
		if !strings.HasSuffix(fileName, "/") { // don't append directories
			outLines = append(outLines, strings.Replace(fileName, gcsPath, "", 1))
		}
	}

	return outLines, nil
}

// DownloadFromGcs downloads a single file from GCS
func DownloadFromGcs(ctx context.Context, iClient interface{}, path string, writer io.Writer) error {
	client := iClient.(*storage.Client)
	pSplit := strings.Split(path, "/")
	if err := validateGcsPath(pSplit); err != nil {
		return err
	}
	bucketName, gcsPath := initGcsVariables(pSplit)

	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++

		if err := copyGcsFileToWriter(ctx, client, bucketName, gcsPath, writer); err != nil {
			if attempt == attempts {
				return err
			}
			utils.Sleep(attempt)
			continue
		}
		return nil
	}

	log.Println("Could not download file from GCS at", path)
	return nil
}

func copyGcsFileToWriter(ctx context.Context, client *storage.Client, bucketName string, path string, writer io.Writer) error {
	reader, err := client.Bucket(bucketName).Object(path).NewReader(ctx)
	defer func() {
		if err := reader.Close(); err != nil {
			log.Println("Error in reader.Close()", err)
		}
	}()

	if err != nil {
		return err
	}

	if _, err := io.Copy(writer, reader); err != nil {
		return err
	}

	return nil
}

func validateGcsPath(pathSplit []string) error {
	if len(pathSplit) >= 1 {
		return nil
	}
	return fmt.Errorf("illegal path: %s", filepath.Join(pathSplit...))
}

func initGcsVariables(split []string) (string, string) {
	bucket := split[0]
	path := filepath.Join(split[1:]...)

	return bucket, path
}
