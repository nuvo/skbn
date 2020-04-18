package skbn

import (
	"cloud.google.com/go/storage"
	"github.com/nuvo/skbn/pkg/utils"
	"golang.org/x/net/context"
	"log"
	"path/filepath"
	"strings"
)

// GetClientToGcs checks the connection to GCS and returns the tested client
func GetClientToGcs(ctx context.Context, path string) (*storage.Client, error) {
	pSplit := strings.Split(path, "/")
	bucketName, _ := initGcsVariables(pSplit)
	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++

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

func initGcsVariables(split []string) (string, string) {
	bucket := split[0]
	path := filepath.Join(split[1:]...)

	return bucket, path
}
