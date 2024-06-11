package skbn

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

// GetClientToAbs checks the connection to azure blob storage and returns the tested client (pipeline)
func GetClientToAbs(ctx context.Context, path string) (pipeline.Pipeline, error) {
	pSplit := strings.Split(path, "/")
	a, c, _ := initAbsVariables(pSplit)
	pl, err := getNewPipeline()
	if err != nil {
		return nil, err
	}
	su, err := getServiceURL(pl, a)
	if err != nil {
		return nil, err
	}
	lc, err := listContainers(ctx, su)
	if err != nil {
		return nil, err
	}
	if !containerExists(lc, c) {
		return nil, fmt.Errorf("Azure Blob Storage container doesn't exist")
	}

	return pl, nil
}

// GetListOfFilesFromAbs gets list of files in path from azure blob storage (recursive)
func GetListOfFilesFromAbs(ctx context.Context, iClient interface{}, path string) ([]string, error) {
	pSplit := strings.Split(path, "/")
	if err := validateAbsPath(pSplit); err != nil {
		return nil, err
	}
	a, c, p := initAbsVariables(pSplit)
	pl := iClient.(pipeline.Pipeline)
	cu, err := getContainerURL(pl, a, c)
	if err != nil {
		return nil, err
	}

	bl := []string{}
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listBlob, err := cu.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})
		if err != nil {
			return nil, err
		}

		marker = listBlob.NextMarker
		for _, blobInfo := range listBlob.Segment.BlobItems {
			if !strings.Contains(blobInfo.Name, p) {
				continue
			}
			bl = append(bl, strings.Replace(blobInfo.Name, p, "", 1))
		}
	}

	return bl, nil
}

// DownloadFromAbs downloads a single file from azure blob storage
func DownloadFromAbs(ctx context.Context, iClient interface{}, path string, writer io.Writer) error {
	pSplit := strings.Split(path, "/")

	if err := validateAbsPath(pSplit); err != nil {
		return err
	}
	a, c, p := initAbsVariables(pSplit)
	pl := iClient.(pipeline.Pipeline)
	cu, err := getContainerURL(pl, a, c)
	if err != nil {
		return err
	}

	bu := getBlobURL(cu, p)
	dr, err := bu.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return err
	}

	bs := dr.Body(azblob.RetryReaderOptions{MaxRetryRequests: 20})
	defer bs.Close()
	_, err = io.Copy(writer, bs)
	if err != nil {
		return err
	}

	return nil
}

// UploadToAbs uploads a single file to azure blob storage
func UploadToAbs(ctx context.Context, iClient interface{}, toPath, fromPath string, reader io.Reader) error {
	pSplit := strings.Split(toPath, "/")
	if err := validateAbsPath(pSplit); err != nil {
		return err
	}

	if len(pSplit) == 1 {
		_, fn := filepath.Split(fromPath)
		pSplit = append(pSplit, fn)
	}

	a, c, p := initAbsVariables(pSplit)
	pl := iClient.(pipeline.Pipeline)
	cu, err := getContainerURL(pl, a, c)
	if err != nil {
		return err
	}

	bu := getBlobURL(cu, p)

	_, err = azblob.UploadStreamToBlockBlob(ctx, reader, bu, azblob.UploadStreamToBlockBlobOptions{
		BufferSize: 4 * 1024 * 1024,
		MaxBuffers: 16,
	})
	if err != nil {
		return err
	}

	return nil
}

func validateAbsPath(pathSplit []string) error {
	if len(pathSplit) >= 1 {
		return nil
	}
	return fmt.Errorf("illegal path: %s", filepath.Join(pathSplit...))
}

func initAbsVariables(split []string) (string, string, string) {
	account := split[0]
	container := split[1]
	path := filepath.Join(split[2:]...)

	return account, container, path
}

func getNewPipeline() (pipeline.Pipeline, error) {
	accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_ACCESS_KEY")

	if len(accountName) == 0 {
		return nil, fmt.Errorf("AZURE_STORAGE_ACCOUNT environment variable is not set")
	}
	if len(accountKey) == 0 {
		return nil, fmt.Errorf("AZURE_STORAGE_ACCESS_KEY environment variable is not set")
	}

	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, err
	}

	po := azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			Policy:        azblob.RetryPolicyExponential,
			MaxTries:      3,
			TryTimeout:    time.Second * 3,
			RetryDelay:    time.Second * 1,
			MaxRetryDelay: time.Second * 3,
		},
	}

	pl := azblob.NewPipeline(credential, po)

	return pl, nil
}

func getServiceURL(pl pipeline.Pipeline, accountName string) (azblob.ServiceURL, error) {
	URL, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/", accountName))
	if err != nil {
		return azblob.ServiceURL{}, err
	}

	surl := azblob.NewServiceURL(*URL, pl)
	return surl, nil
}

func getContainerURL(pl pipeline.Pipeline, accountName string, containerName string) (azblob.ContainerURL, error) {
	URL, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))
	if err != nil {
		return azblob.ContainerURL{}, err
	}

	curl := azblob.NewContainerURL(*URL, pl)
	return curl, nil
}

func getBlobURL(curl azblob.ContainerURL, blob string) azblob.BlockBlobURL {
	return curl.NewBlockBlobURL(blob)
}

func listContainers(ctx context.Context, surl azblob.ServiceURL) ([]azblob.ContainerItem, error) {
	lc, err := surl.ListContainersSegment(ctx, azblob.Marker{}, azblob.ListContainersSegmentOptions{})
	if err != nil {
		return nil, err
	}

	return lc.ContainerItems, nil
}

func containerExists(list []azblob.ContainerItem, containerName string) bool {
	exists := false
	for _, v := range list {
		if containerName == v.Name {
			exists = true
		}
	}
	return exists
}
