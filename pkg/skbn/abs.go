package skbn

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

var err error

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

	if len(accountName) == 0 || len(accountKey) == 0 {
		log.Fatal("Either the AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCESS_KEY environment variable is not set")
	}

	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)

	if err != nil {
		log.Fatal("Invalid credentials with error: " + err.Error())
	}
	pl := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	return pl, err
}

func getServiceURL(pl pipeline.Pipeline, accountName string) (azblob.ServiceURL, error) {
	URL, err := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/", accountName))

	surl := azblob.NewServiceURL(*URL, pl)
	return surl, err
}

func getContainerURL(pl pipeline.Pipeline, accountName string, containerName string) (azblob.ContainerURL, error) {
	URL, err := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))

	curl := azblob.NewContainerURL(*URL, pl)
	return curl, err
}

func getBlobURL(curl azblob.ContainerURL, blob string) (azblob.BlockBlobURL, error) {
	return curl.NewBlockBlobURL(blob), err
}

func createContainer(ctx context.Context, pl pipeline.Pipeline, curl azblob.ContainerURL) (*azblob.ContainerCreateResponse, error) {
	cr, err := curl.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	return cr, err
}

func listContainers(ctx context.Context, surl azblob.ServiceURL) ([]azblob.ContainerItem, error) {
	lc, err := surl.ListContainersSegment(ctx, azblob.Marker{}, azblob.ListContainersSegmentOptions{})
	return lc.ContainerItems, err
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

// GetClientToAbs checks the connection to azure blob storage and returns the tested client (pipeline)
func GetClientToAbs(ctx context.Context, path string) (pipeline.Pipeline, error) {
	pSplit := strings.Split(path, "/")
	a, c, _ := initAbsVariables(pSplit)
	pl, err := getNewPipeline()
	su, err := getServiceURL(pl, a)
	lc, err := listContainers(ctx, su)
	cu, err := getContainerURL(pl, a, c)

	if !containerExists(lc, c) {
		_, err := createContainer(ctx, pl, cu)

		if err != nil {
			return nil, err
		}
	}

	return pl, err
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
	bl := []string{}

	for marker := (azblob.Marker{}); marker.NotDone(); {
		listBlob, err := cu.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})

		if err != nil {
			return nil, err
		}

		marker = listBlob.NextMarker

		for _, blobInfo := range listBlob.Segment.BlobItems {
			bl = append(bl, strings.Replace(blobInfo.Name, p, "", 1))
		}
	}

	return bl, err
}

// DownloadFromAbs downloads a single file from azure blob storage
func DownloadFromAbs(ctx context.Context, iClient interface{}, path string) ([]byte, error) {
	pSplit := strings.Split(path, "/")

	if err := validateAbsPath(pSplit); err != nil {
		return nil, err
	}

	a, c, p := initAbsVariables(pSplit)
	pl := iClient.(pipeline.Pipeline)
	cu, err := getContainerURL(pl, a, c)
	bu, err := getBlobURL(cu, p)
	dr, err := bu.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	bs := dr.Body(azblob.RetryReaderOptions{MaxRetryRequests: 20})
	dd := bytes.Buffer{}
	_, err = dd.ReadFrom(bs)

	return dd.Bytes(), err
}

// UploadToAbs uploads a single file to azure blob storage
func UploadToAbs(ctx context.Context, iClient interface{}, toPath, fromPath string, buffer []byte) error {
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
	bu, err := getBlobURL(cu, p)

	_, err = azblob.UploadBufferToBlockBlob(ctx, buffer, bu, azblob.UploadToBlockBlobOptions{
		BlockSize:   4 * 1024 * 1024,
		Parallelism: 16})

	return err
}
