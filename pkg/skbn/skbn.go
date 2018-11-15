package skbn

import (
	"context"
	"fmt"
	"log"
	"math"
	"path/filepath"

	"github.com/nuvo/skbn/pkg/utils"
)

// FromToPair is a pair of FromPath and ToPath
type FromToPair struct {
	FromPath string
	ToPath   string
}

// Copy copies files from src to dst
func Copy(src, dst string, parallel int) error {
	srcPrefix, srcPath := utils.SplitInTwo(src, "://")
	dstPrefix, dstPath := utils.SplitInTwo(dst, "://")

	err := TestImplementationsExist(srcPrefix, dstPrefix)
	if err != nil {
		return err
	}
	srcClient, dstClient, err := GetClients(srcPrefix, dstPrefix, srcPath, dstPath)
	if err != nil {
		return err
	}
	fromToPaths, err := GetFromToPaths(srcClient, srcPrefix, srcPath, dstPath)
	if err != nil {
		return err
	}
	err = PerformCopy(srcClient, dstClient, srcPrefix, dstPrefix, fromToPaths, parallel)
	if err != nil {
		return err
	}

	return nil
}

// TestImplementationsExist checks that implementations exist for the desired action
func TestImplementationsExist(srcPrefix, dstPrefix string) error {
	switch srcPrefix {
	case "k8s":
	case "s3":
	case "abs":
	default:
		return fmt.Errorf(srcPrefix + " not implemented")
	}

	switch dstPrefix {
	case "k8s":
	case "s3":
	case "abs":
	default:
		return fmt.Errorf(dstPrefix + " not implemented")
	}

	return nil
}

// GetClients gets the clients for the source and destination
func GetClients(srcPrefix, dstPrefix, srcPath, dstPath string) (interface{}, interface{}, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srcClient, tested, err := initClient(ctx, nil, srcPrefix, srcPath, "")
	if err != nil {
		return nil, nil, err
	}
	dstClient, _, err := initClient(ctx, srcClient, dstPrefix, dstPath, tested)
	if err != nil {
		return nil, nil, err
	}

	return srcClient, dstClient, nil
}

// GetFromToPaths gets from and to paths to perform the copy on
func GetFromToPaths(srcClient interface{}, srcPrefix, srcPath, dstPath string) ([]FromToPair, error) {
	relativePaths, err := GetListOfFiles(srcClient, srcPrefix, srcPath)
	if err != nil {
		return nil, err
	}

	var fromToPaths []FromToPair
	for _, relativePath := range relativePaths {
		fromPath := filepath.Join(srcPath, relativePath)
		toPath := filepath.Join(dstPath, relativePath)
		fromToPaths = append(fromToPaths, FromToPair{FromPath: fromPath, ToPath: toPath})
	}

	return fromToPaths, nil
}

// PerformCopy performs the actual copy action
func PerformCopy(srcClient, dstClient interface{}, srcPrefix, dstPrefix string, fromToPaths []FromToPair, parallel int) error {

	// Execute in parallel
	totalFiles := len(fromToPaths)
	if parallel == 0 {
		parallel = totalFiles
	}
	bwgSize := int(math.Min(float64(parallel), float64(totalFiles))) // Very stingy :)
	bwg := utils.NewBoundedWaitGroup(bwgSize)
	currentLine := 0
	for _, ftp := range fromToPaths {

		bwg.Add(1)
		currentLine++

		totalDigits := utils.CountDigits(totalFiles)
		currentLinePadded := utils.LeftPad2Len(currentLine, 0, totalDigits)

		go func(srcClient, dstClient interface{}, srcPrefix, fromPath, dstPrefix, toPath, currentLinePadded string, totalFiles int) {
			buffer, err := Download(srcClient, srcPrefix, fromPath)
			if err != nil {
				bwg.Done()
				log.Fatal(err, fmt.Sprintf(" src: file: %s", fromPath))
				return
			}
			log.Println(fmt.Sprintf("file [%s/%d] src: %s", currentLinePadded, totalFiles, fromPath))

			err = Upload(dstClient, dstPrefix, toPath, fromPath, buffer)
			if err != nil {
				bwg.Done()
				log.Fatal(err, fmt.Sprintf(" dst: file: %s", toPath))
				return
			}
			log.Println(fmt.Sprintf("file [%s/%d] dst: %s", currentLinePadded, totalFiles, toPath))

			bwg.Done()
		}(srcClient, dstClient, srcPrefix, ftp.FromPath, dstPrefix, ftp.ToPath, currentLinePadded, totalFiles)
	}
	bwg.Wait()
	return nil
}

// GetListOfFiles gets relative paths from the provided path
func GetListOfFiles(client interface{}, prefix, path string) ([]string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var relativePaths []string

	switch prefix {
	case "k8s":
		paths, err := GetListOfFilesFromK8s(client, path, "f", "*")
		if err != nil {
			return nil, err
		}
		relativePaths = paths
	case "s3":
		paths, err := GetListOfFilesFromS3(client, path)
		if err != nil {
			return nil, err
		}
		relativePaths = paths
	case "abs":
		paths, err := GetListOfFilesFromAbs(ctx, client, path)
		if err != nil {
			return nil, err
		}
		relativePaths = paths
	default:
		return nil, fmt.Errorf(prefix + " not implemented")
	}

	return relativePaths, nil
}

// Download downloads downloads a single file from path and returns a byte array
func Download(srcClient interface{}, srcPrefix, srcPath string) ([]byte, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var buffer []byte

	switch srcPrefix {
	case "k8s":
		bytes, err := DownloadFromK8s(srcClient, srcPath)
		if err != nil {
			return nil, err
		}
		buffer = bytes
	case "s3":
		bytes, err := DownloadFromS3(srcClient, srcPath)
		if err != nil {
			return nil, err
		}
		buffer = bytes
	case "abs":
		bytes, err := DownloadFromAbs(ctx, srcClient, srcPath)
		if err != nil {
			return nil, err
		}
		buffer = bytes
	default:
		return nil, fmt.Errorf(srcPrefix + " not implemented")
	}

	return buffer, nil
}

// Upload uploads a single file provided as a byte array to path
func Upload(dstClient interface{}, dstPrefix, dstPath, srcPath string, buffer []byte) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	switch dstPrefix {
	case "k8s":
		err := UploadToK8s(dstClient, dstPath, srcPath, buffer)
		if err != nil {
			return err
		}
	case "s3":
		err := UploadToS3(dstClient, dstPath, srcPath, buffer)
		if err != nil {
			return err
		}
	case "abs":
		err := UploadToAbs(ctx, dstClient, dstPath, srcPath, buffer)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf(dstPrefix + " not implemented")
	}
	return nil
}

func initClient(ctx context.Context, existingClient interface{}, prefix, path, tested string) (interface{}, string, error) {
	var newClient interface{}
	switch prefix {
	case "k8s":
		if isTestedAndClientExists(prefix, tested, existingClient) {
			newClient = existingClient
			break
		}
		client, err := GetClientToK8s()
		if err != nil {
			return nil, "", err
		}
		newClient = client

	case "s3":
		if isTestedAndClientExists(prefix, tested, existingClient) {
			newClient = existingClient
			break
		}
		client, err := GetClientToS3(path)
		if err != nil {
			return nil, "", err
		}
		newClient = client

	case "abs":
		if isTestedAndClientExists(prefix, tested, existingClient) {
			newClient = existingClient
			break
		}
		client, err := GetClientToAbs(ctx, path)
		if err != nil {
			return nil, "", err
		}
		newClient = client

	default:
		return nil, "", fmt.Errorf(prefix + " not implemented")
	}

	return newClient, prefix, nil
}

func isTestedAndClientExists(prefix, tested string, client interface{}) bool {
	return prefix == tested && client != nil
}
