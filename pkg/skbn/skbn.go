package skbn

import (
	"fmt"
	"log"
	"math"
	"path/filepath"

	"github.com/maorfr/skbn/pkg/utils"
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
	default:
		return fmt.Errorf(srcPrefix + " not implemented")
	}

	switch dstPrefix {
	case "k8s":
	case "s3":
	default:
		return fmt.Errorf(dstPrefix + " not implemented")
	}

	return nil
}

// GetClients gets the clients for the source and destination
func GetClients(srcPrefix, dstPrefix, srcPath, dstPath string) (interface{}, interface{}, error) {
	var srcClient interface{}
	var dstClient interface{}

	k8sTested := false
	s3Tested := false

	switch srcPrefix {
	case "k8s":
		client, err := GetClientToK8s()
		if err != nil {
			return nil, nil, err
		}
		srcClient = client
		k8sTested = true
	case "s3":
		client, err := GetClientToS3(srcPath)
		if err != nil {
			return nil, nil, err
		}
		srcClient = client
		s3Tested = true
	default:
		return nil, nil, fmt.Errorf(srcPrefix + " not implemented")
	}

	switch dstPrefix {
	case "k8s":
		if k8sTested {
			dstClient = srcClient
			break
		}
		client, err := GetClientToK8s()
		if err != nil {
			return nil, nil, err
		}
		dstClient = client
	case "s3":
		if s3Tested {
			dstClient = srcClient
			break
		}
		client, err := GetClientToS3(dstPath)
		if err != nil {
			return nil, nil, err
		}
		dstClient = client
	default:
		return nil, nil, fmt.Errorf(srcPrefix + " not implemented")
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
	default:
		return nil, fmt.Errorf(prefix + " not implemented")
	}

	return relativePaths, nil
}

// Download downloads downloads a single file from path and returns a byte array
func Download(srcClient interface{}, srcPrefix, srcPath string) ([]byte, error) {
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
	default:
		return nil, fmt.Errorf(srcPrefix + " not implemented")
	}

	return buffer, nil
}

// Upload uploads a single file provided as a byte array to path
func Upload(dstClient interface{}, dstPrefix, dstPath, srcPath string, buffer []byte) error {
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
	default:
		return fmt.Errorf(dstPrefix + " not implemented")
	}
	return nil
}
