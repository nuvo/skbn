package skbn

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"path/filepath"

	"github.com/maorfr/skbn/pkg/utils"

	"github.com/djherbis/buffer"
	"gopkg.in/djherbis/nio.v2"
)

// FromToPair is a pair of FromPath and ToPath
type FromToPair struct {
	FromPath string
	ToPath   string
}

// Copy copies files from src to dst
func Copy(src, dst string, parallel int, bufferSize float64) error {
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
	err = PerformCopy(srcClient, dstClient, srcPrefix, dstPrefix, fromToPaths, parallel, bufferSize)
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
	case "gcs":
	default:
		return fmt.Errorf(srcPrefix + " not implemented")
	}

	switch dstPrefix {
	case "k8s":
	case "s3":
	case "abs":
	case "gcs":
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
func PerformCopy(srcClient, dstClient interface{}, srcPrefix, dstPrefix string, fromToPaths []FromToPair, parallel int, bufferSize float64) error {
	// Execute in parallel
	totalFiles := len(fromToPaths)
	if parallel == 0 {
		parallel = totalFiles
	}
	bwgSize := int(math.Min(float64(parallel), float64(totalFiles))) // Very stingy :)
	bwg := utils.NewBoundedWaitGroup(bwgSize)
	errc := make(chan error, 1)
	currentLine := 0
	for _, ftp := range fromToPaths {

		if len(errc) != 0 {
			break
		}

		bwg.Add(1)
		currentLine++

		totalDigits := utils.CountDigits(totalFiles)
		currentLinePadded := utils.LeftPad2Len(currentLine, 0, totalDigits)

		go func(srcClient, dstClient interface{}, srcPrefix, fromPath, dstPrefix, toPath, currentLinePadded string, totalFiles int) {

			if len(errc) != 0 {
				return
			}

			newBufferSize := (int64)(bufferSize * 1024 * 1024) // may not be super accurate
			buf := buffer.New(newBufferSize)
			pr, pw := nio.Pipe(buf)

			log.Printf("[%s/%d] copy: %s://%s -> %s://%s", currentLinePadded, totalFiles, srcPrefix, fromPath, dstPrefix, toPath)

			go func() {
				defer pw.Close()
				if len(errc) != 0 {
					return
				}
				err := Download(srcClient, srcPrefix, fromPath, pw)
				if err != nil {
					log.Println(err, fmt.Sprintf(" src: file: %s", fromPath))
					errc <- err
				}
			}()

			go func() {
				defer pr.Close()
				defer bwg.Done()
				if len(errc) != 0 {
					return
				}
				defer log.Printf("[%s/%d] done: %s://%s -> %s://%s", currentLinePadded, totalFiles, srcPrefix, fromPath, dstPrefix, toPath)
				err := Upload(dstClient, dstPrefix, toPath, fromPath, pr)
				if err != nil {
					log.Println(err, fmt.Sprintf(" dst: file: %s", toPath))
					errc <- err
				}
			}()
		}(srcClient, dstClient, srcPrefix, ftp.FromPath, dstPrefix, ftp.ToPath, currentLinePadded, totalFiles)
	}
	bwg.Wait()
	if len(errc) != 0 {
		// This is not exactly the correct behavior
		// There may be more than 1 error in the channel
		// But first let's make it work
		err := <-errc
		close(errc)
		if err != nil {
			return err
		}
	}
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
	case "gcs":
		paths, err := GetListOfFilesFromGcs(ctx, client, path)
		if err != nil {
			return nil, err
		}
		relativePaths = paths
	default:
		return nil, fmt.Errorf(prefix + " not implemented")
	}

	return relativePaths, nil
}

// Download downloads a single file from path into an io.Writer
func Download(srcClient interface{}, srcPrefix, srcPath string, writer io.Writer) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	switch srcPrefix {
	case "k8s":
		err := DownloadFromK8s(srcClient, srcPath, writer)
		if err != nil {
			return err
		}
	case "s3":
		err := DownloadFromS3(srcClient, srcPath, writer)
		if err != nil {
			return err
		}
	case "abs":
		err := DownloadFromAbs(ctx, srcClient, srcPath, writer)
		if err != nil {
			return err
		}
	case "gcs":
		err := DownloadFromGcs(ctx, srcClient, srcPath, writer)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf(srcPrefix + " not implemented")
	}

	return nil
}

// Upload uploads a single file provided as an io.Reader array to path
func Upload(dstClient interface{}, dstPrefix, dstPath, srcPath string, reader io.Reader) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	switch dstPrefix {
	case "k8s":
		err := UploadToK8s(dstClient, dstPath, srcPath, reader)
		if err != nil {
			return err
		}
	case "s3":
		err := UploadToS3(dstClient, dstPath, srcPath, reader)
		if err != nil {
			return err
		}
	case "abs":
		err := UploadToAbs(ctx, dstClient, dstPath, srcPath, reader)
		if err != nil {
			return err
		}
	case "gcs":
		err := UploadToGcs(ctx, dstClient, dstPath, srcPath, reader)
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

	case "gcs":
		if isTestedAndClientExists(prefix, tested, existingClient) {
			newClient = existingClient
			break
		}
		client, err := GetClientToGcs(ctx, path)
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
