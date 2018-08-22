package skbn

import (
	"fmt"
	"log"
	"math"
	"skbn/pkg/utils"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
)

// Copy copies files from src to dst
func Copy(src, dst string, parallel int) error {
	srcPrefix, srcPath := utils.SplitInTwo(src, "://")
	dstPrefix, dstPath := utils.SplitInTwo(dst, "://")

	err := TestImplementationsExist(srcPrefix, dstPrefix)
	if err != nil {
		return err
	}
	srcClient, dstClient, err := GetClients(srcPrefix, dstPrefix)
	if err != nil {
		return err
	}
	err = PerformCopy(srcClient, dstClient, srcPrefix, srcPath, dstPrefix, dstPath, parallel)
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
		return fmt.Errorf("DownloadFrom" + strings.Title(srcPrefix) + " not implemented")
	}

	switch dstPrefix {
	case "k8s":
		return fmt.Errorf("UploadToK8s not implemented")
	case "s3":
	default:
		return fmt.Errorf("UploadTo" + strings.Title(dstPrefix) + " not implemented")
	}

	return nil
}

// GetClients gets the clients for the source and destination
func GetClients(srcPrefix, dstPrefix string) (interface{}, interface{}, error) {
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
		client, err := GetClientToS3()
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
		client, err := GetClientToS3()
		if err != nil {
			return nil, nil, err
		}
		dstClient = client
	default:
		return nil, nil, fmt.Errorf(srcPrefix + " not implemented")
	}

	return srcClient, dstClient, nil
}

// PerformCopy performs the actual copy action
func PerformCopy(srcClient, dstClient interface{}, srcPrefix, srcPath, dstPrefix, dstPath string, parallel int) error {

	relativePaths, err := getRelativePaths(srcClient, srcPrefix, srcPath)
	if err != nil {
		return err
	}

	// Execute in parallel
	totalFiles := len(relativePaths)
	if parallel == 0 {
		parallel = totalFiles
	}
	bwgSize := int(math.Min(float64(parallel), float64(totalFiles))) // Very stingy :)
	bwg := utils.NewBoundedWaitGroup(bwgSize)
	var chans []chan error

	i := 0
	for i < bwgSize {
		chans = append(chans, make(chan error))
		i++
	}
	currentLine := 0
	for _, relativePath := range relativePaths {

		bwg.Add(1)
		currentLine++

		totalDigits := utils.CountDigits(totalFiles)
		currentLinePadded := utils.LeftPad2Len(currentLine, 0, totalDigits)

		go func(srcClient, dstClient interface{}, srcPrefix, srcPath, dstPrefix, dstPath, relativePath, currentLinePadded string, totalFiles int) {

			fromPath := srcPath + relativePath
			buffer, err := download(srcClient, srcPrefix, fromPath)
			if err != nil {
				log.Fatal(err)
				bwg.Done()
				return
			}
			log.Println(fmt.Sprintf("file [%s/%d] src: %s", currentLinePadded, totalFiles, fromPath))

			toPath := dstPath + relativePath
			err = upload(dstClient, dstPrefix, toPath, buffer)
			if err != nil {
				log.Fatal(err)
				bwg.Done()
				return
			}
			log.Println(fmt.Sprintf("file [%s/%d] dst: %s", currentLinePadded, totalFiles, toPath))

			bwg.Done()
		}(srcClient, dstClient, srcPrefix, srcPath, dstPrefix, dstPath, relativePath, currentLinePadded, totalFiles)
	}
	bwg.Wait()
	return nil
}

// toggleAWSVars handles the use of heptio-authenticator-aws alongside kubectl
func toggleAWSVars(awsProfile, awsSdkLoadConfig string) (string, string) {
	oldAWSProfile := utils.ToggleEnvVar("AWS_PROFILE", awsProfile)
	oldAWSSdkLoadConfig := utils.ToggleEnvVar("AWS_SDK_LOAD_CONFIG", awsSdkLoadConfig)

	return oldAWSProfile, oldAWSSdkLoadConfig
}

func getRelativePaths(client interface{}, prefix, path string) ([]string, error) {
	var relativePaths []string

	switch prefix {
	case "k8s":
		awsProfile, awsSdkLoadConfig := toggleAWSVars("", "")
		paths, err := GetListOfFilesFromK8s(*client.(*k8sClient), path)
		_, _ = toggleAWSVars(awsProfile, awsSdkLoadConfig)
		if err != nil {
			return nil, err
		}
		relativePaths = paths
	case "s3":
		paths, err := GetListOfFilesFromS3(client.(*session.Session), path)
		if err != nil {
			return nil, err
		}
		relativePaths = paths
	default:
		return nil, fmt.Errorf(prefix + " not implemented")
	}

	return relativePaths, nil
}

func download(srcClient interface{}, srcPrefix, srcPath string) ([]byte, error) {
	var buffer []byte

	switch srcPrefix {
	case "k8s":
		awsProfile, awsSdkLoadConfig := toggleAWSVars("", "")
		bytes, err := DownloadFromK8s(*srcClient.(*k8sClient), srcPath)
		_, _ = toggleAWSVars(awsProfile, awsSdkLoadConfig)

		if err != nil {
			return nil, err
		}
		buffer = bytes
	case "s3":
		bytes, err := DownloadFromS3(srcClient.(*session.Session), srcPath)
		if err != nil {
			return nil, err
		}
		buffer = bytes
	default:
		return nil, fmt.Errorf(srcPrefix + " not implemented")
	}

	return buffer, nil
}

func upload(dstClient interface{}, dstPrefix, dstPath string, buffer []byte) error {
	switch dstPrefix {
	case "k8s":
		return fmt.Errorf("UploadToK8s not implemented")
	case "s3":
		if err := UploadToS3(dstClient.(*session.Session), dstPath, buffer); err != nil {
			return err
		}
	default:
		return fmt.Errorf(dstPrefix + " not implemented")
	}
	return nil
}
