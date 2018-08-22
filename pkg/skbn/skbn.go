package skbn

import (
	"fmt"
	"log"
	"skbn/pkg/utils"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws/session"
)

// Copy copies files from src to dst
func Copy(src, dst string) error {
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
	err = PerformCopy(srcClient, dstClient, srcPrefix, srcPath, dstPrefix, dstPath)
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
func PerformCopy(srcClient, dstClient interface{}, srcPrefix, srcPath, dstPrefix, dstPath string) error {

	relativePaths, err := getRelativePaths(srcClient, srcPrefix, srcPath)
	if err != nil {
		return err
	}

	// Execute in parallel
	var wg sync.WaitGroup
	for _, relativePath := range relativePaths {

		if relativePath == "" {
			continue
		}
		wg.Add(1)

		go func(srcClient, dstClient interface{}, srcPrefix, srcPath, dstPrefix, dstPath, relativePath string) error {
			defer wg.Done()

			fromPath := srcPath + relativePath
			buffer, err := download(srcClient, srcPrefix, fromPath)
			if err != nil {
				return err
			}
			log.Println("src: " + fromPath)

			toPath := dstPath + strings.Replace(relativePath, srcPath, "", 0)
			err = upload(dstClient, dstPrefix, toPath, buffer)
			if err != nil {
				return err
			}
			log.Println("dst: " + toPath)

			return nil

		}(srcClient, dstClient, srcPrefix, srcPath, dstPrefix, dstPath, relativePath)
	}
	wg.Wait()
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
		return nil, fmt.Errorf(prefix + " not implemented")
	default:
		return nil, fmt.Errorf(prefix + " not implemented")
	}

	return relativePaths, nil
}

func download(client interface{}, prefix, path string) ([]byte, error) {
	var buffer []byte

	switch prefix {
	case "k8s":
		awsProfile, awsSdkLoadConfig := toggleAWSVars("", "")
		bytes, err := DownloadFromK8s(*client.(*k8sClient), path)
		_, _ = toggleAWSVars(awsProfile, awsSdkLoadConfig)

		if err != nil {
			return nil, err
		}
		buffer = bytes
	case "s3":
		bytes, err := DownloadFromS3(client.(*session.Session), path)
		if err != nil {
			return nil, err
		}
		buffer = bytes
	default:
		return nil, fmt.Errorf(prefix + " not implemented")
	}

	return buffer, nil
}

func upload(client interface{}, prefix, path string, buffer []byte) error {
	switch prefix {
	case "k8s":
		return fmt.Errorf("UploadToK8s not implemented")
	case "s3":
		if err := UploadToS3(client.(*session.Session), path, buffer); err != nil {
			return err
		}
	default:
		return fmt.Errorf(prefix + " not implemented")
	}
	return nil
}
