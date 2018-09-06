package skbn

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/maorfr/skbn/pkg/utils"

	core_v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

// K8sClient holds a clientset and a config
type K8sClient struct {
	ClientSet *kubernetes.Clientset
	Config    *rest.Config
}

// GetClientToK8s returns a k8sClient
func GetClientToK8s() (*K8sClient, error) {
	var kubeconfig string
	if kubeConfigPath := os.Getenv("KUBECONFIG"); kubeConfigPath != "" {
		kubeconfig = kubeConfigPath // CI process
	} else {
		kubeconfig = filepath.Join(os.Getenv("HOME"), ".kube", "config") // Development environment
	}

	var config *rest.Config

	_, err := os.Stat(kubeconfig)
	if err != nil {
		// In cluster configuration
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
	} else {
		// Out of cluster configuration
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, err
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	var client = &K8sClient{ClientSet: clientset, Config: config}
	return client, nil
}

// GetListOfFilesFromK8s gets list of files in path from Kubernetes (recursive)
func GetListOfFilesFromK8s(iClient interface{}, path, findType, findName string) ([]string, error) {
	client := *iClient.(*K8sClient)
	pSplit := strings.Split(path, "/")
	if err := validateK8sPath(pSplit); err != nil {
		return nil, err
	}
	namespace, podName, containerName, findPath := initK8sVariables(pSplit)
	command := fmt.Sprintf("find %s -type %s -name %s", findPath, findType, findName)

	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++

		output, stderr, err := Exec(client, namespace, podName, containerName, command, nil)
		if len(stderr) != 0 {
			if attempt == attempts {
				return nil, fmt.Errorf("STDERR: " + (string)(stderr))
			}
			utils.Sleep(attempt)
			continue
		}
		if err != nil {
			if attempt == attempts {
				return nil, err
			}
			utils.Sleep(attempt)
			continue
		}

		lines := strings.Split((string)(output), "\n")
		var outLines []string
		for _, line := range lines {
			if line != "" {
				outLines = append(outLines, strings.Replace(line, findPath, "", 1))
			}
		}

		return outLines, nil
	}

	return nil, nil
}

// DownloadFromK8s downloads a single file from Kubernetes
func DownloadFromK8s(iClient interface{}, path string) ([]byte, error) {
	client := *iClient.(*K8sClient)
	pSplit := strings.Split(path, "/")
	if err := validateK8sPath(pSplit); err != nil {
		return nil, err
	}
	namespace, podName, containerName, pathToCopy := initK8sVariables(pSplit)
	command := fmt.Sprintf("cat %s", pathToCopy)

	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++

		stdout, stderr, err := Exec(client, namespace, podName, containerName, command, nil)
		if attempt == attempts {
			if len(stderr) != 0 {
				return stdout, fmt.Errorf("STDERR: " + (string)(stderr))
			}
			if err != nil {
				return stdout, err
			}
		}
		if err == nil {
			return stdout, nil
		}
		utils.Sleep(attempt)
	}

	return nil, nil
}

// UploadToK8s uploads a single file to Kubernetes
func UploadToK8s(iClient interface{}, toPath, fromPath string, buffer []byte) error {
	client := *iClient.(*K8sClient)
	pSplit := strings.Split(toPath, "/")
	if err := validateK8sPath(pSplit); err != nil {
		return err
	}
	if len(pSplit) == 3 {
		_, fileName := filepath.Split(fromPath)
		pSplit = append(pSplit, fileName)
	}
	namespace, podName, containerName, pathToCopy := initK8sVariables(pSplit)

	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++
		dir, _ := filepath.Split(pathToCopy)
		command := fmt.Sprintf("mkdir -p %s", dir)
		_, stderr, err := Exec(client, namespace, podName, containerName, command, nil)

		if len(stderr) != 0 {
			if attempt == attempts {
				return fmt.Errorf("STDERR: " + (string)(stderr))
			}
			utils.Sleep(attempt)
			continue
		}
		if err != nil {
			if attempt == attempts {
				return err
			}
			utils.Sleep(attempt)
			continue
		}

		chunkSize := 16777215

		command = fmt.Sprintf("dd if=/dev/stdin of=%s", pathToCopy)
		for i := 0; i < len(buffer); i += chunkSize {
			end := i + chunkSize
			if end > len(buffer) {
				end = len(buffer)
			}
			stdin := bytes.NewReader(buffer[i:end])
			_, _, err = Exec(client, namespace, podName, containerName, command, stdin)
			command = fmt.Sprintf("dd if=/dev/stdin of=%s oflag=append conv=notrunc", pathToCopy)

			// if len(stderr) != 0 {
			// 	if attempt == attempts {
			// 		return fmt.Errorf("STDERR: " + (string)(stderr))
			// 	}
			// 	utils.Sleep(attempt)
			// 	continue
			// }
			if err != nil {
				if attempt == attempts {
					return err
				}
				utils.Sleep(attempt)
				continue
			}
		}
		return nil
	}

	return nil
}

// Exec executes a command in a given container
func Exec(client K8sClient, namespace, podName, containerName, command string, stdin io.Reader) ([]byte, []byte, error) {
	clientset, config := client.ClientSet, client.Config

	req := clientset.Core().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec")
	scheme := runtime.NewScheme()
	if err := core_v1.AddToScheme(scheme); err != nil {
		return nil, nil, fmt.Errorf("error adding to scheme: %v", err)
	}

	parameterCodec := runtime.NewParameterCodec(scheme)
	req.VersionedParams(&core_v1.PodExecOptions{
		Command:   strings.Fields(command),
		Container: containerName,
		Stdin:     stdin != nil,
		Stdout:    true,
		Stderr:    true,
		TTY:       false,
	}, parameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return nil, nil, fmt.Errorf("error while creating Executor: %v", err)
	}

	var stdout, stderr bytes.Buffer
	err = exec.Stream(remotecommand.StreamOptions{
		Stdin:  stdin,
		Stdout: &stdout,
		Stderr: &stderr,
		Tty:    false,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("error in Stream: %v", err)
	}

	return stdout.Bytes(), stderr.Bytes(), nil
}

func validateK8sPath(pathSplit []string) error {
	if len(pathSplit) >= 3 {
		return nil
	}
	return fmt.Errorf("illegal path: %s", filepath.Join(pathSplit...))
}

func initK8sVariables(split []string) (string, string, string, string) {
	namespace := split[0]
	pod := split[1]
	container := split[2]
	path := getAbsPath(split[3:]...)

	return namespace, pod, container, path
}

func getAbsPath(path ...string) string {
	return filepath.Join("/", filepath.Join(path...))
}
