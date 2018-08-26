package skbn

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"skbn/pkg/utils"
	"strings"

	core_v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

// K8sClient holds a clientset and a config
type K8sClient struct {
	clientSet *kubernetes.Clientset
	config    *rest.Config
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
	var client = &K8sClient{clientSet: clientset, config: config}
	return client, nil
}

// GetListOfFilesFromK8s gets list of files in path from Kubernetes (recursive)
func GetListOfFilesFromK8s(client K8sClient, path string) ([]string, error) {
	pSplit := strings.Split(path, "/")
	if err := validateK8sPath(pSplit); err != nil {
		return nil, err
	}
	namespace := pSplit[0]
	podName := pSplit[1]
	containerName := pSplit[2]
	pathToCopy := getAbsPath(pSplit[3:]...)
	command := "find " + pathToCopy + " -type f"

	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++

		output, stderr, err := exec(client, namespace, podName, containerName, command, nil)
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
				outLines = append(outLines, strings.Replace(line, pathToCopy, "", 1))
			}
		}

		return outLines, nil
	}

	return nil, nil
}

// DownloadFromK8s downloads a single file from Kubernetes
func DownloadFromK8s(client K8sClient, path string) ([]byte, error) {
	pSplit := strings.Split(path, "/")
	if err := validateK8sPath(pSplit); err != nil {
		return nil, err
	}
	namespace := pSplit[0]
	podName := pSplit[1]
	containerName := pSplit[2]
	pathToCopy := getAbsPath(pSplit[3:]...)
	command := "cat " + pathToCopy

	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++

		output, stderr, err := exec(client, namespace, podName, containerName, command, nil)
		if attempt == attempts {
			if len(stderr) != 0 {
				return output, fmt.Errorf("STDERR: " + (string)(stderr))
			}
			if err != nil {
				return output, err
			}
		}
		if err == nil {
			return output, nil
		}
		utils.Sleep(attempt)
	}

	return nil, nil
}

// UploadToK8s uploads a single file to Kubernetes
func UploadToK8s(client K8sClient, toPath, fromPath string, buffer []byte) error {
	pSplit := strings.Split(toPath, "/")
	if err := validateK8sPath(pSplit); err != nil {
		return err
	}
	if len(pSplit) == 3 {
		_, fileName := filepath.Split(fromPath)
		pSplit = append(pSplit, fileName)
	}
	namespace := pSplit[0]
	podName := pSplit[1]
	containerName := pSplit[2]
	pathToCopy := getAbsPath(pSplit[3:]...)

	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++
		dir, _ := filepath.Split(pathToCopy)
		command := "mkdir -p " + dir
		_, stderr, err := exec(client, namespace, podName, containerName, command, nil)

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

		command = "cp /dev/stdin " + pathToCopy
		stdin := bytes.NewReader(buffer)
		_, stderr, err = exec(client, namespace, podName, containerName, command, stdin)

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
	}

	return nil
}

func validateK8sPath(pathSplit []string) error {
	if len(pathSplit) >= 3 {
		return nil
	}
	return fmt.Errorf("illegal path: %s", filepath.Join(pathSplit...))
}

func exec(client K8sClient, namespace, podName, containerName, command string, stdin io.Reader) ([]byte, []byte, error) {
	clientset, config := client.clientSet, client.config

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

func getAbsPath(path ...string) string {
	return filepath.Join("/", filepath.Join(path...))
}
