package skbn

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

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

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}

	// create the clientset
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
	if len(pSplit) < 4 {
		return nil, fmt.Errorf("illegal path")
	}
	namespace := pSplit[0]
	podName := pSplit[1]
	containerName := pSplit[2]
	pathToCopy := filepath.Join(pSplit[3:]...)
	command := "find /" + pathToCopy + " -type f"

	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++

		output, stderr, err := exec(client, namespace, podName, containerName, command, nil)
		if len(stderr) != 0 {
			if attempt == attempts {
				return nil, fmt.Errorf("STDERR: " + (string)(stderr))
			}
			time.Sleep(1 * time.Second)
			continue
		}
		if err != nil {
			if attempt == attempts {
				return nil, err
			}
			time.Sleep(1 * time.Second)
			continue
		}

		lines := strings.Split((string)(output), "\n")
		var outLines []string
		for _, line := range lines {
			if line != "" {
				outLines = append(outLines, strings.Replace(line, "/"+pathToCopy, "", 1))
			}
		}

		return outLines, nil
	}

	return nil, nil
}

// DownloadFromK8s downloads a single file from Kubernetes
func DownloadFromK8s(client K8sClient, path string) ([]byte, error) {
	pSplit := strings.Split(path, "/")
	if len(pSplit) < 4 {
		return nil, fmt.Errorf("illegal path")
	}
	namespace := pSplit[0]
	podName := pSplit[1]
	containerName := pSplit[2]
	pathToCopy := filepath.Join(pSplit[3:]...)
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
		time.Sleep(1 * time.Second)
	}

	return nil, nil
}

// UploadToK8s uploads a single file to Kubernetes
func UploadToK8s(client K8sClient, path string, buffer []byte) error {
	pSplit := strings.Split(path, "/")
	if len(pSplit) < 4 {
		return fmt.Errorf("illegal path")
	}
	namespace := pSplit[0]
	podName := pSplit[1]
	containerName := pSplit[2]
	pathToCopy := filepath.Join(pSplit[3:]...)

	// TODO: mkdir

	// lines := strings.Split((string)(buffer), "\n")
	// for _, line := range lines {
	err := ioutil.WriteFile("/tmp/dat1", buffer, 0644)

	command := "sh -c \"dd of=/tmp/" + pathToCopy + " < /tmp/dat1\""
	fmt.Println(command)
	_, stderr, err := exec(client, namespace, podName, containerName, command, nil)

	if len(stderr) != 0 {
		return fmt.Errorf("STDERR: " + (string)(stderr))
	}
	if err != nil {
		return err
	}
	// }

	return nil
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
