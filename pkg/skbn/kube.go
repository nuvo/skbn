package skbn

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/nuvo/skbn/pkg/utils"

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
	command := []string{"find", findPath, "-type", findType, "-name", findName}

	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++

		output, stderr, err := Exec(client, namespace, podName, containerName, command, nil)
		shouldContinue, err := checkerr(stderr, attempt, attempts, err)
		if shouldContinue {
			continue
		}
		if err != nil {
			return nil, err
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
	command := []string{"cat", pathToCopy}

	attempts := 3
	attempt := 0
	for attempt < attempts {
		attempt++

		stdout, stderr, err := Exec(client, namespace, podName, containerName, command, nil)
		shouldContinue, err := checkerr(stderr, attempt, attempts, err)
		if shouldContinue {
			continue
		}
		if err != nil {
			return nil, err
		}
		return stdout, nil
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
		command := []string{"mkdir", "-p", dir}
		_, stderr, err := Exec(client, namespace, podName, containerName, command, nil)
		shouldContinue, err := checkerr(stderr, attempt, attempts, err)
		if shouldContinue {
			continue
		}
		if err != nil {
			return err
		}

		command = []string{"touch", pathToCopy}
		_, stderr, err = Exec(client, namespace, podName, containerName, command, nil)
		shouldContinue, err = checkerr(stderr, attempt, attempts, err)
		if shouldContinue {
			continue
		}
		if err != nil {
			return err
		}

		command = []string{"cp", "/dev/stdin", pathToCopy}
		stdin := bytes.NewReader(buffer)
		_, stderr, err = Exec(client, namespace, podName, containerName, command, readerWrapper{stdin})
		shouldContinue, err = checkerr(stderr, attempt, attempts, err)
		if shouldContinue {
			continue
		}
		if err != nil {
			return err
		}
		return nil
	}

	return nil
}

// Exec executes a command in a given container
func Exec(client K8sClient, namespace, podName, containerName string, command []string, stdin io.Reader) ([]byte, []byte, error) {
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
		Command:   command,
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

func checkerr(stderr []byte, attempt, attempts int, err error) (bool, error) {
	if len(stderr) != 0 {
		if attempt == attempts {
			return false, fmt.Errorf("STDERR: " + (string)(stderr))
		}
		utils.Sleep(attempt)
		return true, nil
	}
	if err != nil {
		if attempt == attempts {
			return false, err
		}
		utils.Sleep(attempt)
		return true, nil
	}
	return false, nil
}

type readerWrapper struct {
	reader io.Reader
}

func (r readerWrapper) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}
