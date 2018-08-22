package skbn

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	core_v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

type k8sClient struct {
	clientSet *kubernetes.Clientset
	config    *rest.Config
}

func GetClientToK8s() (*k8sClient, error) {
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

	var client = &k8sClient{clientSet: clientset, config: config}
	return client, nil
}

func GetListOfFilesFromK8s(client k8sClient, path string) ([]string, error) {
	pSplit := strings.Split(path, "/")

	if len(pSplit) < 4 {
		return nil, fmt.Errorf("illegal path")
	}

	namespace := pSplit[0]
	podName := pSplit[1]
	containerName := pSplit[2]
	pathToCopy := filepath.Join(pSplit[3:]...)

	command := "find /" + pathToCopy + " -type f"

	output, stderr, err := exec(client, namespace, podName, containerName, command, nil)

	if len(stderr) != 0 {
		return nil, fmt.Errorf("STDERR: " + (string)(stderr))
	}
	if err != nil {
		return nil, err
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

func DownloadFromK8s(client k8sClient, path string) ([]byte, error) {
	pSplit := strings.Split(path, "/")

	if len(pSplit) < 4 {
		return nil, fmt.Errorf("illegal path")
	}

	namespace := pSplit[0]
	podName := pSplit[1]
	containerName := pSplit[2]
	pathToCopy := filepath.Join(pSplit[3:]...)
	command := "cat " + pathToCopy

	output, stderr, err := exec(client, namespace, podName, containerName, command, nil)

	if len(stderr) != 0 {
		return output, fmt.Errorf("STDERR: " + (string)(stderr))
	}
	if err != nil {
		return output, err
	}

	return output, nil
}

func UploadToK8s(client k8sClient, path string, buffer []byte) error {
	pSplit := strings.Split(path, "/")

	if len(pSplit) < 4 {
		return fmt.Errorf("illegal path")
	}

	namespace := pSplit[0]
	podName := pSplit[1]
	containerName := pSplit[2]
	pathToCopy := filepath.Join(pSplit[3:]...)

	// TODO: mkdir

	lines := strings.Split((string)(buffer), "\n")
	for _, line := range lines {
		command := "echo -n " + line + " >> /" + pathToCopy
		command = "touch " + pathToCopy
		_, stderr, err := exec(client, namespace, podName, containerName, command, nil)

		if len(stderr) != 0 {
			return fmt.Errorf("STDERR: " + (string)(stderr))
		}
		if err != nil {
			fmt.Println("HERE!")
			return err
		}
	}

	return nil
}

func exec(client k8sClient, namespace, podName, containerName, command string, stdin io.Reader) ([]byte, []byte, error) {
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
