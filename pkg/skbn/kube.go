package skbn

import (
	"bytes"
	"fmt"
	"io"
	"log"
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

func DownloadFromK8s(path string) ([]byte, error) {
	pSplit := strings.Split(path, "/")

	if len(pSplit) < 4 {
		return nil, fmt.Errorf("illegal path")
	}

	namespace := pSplit[0]
	podName := pSplit[1]
	containerName := pSplit[2]
	pathToCopy := pSplit[3]

	output, stderr, err := execCat(namespace, podName, containerName, pathToCopy, nil)

	if len(stderr) != 0 {
		return output, fmt.Errorf("STDERR: " + (string)(stderr))
	}
	if err != nil {
		return output, err
	}

	return output, nil
}

func execCat(namespace, podName, containerName, pathToCopy string, stdin io.Reader) ([]byte, []byte, error) {

	clientset, config := getClientSetAndConfig()

	req := clientset.Core().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec")
	scheme := runtime.NewScheme()
	if err := core_v1.AddToScheme(scheme); err != nil {
		return nil, nil, fmt.Errorf("error adding to scheme: %v", err)
	}

	command := "cat " + pathToCopy

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

func getClientSetAndConfig() (*kubernetes.Clientset, *rest.Config) {
	var kubeconfig string
	if kubeConfigPath := os.Getenv("KUBECONFIG"); kubeConfigPath != "" {
		kubeconfig = kubeConfigPath // CI process
	} else {
		kubeconfig = filepath.Join(os.Getenv("HOME"), ".kube", "config") // Development environment
	}

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(err.Error())
	}

	return clientset, config
}
