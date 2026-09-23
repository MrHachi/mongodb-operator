package exec

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	corev1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

var (
	mu     sync.Mutex
	config *rest.Config
)

// SetClientConfig sets the package-level REST config explicitly
func SetClientConfig(xconfig *rest.Config) {
	mu.Lock()
	defer mu.Unlock()
	config = xconfig
}

// ensureClientConfig ensures that the REST config is set, creates it if necessary, and returns it to the caller
// (prefer accessing config via this function for concurrency-safety)
func ensureClientConfig() (*rest.Config, error) {
	mu.Lock()
	defer mu.Unlock()

	if config != nil {
		return config, nil
	}

	var err error
	config, err = rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("get in-cluster config: %w", err)
	}

	return config, nil
}

// ExecuteCommand executes the specified command on the given pod in the given namespace.
func ExecuteCommand(ctx context.Context, command []string, containerName, podName, namespace string, stdin io.Reader) (string, string, error) {
	config, err := ensureClientConfig()
	if err != nil {
		return "", "", fmt.Errorf("ensure client config: %w", err)
	}

	client, err := rest.RESTClientFor(config)
	if err != nil {
		return "", "", fmt.Errorf("create REST client: %w", err)
	}

	req := client.Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec")
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		return "", "", fmt.Errorf("add to scheme: %w", err)
	}

	parameterCodec := runtime.NewParameterCodec(scheme)
	req.VersionedParams(&corev1.PodExecOptions{
		Command:   command,
		Container: containerName,
		Stdin:     stdin != nil,
		Stdout:    true,
		Stderr:    true,
		TTY:       false,
	}, parameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return "", "", fmt.Errorf("create SPDY executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  stdin,
		Stdout: &stdout,
		Stderr: &stderr,
		Tty:    false,
	}); err != nil {
		return "", "", fmt.Errorf("stream command: %w", err)
	}

	return stdout.String(), stderr.String(), nil
}
