package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/mrhachi/mongodb-operator/internal/manager"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	DefaultPort = 8080
)

type Client struct {
	kube       kubernetes.Interface
	httpClient *http.Client
	namespace  string
	saName     string
	audience   string
}

// NewInClusterClient initializes a client designed to run inside the Kubernetes cluster.
// It uses client-go's in-cluster config to request audience-bound tokens on demand.
func NewInClusterClient() (*Client, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}

	kube, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("kubernetes clientset: %w", err)
	}

	namespace := os.Getenv("NAMESPACE")
	if namespace == "" {
		return nil, errors.New("NAMESPACE is missing")
	}

	saName := os.Getenv("SERVICEACCOUNT_NAME")
	if saName == "" {
		return nil, errors.New("SERVICEACCOUNT_NAME is missing")
	}

	return &Client{
		kube:      kube,
		namespace: namespace,
		saName:    saName,
		audience:  manager.TokenAudience,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}, nil
}

// getToken requests a ServiceAccount token targeted specifically for the instance-manager sidecar.
func (c *Client) getToken(ctx context.Context) (string, error) {
	expirationSeconds := int64(600) // 10 minutes

	tr, err := c.kube.CoreV1().
		ServiceAccounts(c.namespace).
		CreateToken(
			ctx,
			c.saName,
			&authenticationv1.TokenRequest{
				Spec: authenticationv1.TokenRequestSpec{
					Audiences:         []string{c.audience},
					ExpirationSeconds: &expirationSeconds,
				},
			},
			metav1.CreateOptions{},
		)
	if err != nil {
		return "", fmt.Errorf("create token request: %w", err)
	}

	return tr.Status.Token, nil
}

// doRequest attaches the dynamic Bearer token and performs an HTTP request against the sidecar API.
func (c *Client) doRequest(ctx context.Context, podIP string, method, path string, payload any) (*http.Response, error) {
	var payloadBuffer io.Reader
	if payload != nil {
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal payload: %w", err)
		}
		payloadBuffer = bytes.NewBuffer(payloadBytes)
	}

	token, err := c.getToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire token: %w", err)
	}

	url := fmt.Sprintf("http://%s:%d%s", podIP, DefaultPort, path)

	req, err := http.NewRequestWithContext(ctx, method, url, payloadBuffer)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}

	return resp, nil
}

// Initialize calls the /v1/initialize endpoint on a specific Pod member.
func (c *Client) Initialize(ctx context.Context, podIP, username, password string) error {
	payload := manager.InitializeRequest{
		Username: username,
		Password: password,
	}
	resp, err := c.doRequest(ctx, podIP, http.MethodPost, "/v1/initialize", payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent:
		return nil
	case http.StatusUnauthorized:
		return fmt.Errorf("unauthorized: token validation failed on sidecar")
	case http.StatusForbidden:
		return fmt.Errorf("forbidden: RBAC authorization failed for verb/resource")
	default:
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
}

// CheckLiveness queries the liveness check endpoint.
func (c *Client) CheckLiveness(ctx context.Context, podIP string) (bool, error) {
	url := fmt.Sprintf("http://%s:%d/livez", podIP, DefaultPort)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK, nil
}
