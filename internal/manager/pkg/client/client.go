package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

var (
	ErrNotInitialized  = errors.New("replica set is not initialized")
	ErrUnauthenticated = errors.New("admin credentials are unauthenticated")
)

const responseBodyLimit = 4096

// Client calls the HTTP API exposed by an instance-manager sidecar.
type Client struct {
	baseURL    string
	kube       kubernetes.Interface
	namespace  string
	saName     string
	audience   string
	httpClient *http.Client
}

// New creates a client for one instance-manager pod and the operator ServiceAccount used to call it.
func New(baseURL string, kube kubernetes.Interface, namespace, saName, audience string) *Client {
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		kube:       kube,
		namespace:  namespace,
		saName:     saName,
		audience:   audience,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// getToken requests a short-lived token for the operator ServiceAccount, targeted at the instance-manager.
func (c *Client) getToken(ctx context.Context) (string, error) {
	if c.kube == nil {
		return "", errors.New("kubernetes client is not configured")
	}
	if c.namespace == "" || c.saName == "" || c.audience == "" {
		return "", errors.New("service account namespace, name, and token audience must be configured")
	}

	expirationSeconds := int64(600)
	tokenRequest := &authnv1.TokenRequest{
		Spec: authnv1.TokenRequestSpec{
			Audiences:         []string{c.audience},
			ExpirationSeconds: &expirationSeconds,
		},
	}
	token, err := c.kube.CoreV1().ServiceAccounts(c.namespace).CreateToken(ctx, c.saName, tokenRequest, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("create token request: %w", err)
	}
	if token.Status.Token == "" {
		return "", errors.New("create token request returned an empty token")
	}
	return token.Status.Token, nil
}

// GetTopology reports whether the replica set has been initialized.
func (c *Client) GetTopology(ctx context.Context) error {
	status, body, err := c.request(ctx, http.MethodGet, "/v1/topology", nil)
	if err != nil {
		return fmt.Errorf("get topology: %w", err)
	}

	switch status {
	case http.StatusOK:
		return nil
	case http.StatusPreconditionFailed:
		return ErrNotInitialized
	default:
		return responseError("get topology", status, body)
	}
}

// Initiate starts the replica set. A conflict means it was already initiated.
func (c *Client) Initiate(ctx context.Context) error {
	status, body, err := c.request(ctx, http.MethodPost, "/v1/initiate", nil)
	if err != nil {
		return fmt.Errorf("initiate replica set: %w", err)
	}
	if status != http.StatusOK && status != http.StatusConflict {
		return responseError("initiate replica set", status, body)
	}
	return nil
}

// Authenticate verifies the admin credentials and switches the sidecar's MongoDB client to them.
func (c *Client) Authenticate(ctx context.Context, username, password string) error {
	request := struct {
		Username   string `json:"username"`
		Password   string `json:"password"`
		AuthSource string `json:"auth_source"`
	}{Username: username, Password: password, AuthSource: "admin"}
	status, body, err := c.request(ctx, http.MethodPost, "/v1/authenticate", request)
	if err != nil {
		return fmt.Errorf("authenticate admin user: %w", err)
	}
	switch status {
	case http.StatusOK:
		return nil
	case http.StatusUnauthorized:
		return ErrUnauthenticated
	default:
		return responseError("authenticate admin user", status, body)
	}
}

// CreateAdmin creates the admin user. A conflict means the user already exists.
func (c *Client) CreateAdmin(ctx context.Context, username, password string) error {
	request := struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}{Username: username, Password: password}
	status, body, err := c.request(ctx, http.MethodPost, "/v1/admin", request)
	if err != nil {
		return fmt.Errorf("create admin user: %w", err)
	}
	if status != http.StatusOK && status != http.StatusConflict {
		return responseError("create admin user", status, body)
	}
	return nil
}

func (c *Client) request(ctx context.Context, method, path string, payload any) (int, []byte, error) {
	token, err := c.getToken(ctx)
	if err != nil {
		return 0, nil, err
	}

	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, fmt.Errorf("encode request body: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return 0, nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("send request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, responseBodyLimit))
	if err != nil {
		return 0, nil, fmt.Errorf("read response: %w", err)
	}
	return resp.StatusCode, responseBody, nil
}

func responseError(operation string, status int, body []byte) error {
	message := strings.TrimSpace(string(body))
	if message == "" {
		return fmt.Errorf("%s returned HTTP status %d", operation, status)
	}
	return fmt.Errorf("%s returned HTTP status %d: %s", operation, status, message)
}
