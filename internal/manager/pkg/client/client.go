package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	managerapi "github.com/MrHachi/mongodb-operator/internal/manager/pkg/api"
	managerconfig "github.com/MrHachi/mongodb-operator/internal/manager/pkg/config"
	mdb "github.com/MrHachi/mongodb-operator/internal/mongodb"
	authnv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const responseBodyLimit = 1 << 20

var (
	ErrNotInitialized  = errors.New("replica set is not initialized")
	ErrUnauthenticated = errors.New("needs authentication")
)

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
func New(baseURL string, kube kubernetes.Interface, namespace, saName string) *Client {
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		kube:       kube,
		namespace:  namespace,
		saName:     saName,
		audience:   managerconfig.InstanceManagerAudience,
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

// GetTopology retrieves the replica set topology and member states.
func (c *Client) GetTopology(ctx context.Context) (*mdb.Topology, error) {
	status, body, err := c.request(ctx, http.MethodGet, "/v1/mongo/topology", nil)
	if err != nil {
		return nil, fmt.Errorf("get topology: %w", err)
	}

	switch status {
	case http.StatusOK:
		var topology mdb.Topology
		if err := json.Unmarshal(body, &topology); err != nil {
			return nil, fmt.Errorf("decode topology response: %w", err)
		}
		return &topology, nil
	case http.StatusPreconditionFailed:
		var apiError managerapi.ErrorResponse
		if err := json.Unmarshal(body, &apiError); err == nil && apiError.Code == managerapi.ErrorCodeMongoUnauthorized {
			return nil, fmt.Errorf("%w: %s", ErrUnauthenticated, apiError.Message)
		}
		return nil, ErrNotInitialized
	default:
		return nil, responseError("get topology", status, body)
	}
}

// GetConfig retrieves the replica set voting and election configuration.
func (c *Client) GetConfig(ctx context.Context) (*mdb.ReplicaSetConfig, error) {
	status, body, err := c.request(ctx, http.MethodGet, "/v1/mongo/config", nil)
	if err != nil {
		return nil, fmt.Errorf("get replica set configuration: %w", err)
	}
	switch status {
	case http.StatusOK:
		var config mdb.ReplicaSetConfig
		if err := json.Unmarshal(body, &config); err != nil {
			return nil, fmt.Errorf("decode replica set configuration response: %w", err)
		}
		return &config, nil
	case http.StatusPreconditionFailed:
		return nil, ErrNotInitialized
	default:
		return nil, responseError("get replica set configuration", status, body)
	}
}

// Initiate starts the replica set. A conflict means it was already initiated.
func (c *Client) Initiate(ctx context.Context) error {
	status, body, err := c.request(ctx, http.MethodPost, "/v1/mongo/initiate", nil)
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
	status, body, err := c.request(ctx, http.MethodPost, "/v1/mongo/authenticate", request)
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
	status, body, err := c.request(ctx, http.MethodPost, "/v1/mongo/admin", request)
	if err != nil {
		return fmt.Errorf("create admin user: %w", err)
	}
	if status != http.StatusOK && status != http.StatusConflict {
		return responseError("create admin user", status, body)
	}
	return nil
}

// RemoveMember removes the replica-set member associated with podName.
func (c *Client) RemoveMember(ctx context.Context, podName string) error {
	path := "/v1/mongo/members/" + url.PathEscape(podName)
	status, body, err := c.request(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return fmt.Errorf("remove replica-set member %q: %w", podName, err)
	}
	if status != http.StatusNoContent {
		return responseError("remove replica-set member", status, body)
	}
	return nil
}

// AddMember adds the replica associated with podName. Repeating the request is safe.
func (c *Client) AddMember(ctx context.Context, podName string) error {
	path := "/v1/mongo/members/" + url.PathEscape(podName)
	status, body, err := c.request(ctx, http.MethodPost, path, nil)
	if err != nil {
		return fmt.Errorf("add replica-set member %q: %w", podName, err)
	}
	if status != http.StatusNoContent {
		return responseError("add replica-set member", status, body)
	}
	return nil
}

// TransferLeadership asks this instance-manager to move leadership away from its node.
func (c *Client) TransferLeadership(ctx context.Context) error {
	status, body, err := c.request(ctx, http.MethodPost, "/v1/manager/leadership/transfer", nil)
	if err != nil {
		return fmt.Errorf("request leadership transfer: %w", err)
	}
	if status != http.StatusAccepted {
		return responseError("request leadership transfer", status, body)
	}
	return nil
}

// GetLeadership reports the current instance-manager leadership identity.
func (c *Client) GetLeadership(ctx context.Context) (*managerapi.LeadershipStatus, error) {
	status, body, err := c.request(ctx, http.MethodGet, "/v1/manager/leadership", nil)
	if err != nil {
		return nil, fmt.Errorf("get instance-manager leadership: %w", err)
	}
	if status != http.StatusOK {
		return nil, responseError("get instance-manager leadership", status, body)
	}
	var leadership managerapi.LeadershipStatus
	if err := json.Unmarshal(body, &leadership); err != nil {
		return nil, fmt.Errorf("decode instance-manager leadership response: %w", err)
	}
	return &leadership, nil
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
