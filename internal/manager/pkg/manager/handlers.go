package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	managerapi "github.com/MrHachi/mongodb-operator/internal/manager/pkg/api"
	"github.com/MrHachi/mongodb-operator/internal/manager/pkg/auth"
	"github.com/MrHachi/mongodb-operator/internal/manager/pkg/mongodb"
	"github.com/go-chi/chi/v5"
)

type InstanceManager struct {
	mu                          sync.RWMutex
	client                      *mongodb.Client
	clientIsDirect              bool
	instanceManagerPasswordFile string

	authenticator                   *auth.Authenticator
	rsName, hostname                string
	serviceName                     string
	namespace                       string
	operatorServiceAccountNamespace string
	operatorServiceAccountName      string
}

func NewInstanceManager(client *mongodb.Client, authenticator *auth.Authenticator, instanceManagerPasswordFile, rsName, hostname, serviceName, namespace, operatorServiceAccountNamespace, operatorServiceAccountName string) *InstanceManager {
	return &InstanceManager{
		client:                      client,
		clientIsDirect:              true,
		instanceManagerPasswordFile: instanceManagerPasswordFile,
		authenticator:               authenticator,
		rsName:                      rsName,
		hostname: fmt.Sprintf(
			"%s.%s.%s.svc.cluster.local:27017",
			hostname,
			serviceName,
			namespace,
		),
		serviceName:                     serviceName,
		namespace:                       namespace,
		operatorServiceAccountNamespace: operatorServiceAccountNamespace,
		operatorServiceAccountName:      operatorServiceAccountName,
	}
}

// RunPromotionLoop periodically promotes the direct localhost client once the
// instance-manager MongoDB user is available.
func (m *InstanceManager) RunPromotionLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.promoteClient(ctx)
		}
	}
}

func (m *InstanceManager) promoteClient(ctx context.Context) {
	m.mu.RLock()
	if !m.clientIsDirect {
		m.mu.RUnlock()
		return
	}
	directClient := m.client
	m.mu.RUnlock()
	password, err := m.readInstanceManagerPassword()
	if err != nil {
		return
	}

	authenticatedClient, err := directClient.NewAuthenticatedClient(
		ctx,
		mongodb.InstanceManagerUsername,
		password,
		"admin",
	)
	if err != nil {
		return
	}

	m.mu.Lock()
	if !m.clientIsDirect {
		m.mu.Unlock()
		_ = authenticatedClient.Close(context.Background())
		return
	}
	m.client = authenticatedClient
	m.clientIsDirect = false
	m.mu.Unlock()

	_ = directClient.Close(context.Background())
}

func (m *InstanceManager) acquireClient() (*mongodb.Client, func()) {
	m.mu.RLock()
	return m.client, m.mu.RUnlock
}

func (m *InstanceManager) readInstanceManagerPassword() (string, error) {
	passwordBytes, err := os.ReadFile(m.instanceManagerPasswordFile)
	if err != nil {
		return "", fmt.Errorf("read instance-manager password file: %w", err)
	}
	password := strings.TrimSpace(string(passwordBytes))
	if password == "" {
		return "", fmt.Errorf("instance-manager password file is empty")
	}
	return password, nil
}

func (m *InstanceManager) requirePermission(verb string) func(http.Handler) http.Handler {
	return m.requireToken(func(ctx context.Context, token string) error {
		return m.authenticator.VerifyOperatorToken(ctx, token, m.namespace, verb)
	})
}

func (m *InstanceManager) requireOperator() func(http.Handler) http.Handler {
	return m.requireToken(func(ctx context.Context, token string) error {
		return m.authenticator.VerifyOperatorTokenIdentity(
			ctx,
			token,
			m.operatorServiceAccountNamespace,
			m.operatorServiceAccountName,
		)
	})
}

func (m *InstanceManager) requireToken(verify func(context.Context, string) error) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
				http.Error(w, "Missing or invalid authorization header", http.StatusUnauthorized)
				return
			}
			token := strings.TrimPrefix(authHeader, "Bearer ")

			if err := verify(r.Context(), token); err != nil {
				http.Error(w, fmt.Sprintf("verify operator token: %v", err), http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func (m *InstanceManager) HandleLivez(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func (m *InstanceManager) HandleReadyz(w http.ResponseWriter, r *http.Request) {
	client, release := m.acquireClient()
	defer release()

	if client == nil || client.Ping(r.Context()) != nil {
		http.Error(w, "MongoDB is not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *InstanceManager) HandleInitiate(w http.ResponseWriter, r *http.Request) {
	client, release := m.acquireClient()
	defer release()

	err := client.InitiateReplicaSet(r.Context(), m.rsName, m.hostname)
	if err != nil {
		if writeMongoError(w, err) {
			return
		}
		if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "already initialized") {
			w.WriteHeader(http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (m *InstanceManager) HandleAdmin(w http.ResponseWriter, r *http.Request) {
	var req AdminRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	if err := decoder.Decode(&req); err != nil || req.Username == "" || req.Password == "" {
		http.Error(w, "Invalid admin credentials", http.StatusBadRequest)
		return
	}

	client, release := m.acquireClient()
	defer release()

	adminClient, err := client.NewAuthenticatedClient(r.Context(), req.Username, req.Password, "admin")
	if err == nil {
		_ = adminClient.Close(context.Background())
		w.WriteHeader(http.StatusOK)
		return
	}

	err = client.CreateAdminUser(r.Context(), req.Username, req.Password)
	if err != nil {
		if writeMongoError(w, err) {
			return
		}
		if strings.Contains(err.Error(), "already exists") {
			w.WriteHeader(http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (m *InstanceManager) HandleInstanceManagerUser(w http.ResponseWriter, r *http.Request) {
	var req AdminRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	if err := decoder.Decode(&req); err != nil || req.Username == "" || req.Password == "" {
		http.Error(w, "Invalid admin credentials", http.StatusBadRequest)
		return
	}

	client, release := m.acquireClient()
	defer release()
	adminClient, err := client.NewAuthenticatedClient(r.Context(), req.Username, req.Password, "admin")
	if err != nil {
		if errors.Is(err, mongodb.ErrAuthenticationFailed) {
			http.Error(w, "Admin authentication failed", http.StatusUnauthorized)
			return
		}
		if writeMongoError(w, err) {
			return
		}
		http.Error(w, "Could not authenticate with MongoDB", http.StatusServiceUnavailable)
		return
	}
	defer func() { _ = adminClient.Close(context.Background()) }()

	instanceManagerPassword, err := m.readInstanceManagerPassword()
	if err != nil {
		http.Error(w, "Could not read instance-manager credentials", http.StatusInternalServerError)
		return
	}
	if err := adminClient.EnsureInstanceManagerUser(r.Context(), instanceManagerPassword); err != nil {
		if writeMongoError(w, err) {
			return
		}
		http.Error(w, "Could not create or update instance-manager user", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *InstanceManager) HandleGetTopology(w http.ResponseWriter, r *http.Request) {
	client, release := m.acquireClient()
	defer release()

	topology, err := client.GetTopology(r.Context())
	if err != nil {
		if !writeMongoError(w, err) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(topology); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (m *InstanceManager) HandleGetConfig(w http.ResponseWriter, r *http.Request) {
	client, release := m.acquireClient()
	defer release()

	config, err := client.GetConfig(r.Context())
	if err != nil {
		if !writeMongoError(w, err) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(config); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (m *InstanceManager) HandleRemoveMember(w http.ResponseWriter, r *http.Request) {
	podName := chi.URLParam(r, "podName")
	if podName == "" {
		http.Error(w, "Pod name is required", http.StatusBadRequest)
		return
	}

	client, release := m.acquireClient()
	defer release()

	if err := client.RemoveMember(r.Context(), m.memberHost(podName)); err != nil {
		if !writeMongoError(w, err) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *InstanceManager) HandleAddMember(w http.ResponseWriter, r *http.Request) {
	podName := chi.URLParam(r, "podName")
	if podName == "" {
		http.Error(w, "Pod name is required", http.StatusBadRequest)
		return
	}

	client, release := m.acquireClient()
	defer release()

	if err := client.AddMember(r.Context(), m.memberHost(podName)); err != nil {
		if !writeMongoError(w, err) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *InstanceManager) memberHost(podName string) string {
	return fmt.Sprintf("%s.%s.%s.svc.cluster.local:27017", podName, m.serviceName, m.namespace)
}

func writeMongoError(w http.ResponseWriter, err error) bool {
	var code string
	switch {
	case errors.Is(err, mongodb.ErrNotInitialized):
		code = managerapi.ErrorCodeMongoNotInitialized
	case errors.Is(err, mongodb.ErrUnauthorized):
		code = managerapi.ErrorCodeMongoUnauthorized
	default:
		return false
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusPreconditionFailed)
	_ = json.NewEncoder(w).Encode(managerapi.ErrorResponse{
		Code:    code,
		Message: err.Error(),
	})
	return true
}

func (m *InstanceManager) HandleTransferLeadership(w http.ResponseWriter, _ *http.Request) {
	log.Printf("WARNING: Leadership transfer endpoint is waiting for implementation; request is a no-op for node %s", m.hostname)
	w.WriteHeader(http.StatusAccepted)
}

func (m *InstanceManager) HandleGetLeadership(w http.ResponseWriter, _ *http.Request) {
	log.Printf("WARNING: Leadership state endpoint is waiting for implementation; returning placeholder leader %q", managerapi.NoLeader)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(managerapi.LeadershipStatus{Leader: managerapi.NoLeader}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type AdminRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
