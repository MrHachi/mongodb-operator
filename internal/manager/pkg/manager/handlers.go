package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	managerapi "github.com/MrHachi/mongodb-operator/internal/manager/pkg/api"
	"github.com/MrHachi/mongodb-operator/internal/manager/pkg/auth"
	"github.com/MrHachi/mongodb-operator/internal/manager/pkg/mongodb"
	"github.com/go-chi/chi/v5"
)

type InstanceManager struct {
	mu     sync.Mutex
	client *mongodb.Client

	authenticator                   *auth.Authenticator
	rsName, hostname                string
	serviceName                     string
	namespace                       string
	operatorServiceAccountNamespace string
	operatorServiceAccountName      string
}

func NewInstanceManager(client *mongodb.Client, authenticator *auth.Authenticator, rsName, hostname, serviceName, namespace, operatorServiceAccountNamespace, operatorServiceAccountName string) *InstanceManager {
	return &InstanceManager{
		client:        client,
		authenticator: authenticator,
		rsName:        rsName,
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
	m.mu.Lock()
	client := m.client
	m.mu.Unlock()

	if client == nil || client.Ping(r.Context()) != nil {
		http.Error(w, "MongoDB is not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *InstanceManager) HandleAuthenticate(w http.ResponseWriter, r *http.Request) {
	var req AuthenticationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	newURI, err := m.client.Authenticate(r.Context(), req.Username, req.Password, req.AuthSource)
	if err != nil {
		if writeMongoError(w, err) {
			return
		}
		http.Error(w, "Authentication failed", http.StatusUnauthorized)
		return
	}

	newClient, err := mongodb.NewClient(r.Context(), newURI)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	oldClient := m.client
	m.client = newClient

	if oldClient != nil {
		go oldClient.Close(context.Background())
	}

	w.WriteHeader(http.StatusOK)
}

func (m *InstanceManager) HandleInitiate(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	client := m.client
	m.mu.Unlock()

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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	client := m.client
	m.mu.Unlock()

	err := client.CreateAdminUser(r.Context(), req.Username, req.Password)
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

func (m *InstanceManager) HandleGetTopology(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	client := m.client
	m.mu.Unlock()

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
	m.mu.Lock()
	client := m.client
	m.mu.Unlock()

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

	m.mu.Lock()
	client := m.client
	m.mu.Unlock()

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

	m.mu.Lock()
	client := m.client
	m.mu.Unlock()

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

type AuthenticationRequest struct {
	Username   string `json:"username"`
	Password   string `json:"password"`
	AuthSource string `json:"auth_source"`
}

type AdminRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
