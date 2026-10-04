package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/MrHachi/mongodb-operator/internal/manager/pkg/auth"
	"github.com/MrHachi/mongodb-operator/internal/manager/pkg/mongodb"
)

type InstanceManager struct {
	mu     sync.Mutex
	client *mongodb.Client

	authenticator    *auth.Authenticator
	rsName, hostname string
	namespace        string
}

func NewInstanceManager(client *mongodb.Client, authenticator *auth.Authenticator, rsName, hostname, serviceName, namespace string) *InstanceManager {
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
		namespace: namespace,
	}
}

func (m *InstanceManager) RequirePermission(verb, group, resource string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
				http.Error(w, "Missing or invalid authorization header", http.StatusUnauthorized)
				return
			}
			token := strings.TrimPrefix(authHeader, "Bearer ")

			if err := m.authenticator.VerifyOperatorToken(r.Context(), token, m.namespace, verb); err != nil {
				http.Error(w, fmt.Sprintf("verify operator token: %v", err), http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
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
		if strings.Contains(err.Error(), "cluster not initialized") {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(topology); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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
