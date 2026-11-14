package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/MrHachi/mongodb-operator/internal/manager/pkg/auth"
	"github.com/MrHachi/mongodb-operator/internal/manager/pkg/mongodb"
)

type InstanceManager struct {
	client           *mongodb.Client
	authenticator    *auth.Authenticator
	rsName, hostname string
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
	}
}

func (m *InstanceManager) Authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			http.Error(w, "Missing or invalid authorization header", http.StatusUnauthorized)
			return
		}
		token := strings.TrimPrefix(authHeader, "Bearer ")

		namespace := r.Header.Get("X-Kubernetes-Namespace")
		if namespace == "" {
			http.Error(w, "Missing X-Kubernetes-Namespace header", http.StatusBadRequest)
			return
		}

		if err := m.authenticator.VerifyOperatorToken(r.Context(), token, namespace, "create"); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (m *InstanceManager) HandleInitiate(w http.ResponseWriter, r *http.Request) {
	err := m.client.InitiateReplicaSet(r.Context(), m.rsName, m.hostname)
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

	err := m.client.CreateAdminUser(r.Context(), req.Username, req.Password)
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

type AdminRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
