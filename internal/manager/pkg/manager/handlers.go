package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
	clientIsLocalhost           bool
	instanceManagerPasswordFile string
	logger                      *slog.Logger

	authenticator                   *auth.Authenticator
	rsName, hostname                string
	serviceName                     string
	namespace                       string
	operatorServiceAccountNamespace string
	operatorServiceAccountName      string
}

func NewInstanceManager(
	client *mongodb.Client,
	authenticator *auth.Authenticator,
	logger *slog.Logger,
	instanceManagerPasswordFile, rsName, hostname,
	serviceName, namespace,
	operatorServiceAccountNamespace, operatorServiceAccountName string,
) *InstanceManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &InstanceManager{
		client:                      client,
		clientIsLocalhost:           true,
		instanceManagerPasswordFile: instanceManagerPasswordFile,
		logger:                      logger,
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
	if !m.clientIsLocalhost {
		m.mu.RUnlock()
		return
	}
	directClient := m.client
	m.mu.RUnlock()
	if directClient == nil {
		m.logger.Warn(
			"Cannot promote MongoDB client because no direct client is configured",
			"replica_set", m.rsName, "hostname", m.hostname,
		)
		return
	}
	password, err := m.readInstanceManagerPassword()
	if err != nil {
		m.logger.Warn(
			"Could not read instance-manager credentials for MongoDB client promotion",
			"replica_set", m.rsName, "hostname", m.hostname, "error", err,
		)
		return
	}

	authenticatedClient, err := directClient.NewAuthenticatedClient(
		ctx,
		mongodb.InstanceManagerUsername,
		password,
		"admin",
	)
	if err != nil {
		m.logger.Warn(
			"Could not authenticate MongoDB client with instance-manager credentials",
			"replica_set", m.rsName, "hostname", m.hostname, "error", err,
		)
		return
	}

	m.mu.Lock()
	if !m.clientIsLocalhost {
		m.mu.Unlock()
		_ = authenticatedClient.Close(context.Background())
		return
	}
	m.client = authenticatedClient
	m.clientIsLocalhost = false
	m.mu.Unlock()

	if err := directClient.Close(context.Background()); err != nil {
		m.logger.Warn(
			"Could not close direct MongoDB client after promotion",
			"replica_set", m.rsName, "hostname", m.hostname, "error", err,
		)
	}
	m.logger.Info("Promoted MongoDB client to instance-manager credentials", "replica_set", m.rsName, "hostname", m.hostname)
}

func (m *InstanceManager) acquireClient() (*mongodb.Client, string, func()) {
	m.mu.RLock()
	mode := "authenticated"
	if m.clientIsLocalhost {
		mode = "direct"
	}
	if m.client == nil {
		mode = "unavailable"
	}
	return m.client, mode, m.mu.RUnlock
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
	client, clientMode, release := m.acquireClient()
	defer release()

	if client == nil {
		m.logger.Warn(
			"MongoDB readiness check failed because no client is available",
			"mongo_client_mode", clientMode,
		)
		http.Error(w, "MongoDB is not ready", http.StatusServiceUnavailable)
		return
	}
	if err := client.Ping(r.Context()); err != nil {
		m.logger.Warn(
			"MongoDB readiness check failed",
			"mongo_client_mode", clientMode, "error", err,
		)
		http.Error(w, "MongoDB is not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *InstanceManager) HandleInitiate(w http.ResponseWriter, r *http.Request) {
	client, clientMode, release := m.acquireClient()
	defer release()

	err := client.InitiateReplicaSet(r.Context(), m.rsName, m.hostname)
	if err != nil {
		m.logger.Error(
			"Could not initiate MongoDB replica set",
			"replica_set", m.rsName, "mongo_client_mode", clientMode, "error", err,
		)
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

	m.logger.Info("Initiated MongoDB replica set", "replica_set", m.rsName)
	w.WriteHeader(http.StatusOK)
}

func (m *InstanceManager) HandleAdmin(w http.ResponseWriter, r *http.Request) {
	var req AdminRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	if err := decoder.Decode(&req); err != nil || req.Username == "" || req.Password == "" {
		http.Error(w, "Invalid admin credentials", http.StatusBadRequest)
		return
	}

	client, clientMode, release := m.acquireClient()
	defer release()

	adminClient, err := client.NewAuthenticatedClient(r.Context(), req.Username, req.Password, "admin")
	if err == nil {
		_ = adminClient.Close(context.Background())
		m.logger.Info("Validated existing MongoDB admin credentials", "mongo_client_mode", clientMode)
		w.WriteHeader(http.StatusOK)
		return
	}
	m.logger.Info(
		"Could not authenticate existing MongoDB admin user; attempting creation",
		"mongo_client_mode", clientMode, "error", err,
	)

	err = client.CreateAdminUser(r.Context(), req.Username, req.Password)
	if err != nil {
		m.logger.Error(
			"Could not create MongoDB admin user",
			"mongo_client_mode", clientMode, "error", err,
		)
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

	m.logger.Info("Created MongoDB admin user", "mongo_client_mode", clientMode)
	w.WriteHeader(http.StatusOK)
}

func (m *InstanceManager) HandleInstanceManagerUser(w http.ResponseWriter, r *http.Request) {
	var req AdminRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	if err := decoder.Decode(&req); err != nil || req.Username == "" || req.Password == "" {
		http.Error(w, "Invalid admin credentials", http.StatusBadRequest)
		return
	}

	client, clientMode, release := m.acquireClient()
	defer release()
	adminClient, err := client.NewAuthenticatedClient(r.Context(), req.Username, req.Password, "admin")
	if err != nil {
		m.logger.Warn(
			"Could not authenticate admin credentials while provisioning instance-manager user",
			"mongo_client_mode", clientMode, "error", err,
		)
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
		m.logger.Error("Could not read instance-manager credentials", "error", err)
		http.Error(w, "Could not read instance-manager credentials", http.StatusInternalServerError)
		return
	}
	if err := adminClient.EnsureInstanceManagerUser(r.Context(), instanceManagerPassword); err != nil {
		m.logger.Error("Could not ensure instance-manager MongoDB user", "error", err)
		if writeMongoError(w, err) {
			return
		}
		http.Error(w, "Could not create or update instance-manager user", http.StatusInternalServerError)
		return
	}
	m.logger.Info("Ensured instance-manager MongoDB user")
	w.WriteHeader(http.StatusNoContent)
}

func (m *InstanceManager) HandleGetTopology(w http.ResponseWriter, r *http.Request) {
	client, clientMode, release := m.acquireClient()
	defer release()

	topology, err := client.GetTopology(r.Context())
	if err != nil {
		m.logger.Error(
			"Could not get MongoDB topology",
			"mongo_client_mode", clientMode, "error", err,
		)
		if !writeMongoError(w, err) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	m.logger.Info(
		"Retrieved MongoDB topology",
		"mongo_client_mode", clientMode, "config_version", topology.ConfigVersion, "member_count", len(topology.Members),
	)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(topology); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (m *InstanceManager) HandleGetConfig(w http.ResponseWriter, r *http.Request) {
	client, clientMode, release := m.acquireClient()
	defer release()

	config, err := client.GetConfig(r.Context())
	if err != nil {
		m.logger.Error("Could not get MongoDB replica set configuration",
			"mongo_client_mode", clientMode, "error", err,
		)
		if !writeMongoError(w, err) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	m.logger.Info(
		"Retrieved MongoDB replica set configuration",
		"mongo_client_mode", clientMode, "version", config.Version, "member_count", len(config.Members),
	)
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

	client, clientMode, release := m.acquireClient()
	defer release()

	if err := client.RemoveMember(r.Context(), m.memberHost(podName)); err != nil {
		m.logger.Error(
			"Could not remove MongoDB replica-set member",
			"pod_name", podName, "mongo_client_mode", clientMode, "error", err,
		)
		if !writeMongoError(w, err) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	m.logger.Info(
		"Removed MongoDB replica-set member",
		"pod_name", podName, "mongo_client_mode", clientMode,
	)
	w.WriteHeader(http.StatusNoContent)
}

func (m *InstanceManager) HandleAddMember(w http.ResponseWriter, r *http.Request) {
	podName := chi.URLParam(r, "podName")
	if podName == "" {
		http.Error(w, "Pod name is required", http.StatusBadRequest)
		return
	}

	client, clientMode, release := m.acquireClient()
	defer release()

	if err := client.AddMember(r.Context(), m.memberHost(podName)); err != nil {
		m.logger.Error(
			"Could not add MongoDB replica-set member",
			"pod_name", podName, "mongo_client_mode", clientMode, "error", err,
		)
		if !writeMongoError(w, err) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	m.logger.Info(
		"Added MongoDB replica-set member",
		"pod_name", podName, "mongo_client_mode", clientMode,
	)
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
	m.logger.Warn("Leadership transfer endpoint is not implemented", "hostname", m.hostname)
	w.WriteHeader(http.StatusAccepted)
}

func (m *InstanceManager) HandleGetLeadership(w http.ResponseWriter, _ *http.Request) {
	m.logger.Warn(
		"Leadership state endpoint is not implemented; returning placeholder leader",
		"leader", managerapi.NoLeader,
	)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(managerapi.LeadershipStatus{Leader: managerapi.NoLeader}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type AdminRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
