package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

func (m *MongoManager) Serve(ctx context.Context, addr string) error {
	r := chi.NewRouter()

	r.Post("/livez", m.HandleLivez)
	r.Post("/readyz", m.HandleReadyz)
	r.Route("/v1", func(r chi.Router) {
		r.With(func(next http.Handler) http.Handler {
			return m.authorize("create", "mongodbs", next)
		}).Post("/initialize", m.HandleInitialize)
	})

	server := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			m.logger.Error("failed to shut down HTTP server",
				"error", err,
			)
		}

		if err := m.client.Disconnect(shutdownCtx); err != nil {
			m.logger.Error("failed to disconnect mongodb client",
				"error", err,
			)
		}
	}()

	m.logger.Info("starting HTTP server", "addr", addr)

	if err := server.ListenAndServe(); err != nil &&
		!errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("HTTP server: %w", err)
	}

	return nil
}

func (m *MongoManager) HandleLivez(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func (m *MongoManager) HandleReadyz(w http.ResponseWriter, r *http.Request) {
	pingCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := m.client.Ping(pingCtx, nil); err != nil {
		http.Error(w, "ping mongodb: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (m *MongoManager) HandleInitialize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req InitializeRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Username == "" ||
		req.Password == "" {
		http.Error(w, "missing required field", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	if err := m.EnsureAdminUser(ctx, req.Username, req.Password); err != nil {
		m.logger.Error("failed to ensure admin user",
			"error", err,
		)
		http.Error(w, "failed to ensure admin user", http.StatusInternalServerError)
		return
	}

	if err := m.EnsureReplicaSet(
		ctx,
		m.hostname,
		m.serviceName,
	); err != nil {
		m.logger.Error("failed to ensure replica set",
			"error", err,
		)
		http.Error(w, "failed to initialize replica set", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
