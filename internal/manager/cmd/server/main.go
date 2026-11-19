package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/MrHachi/mongodb-operator/internal/manager/pkg/auth"
	"github.com/MrHachi/mongodb-operator/internal/manager/pkg/config"
	mgr "github.com/MrHachi/mongodb-operator/internal/manager/pkg/manager"
	"github.com/MrHachi/mongodb-operator/internal/manager/pkg/mongodb"
	"k8s.io/client-go/rest"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx := context.Background()
	cfg := config.LoadConfig()

	// Use in-cluster config for running in Pod
	k8sCfg, err := rest.InClusterConfig()
	if err != nil {
		logger.Warn("Could not load in-cluster Kubernetes configuration", "error", err)
	}

	dbClient, err := mongodb.NewClient(ctx, cfg.MongoURI)
	if err != nil {
		logger.Error("Could not connect to MongoDB", "error", err)
		os.Exit(1)
	}
	defer dbClient.Close(ctx)

	authenticator, err := auth.NewAuthenticator(k8sCfg)
	if err != nil {
		logger.Error("Could not initialize authenticator", "error", err)
		os.Exit(1)
	}

	m := mgr.NewInstanceManager(
		dbClient,
		authenticator,
		logger,
		cfg.InstanceManagerPasswordFile,
		cfg.RSName,
		cfg.Hostname,
		cfg.ServiceName,
		cfg.Namespace,
		cfg.OperatorServiceAccountNamespace,
		cfg.OperatorServiceAccountName,
	)
	go m.RunPromotionLoop(ctx)
	router := m.Serve()

	port := "8080"
	logger.Info("Starting instance-manager API server", "port", port)
	if err := http.ListenAndServe(":"+port, router); err != nil {
		logger.Error("Instance-manager API server failed", "error", err)
		os.Exit(1)
	}
}
