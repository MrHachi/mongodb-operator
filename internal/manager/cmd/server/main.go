package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/MrHachi/mongodb-operator/internal/manager/pkg/auth"
	mgr "github.com/MrHachi/mongodb-operator/internal/manager/pkg/manager"
	"github.com/MrHachi/mongodb-operator/internal/manager/pkg/mongodb"
	"github.com/MrHachi/mongodb-operator/internal/manager/pkg/config"
	"k8s.io/client-go/rest"
)

func main() {
	ctx := context.Background()

	// Use in-cluster config for running in Pod
	k8sCfg, err := rest.InClusterConfig()
	if err != nil {
		log.Println("Failed to load in-cluster config, using default kubeconfig")
		// In a real app, we'd use clientcmd.BuildConfigFromFlags("", kubeconfig)
	}

	dbURI := os.Getenv("MONGODB_URI")
	if dbURI == "" {
		dbURI = "mongodb://localhost:27017"
	}

	dbClient, err := mongodb.NewClient(ctx, dbURI)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer dbClient.Close(ctx)

	authenticator, err := auth.NewAuthenticator(k8sCfg)
	if err != nil {
		log.Fatalf("Failed to initialize authenticator: %v", err)
	}

	cfg := config.LoadConfig()
	m := mgr.NewInstanceManager(dbClient, authenticator, cfg.RSName, cfg.Hostname, cfg.ServiceName, cfg.Namespace)
	router := m.Serve()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting instance-manager on port %s", port)
	if err := http.ListenAndServe(":"+port, router); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
