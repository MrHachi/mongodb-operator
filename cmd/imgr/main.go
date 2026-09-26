package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/mrhachi/mongodb-operator/internal/manager"

	"go.uber.org/zap"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := zap.L()

	m, err := manager.NewMongoManager(ctx, log)
	if err != nil {
		log.Error("failed to create mongo instance manager", zap.Error(err))
		os.Exit(1)
	}

	if err := m.Serve(ctx, ":8080"); err != nil {
		log.Error("failed to serve", zap.Error(err), zap.Int32("port", 8080))
		os.Exit(1)
	}
}
