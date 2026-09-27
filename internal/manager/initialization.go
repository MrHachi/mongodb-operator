package manager

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type InitializeRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// Attempt the create the admin user with the current client (being the localhost
// exception client immediately post-creation).
func (m *MongoManager) ensureAdminUser(
	ctx context.Context,
	username, password string,
) error {
	// First, try connecting with the localhost exception so
	// we can create the initial admin user.
	adminDB := m.client.Database("admin")

	_, err := adminDB.RunCommand(ctx, bson.D{
		{Key: "createUser", Value: username},
		{Key: "pwd", Value: password},
		{Key: "roles", Value: bson.A{
			bson.D{
				{Key: "role", Value: "root"},
				{Key: "db", Value: "admin"},
			},
		}},
	}).Raw()

	if err != nil {
		if cmdErr, ok := errors.AsType[mongo.CommandError](err); ok {
			if cmdErr.Code != unauthorizedCode {
				return fmt.Errorf("create user (%s, %d: %s)", cmdErr.Name, cmdErr.Code, cmdErr.Message)
			}
			m.logger.Info("received unauthorized error when creating admin user, will attempt to authenticate with credentials")
		} else {
			return fmt.Errorf("create user (not command error): %w", err)
		}
	}

	// Try authenticating as the requested user.
	if err := m.dbAuth(ctx, username, password); err != nil {
		return fmt.Errorf("authenticate against admin database: %w", err)
	}

	return nil
}

// Expects that a headless service has been created by the intialization step and currently exists
func (m *MongoManager) ensureReplicaSet(ctx context.Context, hostname, serviceName string) error {
	_, err := m.client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "replSetGetStatus", Value: 1},
	}).Raw()

	if err == nil {
		return nil
	}

	cmdErr, ok := errors.AsType[mongo.CommandError](err)
	if !ok {
		return fmt.Errorf("check replica set status: %w", err)
	}

	switch cmdErr.Code {
	case notYetInitializedCode:
		memberHost := fmt.Sprintf(
			"%s.%s.%s.svc.cluster.local:27017",
			hostname,
			serviceName,
			m.namespace,
		)

		config := bson.D{
			{Key: "_id", Value: m.rsName},
			{Key: "members", Value: bson.A{
				bson.D{
					{Key: "_id", Value: 0},
					{Key: "host", Value: memberHost},
				},
			}},
		}
		if _, err := m.client.Database("admin").RunCommand(ctx, bson.D{
			{Key: "replSetInitiate", Value: config},
		}).Raw(); err != nil {
			return fmt.Errorf("initialize replica set: %w", err)
		}

		m.logger.Info("replica set initialized")
		return nil

	case unauthorizedCode:
		m.logger.Info(
			"received unauthorized error checking replica set status; assuming admin user exists and bootstrap is complete",
		)
		return nil

	default:
		return fmt.Errorf(
			"check replica set status (%s, %d: %s): %w",
			cmdErr.Name,
			cmdErr.Code,
			cmdErr.Message,
			err,
		)
	}
}
