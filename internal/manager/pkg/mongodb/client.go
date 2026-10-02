package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Client struct {
	client *mongo.Client
}

func NewClient(ctx context.Context, uri string) (*Client, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	return &Client{client: client}, nil
}

func (c *Client) Close(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}

// InitiateReplicaSet triggers the MongoDB rs.initivate() command.
func (c *Client) InitiateReplicaSet(ctx context.Context, rsName, host string) error {
	db := c.client.Database("admin")

	command := bson.D{
		{Key: "_id", Value: rsName},
		{Key: "members", Value: bson.A{
			bson.D{{Key: "_id", Value: 0}, {Key: "host", Value: host}},
		}},
	}

	err := db.RunCommand(ctx, command).Err()
	if err != nil {
		return fmt.Errorf("failed to initiate replica set: %w", err)
	}
	return nil
}

// CreateAdminUser creates the administrative user for the database.
func (c *Client) CreateAdminUser(ctx context.Context, username, password string) error {
	db := c.client.Database("admin")

	command := bson.D{
		{Key: "createUser", Value: username},
		{Key: "pwd", Value: password},
		{Key: "roles", Value: bson.A{
			bson.D{{Key: "role", Value: "root"}, {Key: "db", Value: "admin"}},
		}},
	}

	if err := db.RunCommand(ctx, command).Err(); err != nil {
		// Check if error is because user already exists
		// In MongoDB, error code for duplicate key is 11000, but for createUser it might be different.
		// According to Mongo error codes, duplicate key error is 11000.
		// Let's check if it's a "user already exists" error.
		// For now, I'll just return the error but the handler will handle it.
		return err
	}
	return nil
}
