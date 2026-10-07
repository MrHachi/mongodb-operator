package mongodb

import (
	"context"
	"fmt"
	"net/url"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Topology struct {
	Members []Member `bson:"members" json:"members"`
}

type Member struct {
	ID        int    `bson:"_id" json:"id"`
	Host      string `bson:"name" json:"host"`
	State     string `bson:"stateStr" json:"state"`
	StateCode int    `bson:"state" json:"state_code"`
}

type Client struct {
	client *mongo.Client
	uri    string
}

func NewClient(ctx context.Context, uri string) (*Client, error) {
	client, err := mongo.Connect(ctx,
		options.Client().
			ApplyURI(uri).
			SetDirect(true), // connect directly to the instance when using the localhost client
	)
	if err != nil {
		return nil, fmt.Errorf("connect to mongodb: %w", err)
	}
	return &Client{client: client, uri: uri}, nil
}

func (c *Client) Close(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}

func (c *Client) Ping(ctx context.Context) error {
	if err := c.client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("ping mongodb: %w", err)
	}
	return nil
}

// Authenticate verifies the provided credentials and returns a new connection URI.
func (c *Client) Authenticate(ctx context.Context, username, password, authSource string) (string, error) {
	u, err := url.Parse(c.uri)
	if err != nil {
		return "", fmt.Errorf("parse uri: %w", err)
	}
	newURI := u.String()

	credential := options.Credential{
		AuthSource: authSource,
		Username:   username,
		Password:   password,
	}

	client, err := mongo.Connect(
		ctx,
		options.Client().
			ApplyURI(newURI).
			SetAuth(credential),
	)
	if err != nil {
		return "", fmt.Errorf("connect to mongodb with new credentials: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(ctx)
		return "", fmt.Errorf("authentication failed: %w", err)
	}

	return newURI, nil
}

// InitiateReplicaSet triggers the MongoDB rs.initiate() command.
func (c *Client) InitiateReplicaSet(ctx context.Context, rsName, host string) error {
	// This operation requires the admin DB
	db := c.client.Database("admin")

	config := bson.D{
		{Key: "_id", Value: rsName},
		{Key: "members", Value: bson.A{
			bson.D{{Key: "_id", Value: 0}, {Key: "host", Value: host}},
		}},
	}

	err := db.RunCommand(ctx, bson.D{
		{Key: "replSetInitiate", Value: config},
	}).Err()
	if err != nil {
		return fmt.Errorf("initiate replica set: %w", err)
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
		// Let's check if it's a "user already exists" error.
		// For now, I'll just return the error but the handler will handle it.
		return fmt.Errorf("create admin user: %w", err)
	}
	return nil
}

// GetTopology retrieves the cluster topology from MongoDB.
func (c *Client) GetTopology(ctx context.Context) (*Topology, error) {
	db := c.client.Database("admin")

	command := bson.D{
		{Key: "replSetGetStatus", Value: 1},
	}

	var topology Topology
	if err := db.RunCommand(ctx, command).Decode(&topology); err != nil {
		return nil, fmt.Errorf("cluster not initialized: %w", err)
	}

	return &topology, nil
}
