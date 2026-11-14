package mongodb

import (
	"context"
	"fmt"
	"net/url"
	"time"

	mdb "github.com/MrHachi/mongodb-operator/internal/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

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
func (c *Client) GetTopology(ctx context.Context) (*mdb.Topology, error) {
	db := c.client.Database("admin")

	command := bson.D{
		{Key: "replSetGetStatus", Value: 1},
	}

	var topology mdb.Topology
	if err := db.RunCommand(ctx, command).Decode(&topology); err != nil {
		return nil, fmt.Errorf("cluster not initialized: %w", err)
	}

	var primaryAppliedAt time.Time
	for _, member := range topology.Members {
		if member.State == mdb.StatePrimary {
			primaryAppliedAt = member.Optimes.AppliedAt
			break
		}
	}

	if !primaryAppliedAt.IsZero() {
		for i := range topology.Members {
			member := &topology.Members[i]
			if member.State != mdb.StateSecondary || member.Optimes.AppliedAt.IsZero() {
				continue
			}
			member.ReplicationLag = max(primaryAppliedAt.Sub(member.Optimes.AppliedAt), 0)
			member.LagAvailable = true
		}
	}
	return &topology, nil
}

// GetConfig retrieves the replica set voting and election configuration.
func (c *Client) GetConfig(ctx context.Context) (*mdb.ReplicaSetConfig, error) {
	db := c.client.Database("admin")
	var configResponse struct {
		Config struct {
			Version int32 `bson:"version"`
			Members []struct {
				ID               int `bson:"_id"`
				mdb.MemberConfig `bson:",inline"`
			} `bson:"members"`
		} `bson:"config"`
	}
	if err := db.RunCommand(ctx, bson.D{{Key: "replSetGetConfig", Value: 1}}).Decode(&configResponse); err != nil {
		return nil, fmt.Errorf("cluster not initialized: get replica set configuration: %w", err)
	}
	config := &mdb.ReplicaSetConfig{Version: configResponse.Config.Version, Members: make([]mdb.ReplicaSetConfigMember, 0, len(configResponse.Config.Members))}
	for _, member := range configResponse.Config.Members {
		if member.Votes == nil {
			votes := int32(1)
			member.Votes = &votes
		}
		if member.Priority == nil {
			priority := 1.0
			if member.ArbiterOnly {
				priority = 0
			}
			member.Priority = &priority
		}
		member.ConfigVersion = configResponse.Config.Version
		member.VotingEligible = *member.Votes > 0 && !member.ArbiterOnly && !member.Hidden && *member.Priority > 0
		config.Members = append(config.Members, mdb.ReplicaSetConfigMember{ID: member.ID, Config: member.MemberConfig})
	}
	return config, nil
}

// RemoveMember removes the configured replica-set member with the given host.
// It is idempotent when the host is already absent. The caller must connect to
// the current PRIMARY; this method never forces reconfiguration.
func (c *Client) RemoveMember(ctx context.Context, host string) error {
	db := c.client.Database("admin")
	var response struct {
		Config bson.M `bson:"config"`
	}
	if err := db.RunCommand(ctx, bson.D{{Key: "replSetGetConfig", Value: 1}}).Decode(&response); err != nil {
		return fmt.Errorf("get replica set configuration before removing member: %w", err)
	}
	if response.Config == nil {
		return fmt.Errorf("replica set configuration is missing")
	}
	members, ok := response.Config["members"].(bson.A)
	if !ok {
		return fmt.Errorf("replica set configuration members have unexpected type %T", response.Config["members"])
	}
	filtered := make(bson.A, 0, len(members))
	removed := false
	for _, value := range members {
		member, ok := value.(bson.M)
		if !ok {
			return fmt.Errorf("replica set member has unexpected type %T", value)
		}
		memberHost, ok := member["host"].(string)
		if !ok {
			return fmt.Errorf("replica set member host has unexpected type %T", member["host"])
		}
		if memberHost == host {
			removed = true
			continue
		}
		filtered = append(filtered, member)
	}
	if !removed {
		return nil
	}

	version, ok := response.Config["version"].(int32)
	if !ok {
		return fmt.Errorf("replica set configuration version has unexpected type %T", response.Config["version"])
	}
	response.Config["members"] = filtered
	response.Config["version"] = version + 1
	if err := db.RunCommand(ctx, bson.D{{Key: "replSetReconfig", Value: response.Config}}).Err(); err != nil {
		return fmt.Errorf("reconfigure replica set without member %q: %w", host, err)
	}
	return nil
}
