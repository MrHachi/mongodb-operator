package mongo

import (
	"context"
	"fmt"
	"slices"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoRSConf struct {
	ID      string     `bson:"_id"`
	Version int        `bson:"version"`
	Members []RSMember `bson:"members"`
}

type MongoUser struct {
	Username string `bson:"user"`
	Password string
	Roles    []MongoRole `bson:"roles"`
}

type MongoRole struct {
	Role     string `bson:"role"`
	Database string `bson:"db"`
}

func MakeMongoUser(username, password string, roles []MongoRole) MongoUser {
	return MongoUser{
		Username: username,
		Password: password,
		Roles:    roles,
	}
}

func MakeMongoRole(role, database string) MongoRole {
	return MongoRole{
		Role:     role,
		Database: database,
	}
}

type replSetGetConfigResponse struct {
	Config MongoRSConf `bson:"config"`
}

type mongoCommandResultResponse struct {
	Ok       int    `bson:"ok"`
	Code     int    `bson:"code"`
	CodeName string `bson:"codeName"`
}

type MongoRSManager struct {
	client *mongo.Client
	uri    string
}

func NewMongoRSManager(ctx context.Context, uri string) (*MongoRSManager, error) {
	client, err := mongo.Connect(
		options.Client().ApplyURI(uri),
	)
	if err != nil {
		return nil, fmt.Errorf("initialize mongo rs manager: %w", err)
	}

	return &MongoRSManager{client: client, uri: uri}, nil
}

// Returns nil with a nil error if DB needs initiation
func (mrm *MongoRSManager) getConfig(
	ctx context.Context,
) (*MongoRSConf, error) {
	db := mrm.client.Database("admin")

	result := db.RunCommand(
		ctx,
		bson.D{{Key: "replSetGetConfig", Value: 1}},
	)

	var getCfgResp replSetGetConfigResponse
	if err := result.Decode(&getCfgResp); err == nil {
		return &getCfgResp.Config, nil
	}
	var cmdResp mongoCommandResultResponse
	if err := result.Decode(&cmdResp); err == nil {
		if cmdResp.Code != 94 {
			return nil, fmt.Errorf("unexpected response: code: %d, name: %s, ok?: %d",
				cmdResp.Code, cmdResp.CodeName, cmdResp.Ok,
			)
		}
		return nil, nil
	} else {
		return nil, fmt.Errorf("decode response: %w", err)
	}
}

func (mrm *MongoRSManager) ReconcileUsers(ctx context.Context, db string, users []MongoUser) error {
	existing, err := mrm.listUsers(ctx, db)
	if err != nil {
		return fmt.Errorf("list users: %w", err)
	}

	existingByName := make(map[string]MongoUser, len(existing))
	for _, user := range existing {
		existingByName[user.Username] = user
	}

	for _, desired := range users {
		current, exists := existingByName[desired.Username]

		if !exists {
			if err := mrm.createUser(ctx, db, desired); err != nil {
				return fmt.Errorf("create user %s: %w", desired.Username, err)
			}
			continue
		}

		if err := mrm.syncUser(ctx, db, current, desired); err != nil {
			return fmt.Errorf("sync user %s: %w", desired.Username, err)
		}
	}

	return nil
}

func (mrm *MongoRSManager) listUsers(
	ctx context.Context,
	dbName string,
) ([]MongoUser, error) {
	db := mrm.client.Database(dbName)

	var result struct {
		Users []MongoUser `bson:"users"`
	}

	err := db.RunCommand(ctx, bson.D{
		{Key: "usersInfo", Value: 1},
	}).Decode(&result)

	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}

	return result.Users, nil
}

func (mrm *MongoRSManager) syncUser(
	ctx context.Context,
	db string,
	before, after MongoUser,
) error {

	// This is hard, potentially dangerouss
	// if before.Username != after.Username {
	// 	if err := mrm.renameUser(ctx, db, before.Username, after.Username); err != nil {
	// 		return fmt.Errorf("rename user: %w", err)
	// 	}
	// }

	if before.Password != after.Password || !slices.Equal(before.Roles, after.Roles) {
		if err := mrm.updateUser(ctx, db, after); err != nil {
			return fmt.Errorf("update user: %w", err)
		}
	}

	return nil
}

func (mrm *MongoRSManager) createUser(ctx context.Context, db string, after MongoUser) error {
	roles := make(bson.A, 0, len(after.Roles))
	for _, r := range after.Roles {
		roles = append(roles, r)
	}

	return mrm.client.Database(db).RunCommand(ctx, bson.D{
		{Key: "createUser", Value: after.Username},
		{Key: "pwd", Value: after.Password},
		{Key: "roles", Value: roles},
	}).Err()
}

// func (mrm *MongoRSManager) renameUser(ctx context.Context, db, before, after string) error {
// 	return mrm.client.Database(db).RunCommand(ctx, bson.D{
// 		{Key: "renameUser", Value: before},
// 		{Key: "to", Value: db + "." + after},
// 	}).Err()
// }

func (mrm *MongoRSManager) updateUser(ctx context.Context, db string, after MongoUser) error {
	roles := make(bson.A, 0, len(after.Roles))
	for _, r := range after.Roles {
		roles = append(roles, r)
	}

	return mrm.client.Database(db).RunCommand(ctx, bson.D{
		{Key: "updateUser", Value: after.Username},
		{Key: "pwd", Value: after.Password},
		{Key: "roles", Value: roles},
	}).Err()
}

func (mrm *MongoRSManager) Reconfigure(
	ctx context.Context,
	members []RSMember,
) error {
	getcfgctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	cfg, err := mrm.getConfig(getcfgctx)
	cancel()
	if err != nil {
		return fmt.Errorf("get config: %w", err)
	}

	currentMembers := cfg.Members
	if allRSMembersEqualEnough(members, currentMembers) {
		return nil
	}

	cfg.Members = members
	cfg.Version++
	return mrm.client.Database("admin").
		RunCommand(
			ctx,
			bson.D{
				{Key: "replSetReconfig", Value: cfg},
			},
		).Err()
}

func (mrm *MongoRSManager) Ping(ctx context.Context) error {
	return mrm.client.Ping(ctx, nil)
}

func rsMembersEqualEnough(A, B RSMember) bool {
	return A.Host == B.Host && A.ID == B.ID
}

func allRSMembersEqualEnough(A, B []RSMember) bool {
	a := slices.Clone(A)
	b := slices.Clone(B)
	slices.SortFunc(a, compareRSMembers)
	slices.SortFunc(b, compareRSMembers)
	return slices.EqualFunc(a, b, rsMembersEqualEnough)
}
