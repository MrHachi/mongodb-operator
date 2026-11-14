package mongodb

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Topology struct {
	Members       []Member `bson:"members" json:"members"`
	ConfigVersion int32    `bson:"configVersion" json:"config_version"`
}

type Member struct {
	ID        int    `bson:"_id" json:"id"`
	Host      string `bson:"name" json:"host"`
	State     string `bson:"stateStr" json:"state"`
	StateCode int    `bson:"state" json:"state_code"`

	Optimes MemberOptimes `bson:",inline" json:"optimes"`

	ReplicationLag time.Duration `bson:"-" json:"replication_lag,omitempty"`
	LagAvailable   bool          `bson:"-" json:"lag_available"`
}

// MemberOptimes groups the observed applied and durable replication positions.
type MemberOptimes struct {
	AppliedOptime Optime    `bson:"optime" json:"applied"`
	DurableOptime Optime    `bson:"optimeDurable" json:"durable"`
	AppliedAt     time.Time `bson:"optimeDate" json:"applied_at"`
	DurableAt     time.Time `bson:"optimeDurableDate" json:"durable_at"`
}

// MemberConfig groups the member's voting and election configuration.
type MemberConfig struct {
	Votes          *int32   `bson:"votes,omitempty" json:"votes,omitempty"`
	Priority       *float64 `bson:"priority,omitempty" json:"priority,omitempty"`
	ArbiterOnly    bool     `bson:"arbiterOnly,omitempty" json:"arbiter_only,omitempty"`
	Hidden         bool     `bson:"hidden,omitempty" json:"hidden,omitempty"`
	VotingEligible bool     `bson:"-" json:"voting_eligible,omitempty"`
	ConfigVersion  int32    `bson:"configVersion" json:"version"`
}

// ReplicaSetConfig describes the current voting and election configuration.
type ReplicaSetConfig struct {
	Version int32                    `json:"version"`
	Members []ReplicaSetConfigMember `json:"members"`
}

// ReplicaSetConfigMember associates a member ID with its configured settings.
type ReplicaSetConfigMember struct {
	ID     int          `json:"id"`
	Config MemberConfig `json:"config"`
}

// Optime identifies an operation in the replica set oplog.
type Optime struct {
	Timestamp primitive.Timestamp `bson:"ts" json:"timestamp"`
	Term      int64               `bson:"t" json:"term"`
}
