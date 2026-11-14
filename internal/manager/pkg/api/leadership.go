package api

// NoLeader is a placeholder identity used until instance-manager leadership is implemented.
const NoLeader = "-r-0"

// LeadershipStatus reports the current instance-manager leadership identity.
type LeadershipStatus struct {
	Leader string `json:"leader"`
}
