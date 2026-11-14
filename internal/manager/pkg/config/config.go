package config

import (
	"os"
)

const InstanceManagerAudience = "db.mrhachi.dev/mongodb/instance-manager"

type Config struct {
	RSName                          string
	Hostname                        string
	ServiceName                     string
	Namespace                       string
	OperatorServiceAccountName      string
	OperatorServiceAccountNamespace string
}

func LoadConfig() *Config {
	return &Config{
		RSName:                          getEnv("MONGODB_RS_NAME", ""),
		Hostname:                        getEnv("MONGODB_HOSTNAME", ""),
		ServiceName:                     getEnv("MONGODB_SERVICE_NAME", ""),
		Namespace:                       getEnv("MONGODB_NAMESPACE", ""),
		OperatorServiceAccountName:      getEnv("OPERATOR_SERVICE_ACCOUNT_NAME", ""),
		OperatorServiceAccountNamespace: getEnv("OPERATOR_SERVICE_ACCOUNT_NAMESPACE", ""),
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
