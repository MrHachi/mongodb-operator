package config

import (
	"os"
)

const InstanceManagerAudience = "db.mrhachi.dev/mongodb/instance-manager"

const DefaultMongoURI = "mongodb://127.0.0.1:27017"
const DefaultInstanceManagerPasswordFile = "/etc/mongodb-operator/credentials/password"

type Config struct {
	RSName                          string
	Hostname                        string
	ServiceName                     string
	Namespace                       string
	OperatorServiceAccountName      string
	OperatorServiceAccountNamespace string
	MongoURI                        string
	InstanceManagerPasswordFile     string
}

func LoadConfig() *Config {
	return &Config{
		RSName:                          getEnv("MONGODB_RS_NAME", ""),
		Hostname:                        getEnv("MONGODB_HOSTNAME", ""),
		ServiceName:                     getEnv("MONGODB_SERVICE_NAME", ""),
		Namespace:                       getEnv("MONGODB_NAMESPACE", ""),
		OperatorServiceAccountName:      getEnv("OPERATOR_SERVICE_ACCOUNT_NAME", ""),
		OperatorServiceAccountNamespace: getEnv("OPERATOR_SERVICE_ACCOUNT_NAMESPACE", ""),
		MongoURI:                        getEnv("MONGODB_URI", DefaultMongoURI),
		InstanceManagerPasswordFile:     getEnv("INSTANCE_MANAGER_PASSWORD_FILE", DefaultInstanceManagerPasswordFile),
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
