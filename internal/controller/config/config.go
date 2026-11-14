package config

import "os"

type Config struct {
	ServiceAccountNamespace string
	ServiceAccountName      string
}

func LoadConfig() *Config {
	return &Config{
		ServiceAccountNamespace: os.Getenv("POD_NAMESPACE"),
		ServiceAccountName:      os.Getenv("SERVICE_ACCOUNT_NAME"),
	}
}
