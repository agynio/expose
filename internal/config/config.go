package config

import (
	"fmt"
	"os"
	"time"
)

type Config struct {
	GRPCAddress            string
	HTTPAddress            string
	DatabaseURL            string
	ZitiManagementAddress  string
	RunnersAddress         string
	NotificationsAddress   string
	AuthorizationAddress   string
	ReconciliationInterval time.Duration
	DebugEndpointsEnabled  bool
	DebugToken             string
}

func FromEnv() (Config, error) {
	cfg := Config{}
	cfg.GRPCAddress = os.Getenv("GRPC_ADDRESS")
	if cfg.GRPCAddress == "" {
		cfg.GRPCAddress = ":50051"
	}
	cfg.HTTPAddress = os.Getenv("HTTP_ADDRESS")
	if cfg.HTTPAddress == "" {
		cfg.HTTPAddress = ":8080"
	}
	cfg.DatabaseURL = os.Getenv("DATABASE_URL")
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL must be set")
	}
	cfg.ZitiManagementAddress = os.Getenv("ZITI_MANAGEMENT_ADDRESS")
	if cfg.ZitiManagementAddress == "" {
		cfg.ZitiManagementAddress = "ziti-management:50051"
	}
	cfg.RunnersAddress = os.Getenv("RUNNERS_ADDRESS")
	if cfg.RunnersAddress == "" {
		cfg.RunnersAddress = "runners:50051"
	}
	cfg.NotificationsAddress = os.Getenv("NOTIFICATIONS_ADDRESS")
	if cfg.NotificationsAddress == "" {
		cfg.NotificationsAddress = "notifications:50051"
	}
	cfg.AuthorizationAddress = os.Getenv("AUTHORIZATION_ADDRESS")
	if cfg.AuthorizationAddress == "" {
		cfg.AuthorizationAddress = "authorization:50051"
	}
	interval, err := durationFromEnv("RECONCILIATION_INTERVAL", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.ReconciliationInterval = interval
	cfg.DebugEndpointsEnabled = boolFromEnv("EXPOSE_DEBUG_ENDPOINTS")
	cfg.DebugToken = os.Getenv("EXPOSE_DEBUG_TOKEN")
	if cfg.DebugEndpointsEnabled && cfg.DebugToken == "" {
		return Config{}, fmt.Errorf("EXPOSE_DEBUG_TOKEN must be set when EXPOSE_DEBUG_ENDPOINTS is enabled")
	}
	return cfg, nil
}

func boolFromEnv(key string) bool {
	value := os.Getenv(key)
	return value == "1" || value == "true" || value == "TRUE" || value == "yes" || value == "YES"
}

func durationFromEnv(key string, defaultValue time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration: %w", key, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be greater than 0", key)
	}
	return parsed, nil
}
