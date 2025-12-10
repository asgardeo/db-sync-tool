// Package server provides the CDC server implementation.
package server

import (
	"fmt"
	"os"
	"strconv"

	"github.com/pelletier/go-toml/v2"
)

// Config holds the server configuration.
type Config struct {
	// PostgresConnectionString is the PostgreSQL connection string
	PostgresConnectionString string `toml:"postgres_connection_string"`

	// ListenAddr is the address to listen on (e.g., "0.0.0.0:50051")
	ListenAddr string `toml:"listen_addr"`

	// MaxConcurrentBatches is the maximum concurrent batch processing
	MaxConcurrentBatches int `toml:"max_concurrent_batches"`

	// TLS is the TLS configuration
	TLS TLSConfig `toml:"tls"`

	// SchemaMappings contains schema mapping overrides
	SchemaMappings []SchemaMapping `toml:"schema_mappings"`

	// IdempotentWrites enables idempotent upserts (recommended)
	IdempotentWrites bool `toml:"idempotent_writes"`
}

// TLSConfig holds TLS configuration for the server.
type TLSConfig struct {
	// CertPath is the path to server certificate
	CertPath *string `toml:"cert_path"`

	// KeyPath is the path to server private key
	KeyPath *string `toml:"key_path"`

	// CACertPath is the path to CA certificate (for mTLS client verification)
	CACertPath *string `toml:"ca_cert_path"`
}

// SchemaMapping represents a source to target schema mapping.
type SchemaMapping struct {
	SourceSchema string `toml:"source_schema"`
	TargetSchema string `toml:"target_schema"`
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() Config {
	return Config{
		ListenAddr:           "0.0.0.0:50051",
		MaxConcurrentBatches: 4,
		IdempotentWrites:     true,
	}
}

// LoadConfig loads the server configuration from file and environment.
func LoadConfig() (*Config, error) {
	config := DefaultConfig()

	// Determine config file path
	configFile := "config/server.toml"
	if override := os.Getenv("SERVER_CONFIG_FILE"); override != "" {
		configFile = override + ".toml"
	}

	// Load from config file if it exists
	if data, err := os.ReadFile(configFile); err == nil {
		if err := toml.Unmarshal(data, &config); err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}
	}

	// Override with environment variables
	if v := os.Getenv("SERVER__POSTGRES_CONNECTION_STRING"); v != "" {
		config.PostgresConnectionString = v
	}
	if v := os.Getenv("SERVER__LISTEN_ADDR"); v != "" {
		config.ListenAddr = v
	}
	if v := os.Getenv("SERVER__MAX_CONCURRENT_BATCHES"); v != "" {
		if val, err := strconv.Atoi(v); err == nil {
			config.MaxConcurrentBatches = val
		}
	}
	if v := os.Getenv("SERVER__IDEMPOTENT_WRITES"); v != "" {
		config.IdempotentWrites = v == "true" || v == "1"
	}

	// Validate required fields
	if config.PostgresConnectionString == "" {
		return nil, fmt.Errorf("postgres_connection_string is required")
	}

	return &config, nil
}

// MapSchema returns the target schema for a given source schema.
func (c *Config) MapSchema(sourceSchema string) string {
	for _, m := range c.SchemaMappings {
		if m.SourceSchema == sourceSchema {
			return m.TargetSchema
		}
	}
	return sourceSchema
}
