// Package client provides the CDC client implementation.
package client

import (
	"fmt"
	"os"
	"strconv"

	"github.com/pelletier/go-toml/v2"
	"github.com/wso2/db-sync-tool/internal/common"
)

// Config holds the client configuration.
type Config struct {
	// MssqlConnectionString is the MSSQL connection string
	MssqlConnectionString string `toml:"mssql_connection_string"`

	// ServerURL is the CDC Server gRPC URL (e.g., "localhost:50051")
	ServerURL string `toml:"server_url"`

	// StateFilePath is the path to state file for cursor persistence
	StateFilePath string `toml:"state_file_path"`

	// PollIntervalMs is the polling interval in milliseconds when no changes are found
	PollIntervalMs uint64 `toml:"poll_interval_ms"`

	// BatchSize is the maximum number of changes to process per batch
	BatchSize uint32 `toml:"batch_size"`

	// TLS is the TLS configuration
	TLS TLSConfig `toml:"tls"`

	// TrackedTables are the tables to track for CDC
	TrackedTables []common.TrackedTable `toml:"tracked_tables"`

	// AckTimeoutSecs is the acknowledgment timeout in seconds
	AckTimeoutSecs uint64 `toml:"ack_timeout_secs"`
}

// TLSConfig holds TLS configuration for the client.
type TLSConfig struct {
	// CACertPath is the path to CA certificate for verifying the server
	CACertPath *string `toml:"ca_cert_path"`

	// ClientCertPath is the path to client certificate (for mTLS)
	ClientCertPath *string `toml:"client_cert_path"`

	// ClientKeyPath is the path to client private key (for mTLS)
	ClientKeyPath *string `toml:"client_key_path"`

	// DomainName is the server domain name for TLS verification
	DomainName *string `toml:"domain_name"`
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() Config {
	return Config{
		StateFilePath:  "./data/cdc_cursor_state.json",
		PollIntervalMs: 1000,
		BatchSize:      1000,
		AckTimeoutSecs: 30,
	}
}

// LoadConfig loads the client configuration from file and environment.
func LoadConfig() (*Config, error) {
	config := DefaultConfig()

	// Determine config file path
	configFile := "config/client.toml"
	if override := os.Getenv("CLIENT_CONFIG_FILE"); override != "" {
		configFile = override + ".toml"
	}

	// Load from config file if it exists
	if data, err := os.ReadFile(configFile); err == nil {
		if err := toml.Unmarshal(data, &config); err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}
	}

	// Override with environment variables
	if v := os.Getenv("CLIENT__MSSQL_CONNECTION_STRING"); v != "" {
		config.MssqlConnectionString = v
	}
	if v := os.Getenv("CLIENT__SERVER_URL"); v != "" {
		config.ServerURL = v
	}
	if v := os.Getenv("CLIENT__STATE_FILE_PATH"); v != "" {
		config.StateFilePath = v
	}
	if v := os.Getenv("CLIENT__POLL_INTERVAL_MS"); v != "" {
		if val, err := strconv.ParseUint(v, 10, 64); err == nil {
			config.PollIntervalMs = val
		}
	}
	if v := os.Getenv("CLIENT__BATCH_SIZE"); v != "" {
		if val, err := strconv.ParseUint(v, 10, 32); err == nil {
			config.BatchSize = uint32(val)
		}
	}
	if v := os.Getenv("CLIENT__ACK_TIMEOUT_SECS"); v != "" {
		if val, err := strconv.ParseUint(v, 10, 64); err == nil {
			config.AckTimeoutSecs = val
		}
	}

	// Validate required fields
	if config.MssqlConnectionString == "" {
		return nil, fmt.Errorf("mssql_connection_string is required")
	}
	if config.ServerURL == "" {
		return nil, fmt.Errorf("server_url is required")
	}

	return &config, nil
}
