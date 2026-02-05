// Package common provides shared types and utilities for the CDC sync system.
package common

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
)

// Lsn represents an MSSQL CDC Log Sequence Number (LSN).
// LSN is a 10-byte value used to track change positions.
type Lsn []byte

// NewLsn creates a new LSN from bytes.
func NewLsn(bytes []byte) Lsn {
	return Lsn(bytes)
}

// ToHexString formats LSN as hex string for logging.
func (l Lsn) ToHexString() string {
	return strings.ToUpper(hex.EncodeToString(l))
}

// FromHexString parses LSN from hex string (format: 0x00000000000000000000).
func LsnFromHexString(hexStr string) (Lsn, error) {
	hexStr = strings.TrimPrefix(hexStr, "0x")
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, err
	}
	return Lsn(bytes), nil
}

// Bytes returns the underlying byte slice.
func (l Lsn) Bytes() []byte {
	return []byte(l)
}

// MarshalJSON implements json.Marshaler for LSN (serializes as hex string).
func (l Lsn) MarshalJSON() ([]byte, error) {
	return json.Marshal(l.ToHexString())
}

// UnmarshalJSON implements json.Unmarshaler for LSN (deserializes from hex string).
func (l *Lsn) UnmarshalJSON(data []byte) error {
	var hexStr string
	if err := json.Unmarshal(data, &hexStr); err != nil {
		return err
	}
	lsn, err := LsnFromHexString(hexStr)
	if err != nil {
		return err
	}
	*l = lsn
	return nil
}

// TrackedTable represents a tracked table configuration for CDC.
type TrackedTable struct {
	DatabaseName      string            `json:"database_name" toml:"database_name"`
	SourceSchema      string            `json:"source_schema" toml:"source_schema"`
	SourceTable       string            `json:"source_table" toml:"source_table"`
	TargetSchema      string            `json:"target_schema" toml:"target_schema"`
	TargetTable       string            `json:"target_table" toml:"target_table"`
	PrimaryKeyColumns []string          `json:"primary_key_columns,omitempty" toml:"primary_key_columns"`
	ColumnMappings    map[string]string `json:"column_mappings,omitempty" toml:"column_mappings"`
}

// CdcCursorState represents CDC cursor state persisted to track sync progress.
type CdcCursorState struct {
	TableName        string         `json:"table_name"`
	LastProcessedLsn Lsn            `json:"last_processed_lsn"`
	LastAckedLsn     *Lsn           `json:"last_acked_lsn,omitempty"`
	PendingBatches   []PendingBatch `json:"pending_batches"`
	UpdatedAt        time.Time      `json:"updated_at"`
}

// PendingBatch represents a batch that has been sent but not yet acknowledged.
type PendingBatch struct {
	BatchID       string    `json:"batch_id"`
	Lsn           Lsn       `json:"lsn"`
	SequenceCount uint64    `json:"sequence_count"`
	SentAt        time.Time `json:"sent_at"`
}

// RetryConfig contains configuration for connection retry behavior.
type RetryConfig struct {
	MaxRetries      uint32  `json:"max_retries" toml:"max_retries"`
	InitialDelayMs  uint64  `json:"initial_delay_ms" toml:"initial_delay_ms"`
	MaxDelayMs      uint64  `json:"max_delay_ms" toml:"max_delay_ms"`
	ExponentialBase float64 `json:"exponential_base" toml:"exponential_base"`
}

// DefaultRetryConfig returns a default retry configuration.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:      5,
		InitialDelayMs:  100,
		MaxDelayMs:      30000,
		ExponentialBase: 2.0,
	}
}

// DelayForAttempt calculates delay for a given retry attempt.
func (r *RetryConfig) DelayForAttempt(attempt uint32) time.Duration {
	delay := float64(r.InitialDelayMs)
	for i := uint32(0); i < attempt; i++ {
		delay *= r.ExponentialBase
	}
	if delay > float64(r.MaxDelayMs) {
		delay = float64(r.MaxDelayMs)
	}
	return time.Duration(delay) * time.Millisecond
}

// SyncMetrics contains metrics for monitoring sync performance.
type SyncMetrics struct {
	ChangesProcessed  uint64     `json:"changes_processed"`
	BatchesSent       uint64     `json:"batches_sent"`
	BatchesAcked      uint64     `json:"batches_acked"`
	BytesTransferred  uint64     `json:"bytes_transferred"`
	ErrorsEncountered uint64     `json:"errors_encountered"`
	LastSyncTimestamp *time.Time `json:"last_sync_timestamp,omitempty"`
}
