// Package connector defines common interfaces for database connectors.
// This enables pluggable CDC readers and SQL writers for different database systems.
package connector

import (
	"context"

	"github.com/wso2/db-sync-tool/proto"
)

// CDCReader defines the interface for reading CDC changes from a source database.
// Implementations should handle database-specific CDC mechanisms (e.g., MSSQL CDC tables).
type CDCReader interface {
	// PollChanges polls for CDC changes starting from the given position.
	// The fromPosition is database-specific (e.g., LSN for MSSQL).
	// Returns nil slice if no changes are available.
	PollChanges(ctx context.Context, fromPosition []byte, batchSize uint32) ([]*proto.ChangeRequest, error)

	// Close closes the database connection and releases resources.
	Close() error
}
