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
	// Returns the changes (nil slice if none are available) and the maximum
	// position observed in the source at poll time. Callers can use the
	// maxPosition to advance the cursor when no changes are returned, avoiding
	// repeated rescans of the same range.
	PollChanges(ctx context.Context, fromPosition []byte, batchSize uint32) (changes []*proto.ChangeRequest, maxPosition []byte, err error)

	// Close closes the database connection and releases resources.
	Close() error
}
