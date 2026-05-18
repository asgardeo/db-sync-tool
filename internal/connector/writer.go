package connector

import (
	"context"

	"github.com/wso2/db-sync-tool/proto"
)

// SQLWriter defines the interface for writing changes to a target database.
// Implementations should handle database-specific SQL dialects and transaction management.
type SQLWriter interface {
	// ApplyChanges applies a batch of changes within a transaction.
	// If idempotent is true, uses upsert semantics for inserts/updates.
	// Returns the number of rows affected.
	ApplyChanges(ctx context.Context, changes []*proto.ChangeRequest, idempotent bool) (uint64, error)

	// IsHealthy checks if the database connection is healthy.
	IsHealthy(ctx context.Context) bool

	// Close closes the connection pool and releases resources.
	Close()
}
