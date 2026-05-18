package client

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/wso2/db-sync-tool/internal/common"
	"github.com/wso2/db-sync-tool/internal/connector/mssql"
	"github.com/wso2/db-sync-tool/proto"
)

// CDCReader defines the interface for reading CDC changes.
// This allows the SyncOrchestrator to work with different database implementations.
type CDCReader interface {
	PollChanges(ctx context.Context, fromPosition []byte, batchSize uint32) ([]*proto.ChangeRequest, []byte, error)
	Close() error
}

// SyncOrchestrator orchestrates the CDC sync process.
type SyncOrchestrator struct {
	config       *Config
	stateManager *StateManager
	// cdcReaders is a map of source name -> reader
	cdcReaders map[string]CDCReader
	grpcClient *StreamingClient
	// tableMappings is a nested map: SourceName -> SourceSchema -> SourceTable -> TrackedTable
	tableMappings map[string]map[string]map[string]*common.TrackedTable
}

// NewSyncOrchestrator creates a new SyncOrchestrator with all required components.
func NewSyncOrchestrator(config *Config) (*SyncOrchestrator, error) {
	stateManager, err := NewStateManager(config.StateFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create state manager: %w", err)
	}

	cdcReaders := make(map[string]CDCReader)
	for _, source := range config.Sources {
		var cdcReader CDCReader
		switch source.Type {
		case "mssql":
			cdcReader, err = mssql.NewMSSQLReader(source.ConnectionString)
			if err != nil {
				return nil, fmt.Errorf("failed to create MSSQL CDC reader for source %s: %w", source.Name, err)
			}
		default:
			return nil, fmt.Errorf("unsupported reader type for source %s: %s", source.Name, source.Type)
		}
		cdcReaders[source.Name] = cdcReader
	}

	grpcClient, err := NewStreamingClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client: %w", err)
	}

	// Index tracked tables for O(1) lookup
	mappings := make(map[string]map[string]map[string]*common.TrackedTable)
	for i := range config.TrackedTables {
		tt := &config.TrackedTables[i]

		// Handle legacy config or missing database name by defaulting to the first source or "default"
		dbName := tt.DatabaseName
		if dbName == "" && len(config.Sources) > 0 {
			// Backward compatibility: use the first source name if only one exists
			if len(config.Sources) == 1 {
				dbName = config.Sources[0].Name
			} else {
				// If multiple sources, we can't guess. Use "default" or skip?
				// Better to iterate and try to match? No, explicit config is better.
				// Assume "default" as a fallback source name
				dbName = "default"
			}
		}

		if _, ok := mappings[dbName]; !ok {
			mappings[dbName] = make(map[string]map[string]*common.TrackedTable)
		}
		if _, ok := mappings[dbName][tt.SourceSchema]; !ok {
			mappings[dbName][tt.SourceSchema] = make(map[string]*common.TrackedTable)
		}
		mappings[dbName][tt.SourceSchema][tt.SourceTable] = tt
	}

	return &SyncOrchestrator{
		config:        config,
		stateManager:  stateManager,
		cdcReaders:    cdcReaders,
		grpcClient:    grpcClient,
		tableMappings: mappings,
	}, nil
}

// Close closes all resources.
func (o *SyncOrchestrator) Close() error {
	var errs []error
	for name, reader := range o.cdcReaders {
		if err := reader.Close(); err != nil {
			errs = append(errs, fmt.Errorf("source %s: %w", name, err))
		}
	}
	if o.grpcClient != nil {
		if err := o.grpcClient.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing resources: %v", errs)
	}
	return nil
}

// Run runs the main sync loop with retry logic.
func (o *SyncOrchestrator) Run(ctx context.Context) error {
	retryConfig := common.DefaultRetryConfig()
	var consecutiveErrors uint32

	// Wait for initial connection
	log.Info().Msg("Waiting for server connection...")
	for {
		connected, err := o.grpcClient.HealthCheck(ctx)
		if err == nil && connected {
			log.Info().Msg("Connected to server, starting polling")
			break
		}

		consecutiveErrors++
		// If we've been trying for a while, log a warning but keep trying
		delay := retryConfig.DelayForAttempt(consecutiveErrors)
		if err != nil {
			log.Warn().Err(err).Dur("retry_in", delay).Msg("Failed to connect to server")
		} else {
			log.Warn().Dur("retry_in", delay).Msg("Server not ready (health check failed)")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			// Continue trying
		}

		if err := o.grpcClient.Reconnect(); err != nil {
			log.Debug().Err(err).Msg("Reconnect attempt failed during startup")
		}
	}

	// Reset error count for main loop
	consecutiveErrors = 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		changesProcessed, err := o.syncCycle(ctx)
		if err != nil {
			consecutiveErrors++
			log.Error().
				Err(err).
				Uint32("consecutive_errors", consecutiveErrors).
				Msg("Sync cycle failed")

			if consecutiveErrors >= retryConfig.MaxRetries {
				log.Error().Msg("Max retries exceeded, exiting")
				return err
			}

			delay := retryConfig.DelayForAttempt(consecutiveErrors)
			log.Info().Dur("delay", delay).Msg("Backing off before retry")

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}

			if err := o.grpcClient.Reconnect(); err != nil {
				log.Error().Err(err).Msg("Reconnection failed")
			}
		} else {
			consecutiveErrors = 0

			if changesProcessed == 0 {
				// No changes, wait before polling again
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Duration(o.config.PollIntervalMs) * time.Millisecond):
				}
			}
		}
	}
}

func (o *SyncOrchestrator) syncCycle(ctx context.Context) (uint64, error) {
	var totalChangesProcessed uint64

	for _, source := range o.config.Sources {
		reader, ok := o.cdcReaders[source.Name]
		if !ok {
			log.Warn().Str("source", source.Name).Msg("No reader found for source config")
			continue
		}

		// Get the current cursor position
		cursor := o.stateManager.GetCursor(source.Name)
		var fromLsn *common.Lsn
		if cursor != nil {
			if cursor.LastAckedLsn != nil {
				fromLsn = cursor.LastAckedLsn
			} else {
				fromLsn = &cursor.LastProcessedLsn
			}
		}

		if fromLsn != nil {
			log.Debug().
				Str("source", source.Name).
				Str("from_lsn", fromLsn.ToHexString()).
				Msg("Polling CDC changes")
		} else {
			log.Debug().Str("source", source.Name).Msg("Polling CDC changes from beginning")
		}

		// Poll for changes from CDC source
		var fromPosition []byte
		if fromLsn != nil {
			fromPosition = fromLsn.Bytes()
		}
		changes, maxLsn, err := reader.PollChanges(ctx, fromPosition, o.config.BatchSize)
		if err != nil {
			return totalChangesProcessed, fmt.Errorf("failed to poll changes from %s: %w", source.Name, err)
		}

		if len(changes) == 0 {
			// If we have a maxLsn, update cursor to it to avoid re-polling from start
			if len(maxLsn) > 0 {
				log.Debug().
					Str("source", source.Name).
					Str("max_lsn", common.NewLsn(maxLsn).ToHexString()).
					Msg("No changes found, advancing cursor to max_lsn")

				dummyBatchID := "no-changes-" + uuid.New().String()
				if err := o.stateManager.UpdateCursor(source.Name, dummyBatchID, common.NewLsn(maxLsn)); err != nil {
					return totalChangesProcessed, fmt.Errorf("failed to update cursor for %s: %w", source.Name, err)
				}
			}
			continue
		}

		// Filter and map changes based on TrackedTables
		var validChanges []*proto.ChangeRequest
		for _, change := range changes {
			if mapping, ok := o.getTableMapping(source.Name, change.SchemaName, change.TableName); ok {
				change.SchemaName = mapping.TargetSchema
				change.TableName = mapping.TargetTable
				validChanges = append(validChanges, change)
			}
		}

		// Handle cursor advancement even if we filtered out all changes
		if len(changes) > 0 {
			lastLsnParam := changes[len(changes)-1].Lsn
			lastLsn := common.NewLsn(lastLsnParam)

			if len(validChanges) == 0 {
				// Manually update cursor
				log.Debug().Str("source", source.Name).Msg("Batch contained changes but none matched tracked tables. Advancing cursor locally.")
				dummyBatchID := "local-skip-" + uuid.New().String()
				if err := o.stateManager.UpdateCursor(source.Name, dummyBatchID, lastLsn); err != nil {
					return totalChangesProcessed, fmt.Errorf("failed to update local cursor for %s: %w", source.Name, err)
				}
				continue
			}

			batchID := uuid.New().String()
			changeCount := uint64(len(validChanges))

			log.Info().
				Str("source", source.Name).
				Str("batch_id", batchID).
				Uint64("change_count", changeCount).
				Msg("Streaming batch to server")

			if err := o.stateManager.RecordPendingBatch(source.Name, batchID, lastLsn, changeCount); err != nil {
				return totalChangesProcessed, fmt.Errorf("failed to record pending batch: %w", err)
			}

			// Stream changes and wait for acknowledgment
			ack, err := o.grpcClient.StreamBatch(ctx, batchID, validChanges)
			if err != nil {
				return totalChangesProcessed, fmt.Errorf("failed to stream batch for %s: %w", source.Name, err)
			}

			// Process the acknowledgment
			switch ack.Status {
			case proto.AckStatus_ACK_STATUS_OK:
				// Update cursor only after successful ack
				newLsn := common.NewLsn(ack.Lsn)
				if err := o.stateManager.UpdateCursor(source.Name, batchID, newLsn); err != nil {
					return totalChangesProcessed, fmt.Errorf("failed to update cursor: %w", err)
				}
				totalChangesProcessed += ack.RowsProcessed

			case proto.AckStatus_ACK_STATUS_PARTIAL:
				log.Warn().
					Str("source", source.Name).
					Str("batch_id", batchID).
					Uint64("last_sequence", ack.LastSequenceNumber).
					Msg("Partial acknowledgment received")

				// Update cursor to partial position
				newLsn := common.NewLsn(ack.Lsn)
				if err := o.stateManager.UpdateCursor(source.Name, batchID, newLsn); err != nil {
					return totalChangesProcessed, fmt.Errorf("failed to update cursor: %w", err)
				}
				totalChangesProcessed += ack.RowsProcessed

			case proto.AckStatus_ACK_STATUS_RETRY:
				// Retry logic is implicit in next cycle
				log.Info().Str("source", source.Name).Msg("Server requested retry")
				return totalChangesProcessed, nil

			default:
				errMsg := "unknown"
				if ack.ErrorMessage != nil {
					errMsg = *ack.ErrorMessage
				}
				log.Error().
					Str("source", source.Name).
					Str("batch_id", batchID).
					Str("error", errMsg).
					Msg("Batch processing failed")
				return totalChangesProcessed, fmt.Errorf("batch failed: %s", errMsg)
			}
		}
	}

	return totalChangesProcessed, nil
}

func (o *SyncOrchestrator) getTableMapping(sourceName, schema, table string) (*common.TrackedTable, bool) {
	if schemas, ok := o.tableMappings[sourceName]; ok {
		if tables, ok := schemas[schema]; ok {
			if tt, ok := tables[table]; ok {
				return tt, true
			}
		}
	}
	return nil, false
}
