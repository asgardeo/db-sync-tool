package client

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/wso2/db-sync-tool/internal/common"
	"github.com/wso2/db-sync-tool/proto"
)

// SyncOrchestrator orchestrates the CDC sync process.
type SyncOrchestrator struct {
	config       *Config
	stateManager *StateManager
	cdcReader    *CdcReader
	grpcClient   *StreamingClient
}

// NewSyncOrchestrator creates a new SyncOrchestrator with all required components.
func NewSyncOrchestrator(config *Config) (*SyncOrchestrator, error) {
	stateManager, err := NewStateManager(config.StateFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create state manager: %w", err)
	}

	cdcReader, err := NewCdcReader(config.MssqlConnectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to create CDC reader: %w", err)
	}

	grpcClient, err := NewStreamingClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client: %w", err)
	}

	return &SyncOrchestrator{
		config:       config,
		stateManager: stateManager,
		cdcReader:    cdcReader,
		grpcClient:   grpcClient,
	}, nil
}

// Close closes all resources.
func (o *SyncOrchestrator) Close() error {
	var errs []error
	if o.cdcReader != nil {
		if err := o.cdcReader.Close(); err != nil {
			errs = append(errs, err)
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

	for {
		select {
		case <- ctx.Done():
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
			log.Info().
				Dur("delay", delay).
				Msg("Backing off before retry")

			select {
			case <- ctx.Done():
				return ctx.Err()
			case <- time.After(delay):
			}

			// Attempt to reconnect
			if err := o.grpcClient.Reconnect(); err != nil {
				log.Error().Err(err).Msg("Reconnection failed")
			}
		} else {
			consecutiveErrors = 0

			if changesProcessed == 0 {
				// No changes, wait before polling again
				select {
				case <- ctx.Done():
					return ctx.Err()
				case <- time.After(time.Duration(o.config.PollIntervalMs) * time.Millisecond):
				}
			}
		}
	}
}

func (o *SyncOrchestrator) syncCycle(ctx context.Context) (uint64, error) {
	// Get the current cursor position
	cursor := o.stateManager.GetCursor()
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
			Str("from_lsn", fromLsn.ToHexString()).
			Msg("Polling CDC changes")
	} else {
		log.Debug().Msg("Polling CDC changes from beginning")
	}

	// Poll for changes from MSSQL CDC
	changes, err := o.cdcReader.PollChanges(ctx, fromLsn, o.config.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to poll changes: %w", err)
	}

	if len(changes) == 0 {
		return 0, nil
	}

	batchID := uuid.New().String()
	changeCount := uint64(len(changes))

	log.Info().
		Str("batch_id", batchID).
		Uint64("change_count", changeCount).
		Msg("Streaming batch to server")

	// Stream changes and wait for acknowledgment
	ack, err := o.grpcClient.StreamBatch(ctx, batchID, changes)
	if err != nil {
		return 0, fmt.Errorf("failed to stream batch: %w", err)
	}

	// Process the acknowledgment
	switch ack.Status {
	case proto.AckStatus_ACK_STATUS_OK:
		log.Info().
			Str("batch_id", batchID).
			Uint64("rows_processed", ack.RowsProcessed).
			Uint64("processing_time_ms", ack.ProcessingTimeMs).
			Msg("Batch acknowledged successfully")

		// Update cursor only after successful ack
		newLsn := common.NewLsn(ack.Lsn)
		if err := o.stateManager.UpdateCursor(batchID, newLsn); err != nil {
			return 0, fmt.Errorf("failed to update cursor: %w", err)
		}

		return ack.RowsProcessed, nil

	case proto.AckStatus_ACK_STATUS_PARTIAL:
		log.Warn().
			Str("batch_id", batchID).
			Uint64("last_sequence", ack.LastSequenceNumber).
			Msg("Partial acknowledgment received")

		// Update cursor to partial position
		newLsn := common.NewLsn(ack.Lsn)
		if err := o.stateManager.UpdateCursor(batchID, newLsn); err != nil {
			return 0, fmt.Errorf("failed to update cursor: %w", err)
		}

		return ack.RowsProcessed, nil

	case proto.AckStatus_ACK_STATUS_RETRY:
		errorMsg := "unknown"
		if ack.ErrorMessage != nil {
			errorMsg = *ack.ErrorMessage
		}
		log.Warn().
			Str("batch_id", batchID).
			Str("error", errorMsg).
			Msg("Server requested retry")
		// Don't update cursor - will retry on next cycle
		return 0, nil

	default:
		errorMsg := "unknown"
		if ack.ErrorMessage != nil {
			errorMsg = *ack.ErrorMessage
		}
		log.Error().
			Str("batch_id", batchID).
			Str("error", errorMsg).
			Msg("Batch processing failed")
		return 0, fmt.Errorf("batch failed: %s", errorMsg)
	}
}
