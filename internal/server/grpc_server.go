package server

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/wso2/db-sync-tool/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Version is the server version.
const Version = "1.0.0"

// SQLWriter defines the interface for writing changes to a database.
// This allows the CdcSyncService to work with different database implementations.
type SQLWriter interface {
	ApplyChanges(ctx context.Context, changes []*proto.ChangeRequest, idempotent bool) (uint64, error)
	IsHealthy(ctx context.Context) bool
	Close()
}

// CdcSyncService is the CDC synchronization service implementation.
type CdcSyncService struct {
	proto.UnimplementedCdcSyncServer
	writer SQLWriter
	config *Config
}

// NewCdcSyncService creates a new CDC sync service.
func NewCdcSyncService(writer SQLWriter, config *Config) *CdcSyncService {
	return &CdcSyncService{
		writer: writer,
		config: config,
	}
}

// StreamChanges handles bidirectional streaming of CDC changes.
func (s *CdcSyncService) StreamChanges(stream proto.CdcSync_StreamChangesServer) error {
	ctx := stream.Context()

	log.Info().Msg("New CDC stream connection")

	var currentBatchID *string
	var batchChanges []*proto.ChangeRequest
	batchStart := time.Now()
	var lastLsn []byte
	var lastSeq uint64

	for {
		change, err := stream.Recv()
		if err == io.EOF {
			// Process final batch when stream ends
			if len(batchChanges) > 0 {
				ack := s.processBatch(ctx, currentBatchID, batchChanges, lastLsn, lastSeq, time.Since(batchStart))
				if err := stream.Send(ack); err != nil {
					log.Error().Err(err).Msg("Failed to send final acknowledgment")
					return err
				}
			}
			log.Debug().Msg("Stream processing completed")
			return nil
		}
		if err != nil {
			log.Error().Err(err).Msg("Error receiving change")
			return err
		}

		changeBatchID := change.BatchId

		// Detect batch boundary
		if currentBatchID == nil || *currentBatchID != changeBatchID {
			// Process previous batch if exists
			if len(batchChanges) > 0 {
				ack := s.processBatch(ctx, currentBatchID, batchChanges, lastLsn, lastSeq, time.Since(batchStart))
				if err := stream.Send(ack); err != nil {
					log.Warn().Err(err).Msg("Client disconnected")
					return err
				}
				batchChanges = nil
			}

			// Start new batch
			currentBatchID = &changeBatchID
			batchStart = time.Now()
		}

		// Track LSN and sequence for acknowledgment
		if len(change.Lsn) > 0 {
			lastLsn = change.Lsn
		}
		lastSeq = change.SequenceNumber

		batchChanges = append(batchChanges, change)
	}
}

func (s *CdcSyncService) processBatch(ctx context.Context, batchID *string, changes []*proto.ChangeRequest, lsn []byte, lastSeq uint64, elapsed time.Duration) *proto.ChangeAck {
	batchIDStr := "unknown"
	if batchID != nil {
		batchIDStr = *batchID
	}
	changeCount := uint64(len(changes))

	log.Info().
		Str("batch_id", batchIDStr).
		Uint64("change_count", changeCount).
		Msg("Processing batch")

	rowsAffected, err := s.writer.ApplyChanges(ctx, changes, s.config.IdempotentWrites)
	if err != nil {
		log.Error().
			Str("batch_id", batchIDStr).
			Err(err).
			Msg("Batch processing failed")

		// Determine if this is a retryable error
		status, message := categorizeError(err)

		return &proto.ChangeAck{
			BatchId:            batchIDStr,
			Lsn:                lsn,
			LastSequenceNumber: lastSeq,
			Status:             status,
			ErrorMessage:       &message,
			RowsProcessed:      0,
			ProcessingTimeMs:   uint64(elapsed.Milliseconds()),
		}
	}

	log.Info().
		Str("batch_id", batchIDStr).
		Uint64("rows_affected", rowsAffected).
		Int64("elapsed_ms", elapsed.Milliseconds()).
		Msg("Batch applied successfully")

	return &proto.ChangeAck{
		BatchId:            batchIDStr,
		Lsn:                lsn,
		LastSequenceNumber: lastSeq,
		Status:             proto.AckStatus_ACK_STATUS_OK,
		RowsProcessed:      rowsAffected,
		ProcessingTimeMs:   uint64(elapsed.Milliseconds()),
	}
}

// HealthCheck handles health check requests.
func (s *CdcSyncService) HealthCheck(ctx context.Context, req *proto.HealthCheckRequest) (*proto.HealthCheckResponse, error) {
	// Check database connectivity
	var status proto.HealthCheckResponse_ServingStatus
	if s.writer.IsHealthy(ctx) {
		status = proto.HealthCheckResponse_SERVING_STATUS_SERVING
	} else {
		status = proto.HealthCheckResponse_SERVING_STATUS_NOT_SERVING
	}

	return &proto.HealthCheckResponse{
		Status:     status,
		Version:    Version,
		ServerTime: timestamppb.Now(),
	}, nil
}

// categorizeError categorizes an error to determine the appropriate ack status.
func categorizeError(err error) (proto.AckStatus, string) {
	errString := strings.ToLower(err.Error())

	if strings.Contains(errString, "connection") ||
		strings.Contains(errString, "timeout") ||
		strings.Contains(errString, "temporarily unavailable") {
		return proto.AckStatus_ACK_STATUS_RETRY, err.Error()
	} else if strings.Contains(errString, "column") ||
		strings.Contains(errString, "type") ||
		strings.Contains(errString, "schema") {
		return proto.AckStatus_ACK_STATUS_SCHEMA_MISMATCH, err.Error()
	}
	return proto.AckStatus_ACK_STATUS_FAILED, err.Error()
}
