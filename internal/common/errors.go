package common

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SyncErrorType represents the type of sync error.
type SyncErrorType int

const (
	ErrDatabase SyncErrorType = iota
	ErrConnection
	ErrGrpc
	ErrTransport
	ErrSerialization
	ErrTls
	ErrConfig
	ErrCdcCursor
	ErrSchemaMismatch
	ErrAckTimeout
	ErrBatchFailed
	ErrIO
	ErrInternal
)

// SyncError represents a unified error type for CDC synchronization operations.
type SyncError struct {
	Type    SyncErrorType
	Message string
	BatchID string // Used for AckTimeout and BatchFailed
	Cause   error
}

func (e *SyncError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

func (e *SyncError) Unwrap() error {
	return e.Cause
}

// ToGrpcStatus converts SyncError to gRPC Status.
func (e *SyncError) ToGrpcStatus() *status.Status {
	switch e.Type {
	case ErrDatabase:
		return status.New(codes.Internal, e.Error())
	case ErrConnection:
		return status.New(codes.Unavailable, e.Error())
	case ErrGrpc:
		return status.New(codes.Internal, e.Error())
	case ErrTransport:
		return status.New(codes.Unavailable, e.Error())
	case ErrSerialization:
		return status.New(codes.InvalidArgument, e.Error())
	case ErrTls:
		return status.New(codes.Internal, e.Error())
	case ErrConfig:
		return status.New(codes.FailedPrecondition, e.Error())
	case ErrCdcCursor:
		return status.New(codes.Internal, e.Error())
	case ErrSchemaMismatch:
		return status.New(codes.FailedPrecondition, e.Error())
	case ErrAckTimeout:
		return status.New(codes.DeadlineExceeded, fmt.Sprintf("Ack timeout for batch %s", e.BatchID))
	case ErrBatchFailed:
		return status.New(codes.Internal, fmt.Sprintf("Batch %s failed: %s", e.BatchID, e.Message))
	case ErrIO:
		return status.New(codes.Internal, e.Error())
	case ErrInternal:
		return status.New(codes.Internal, e.Error())
	default:
		return status.New(codes.Unknown, e.Error())
	}
}

// NewDatabaseError creates a new database error.
func NewDatabaseError(msg string, cause error) *SyncError {
	return &SyncError{Type: ErrDatabase, Message: "Database error: " + msg, Cause: cause}
}

// NewConnectionError creates a new connection error.
func NewConnectionError(msg string, cause error) *SyncError {
	return &SyncError{Type: ErrConnection, Message: "Connection error: " + msg, Cause: cause}
}

// NewGrpcError creates a new gRPC error.
func NewGrpcError(msg string, cause error) *SyncError {
	return &SyncError{Type: ErrGrpc, Message: "gRPC error: " + msg, Cause: cause}
}

// NewTransportError creates a new transport error.
func NewTransportError(msg string, cause error) *SyncError {
	return &SyncError{Type: ErrTransport, Message: "Transport error: " + msg, Cause: cause}
}

// NewSerializationError creates a new serialization error.
func NewSerializationError(msg string, cause error) *SyncError {
	return &SyncError{Type: ErrSerialization, Message: "Serialization error: " + msg, Cause: cause}
}

// NewTlsError creates a new TLS error.
func NewTlsError(msg string, cause error) *SyncError {
	return &SyncError{Type: ErrTls, Message: "TLS configuration error: " + msg, Cause: cause}
}

// NewConfigError creates a new configuration error.
func NewConfigError(msg string, cause error) *SyncError {
	return &SyncError{Type: ErrConfig, Message: "Configuration error: " + msg, Cause: cause}
}

// NewCdcCursorError creates a new CDC cursor error.
func NewCdcCursorError(msg string, cause error) *SyncError {
	return &SyncError{Type: ErrCdcCursor, Message: "CDC cursor error: " + msg, Cause: cause}
}

// NewSchemaMismatchError creates a new schema mismatch error.
func NewSchemaMismatchError(msg string) *SyncError {
	return &SyncError{Type: ErrSchemaMismatch, Message: "Schema mismatch: " + msg}
}

// NewAckTimeoutError creates a new acknowledgment timeout error.
func NewAckTimeoutError(batchID string) *SyncError {
	return &SyncError{Type: ErrAckTimeout, BatchID: batchID, Message: "Acknowledgment timeout"}
}

// NewBatchFailedError creates a new batch failed error.
func NewBatchFailedError(batchID, msg string) *SyncError {
	return &SyncError{Type: ErrBatchFailed, BatchID: batchID, Message: msg}
}

// NewIOError creates a new IO error.
func NewIOError(msg string, cause error) *SyncError {
	return &SyncError{Type: ErrIO, Message: "IO error: " + msg, Cause: cause}
}

// NewInternalError creates a new internal error.
func NewInternalError(msg string, cause error) *SyncError {
	return &SyncError{Type: ErrInternal, Message: "Internal error: " + msg, Cause: cause}
}
