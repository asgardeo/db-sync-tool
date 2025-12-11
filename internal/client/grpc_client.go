package client

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/wso2/db-sync-tool/internal/common"
	"github.com/wso2/db-sync-tool/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// StreamingClient is a streaming client for CDC synchronization.
type StreamingClient struct {
	conn   *grpc.ClientConn
	client proto.CdcSyncClient
	config *Config
}

// NewStreamingClient creates a new streaming client connected to the CDC server.
func NewStreamingClient(config *Config) (*StreamingClient, error) {
	conn, err := createChannel(config)
	if err != nil {
		return nil, err
	}

	client := proto.NewCdcSyncClient(conn)

	return &StreamingClient{
		conn:   conn,
		client: client,
		config: config,
	}, nil
}

func createChannel(config *Config) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption

	// Configure TLS if certificates are provided
	if config.TLS.CACertPath != nil && *config.TLS.CACertPath != "" {
		domainName := "localhost"
		if config.TLS.DomainName != nil && *config.TLS.DomainName != "" {
			domainName = *config.TLS.DomainName
		}

		tlsCreds, err := common.LoadClientTLSConfig(
			*config.TLS.CACertPath,
			config.TLS.ClientCertPath,
			config.TLS.ClientKeyPath,
			domainName,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS configuration: %w", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(tlsCreds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Connect with timeout
	_, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(config.ServerURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	return conn, nil
}

// Close closes the connection.
func (c *StreamingClient) Close() error {
	return c.conn.Close()
}

// Reconnect reconnects to the CDC server.
func (c *StreamingClient) Reconnect() error {
	log.Info().Msg("Reconnecting to CDC server")

	if c.conn != nil {
		c.conn.Close()
	}

	conn, err := createChannel(c.config)
	if err != nil {
		return err
	}

	c.conn = conn
	c.client = proto.NewCdcSyncClient(conn)
	return nil
}

// StreamBatch streams a batch of changes to the server and waits for acknowledgment.
func (c *StreamingClient) StreamBatch(ctx context.Context, batchID string, changes []*proto.ChangeRequest) (*proto.ChangeAck, error) {
	// Set batch_id on all changes
	for _, change := range changes {
		change.BatchId = batchID
	}

	changeCount := len(changes)
	log.Debug().
		Str("batch_id", batchID).
		Int("change_count", changeCount).
		Msg("Starting bidirectional stream for batch")

	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(c.config.AckTimeoutSecs)*time.Second)
	defer cancel()

	// Start the bidirectional stream
	stream, err := c.client.StreamChanges(timeoutCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to start stream: %w", err)
	}

	// Send all changes
	for _, change := range changes {
		if err := stream.Send(change); err != nil {
			return nil, fmt.Errorf("failed to send change: %w", err)
		}
	}

	// Close send side to signal end of stream
	if err := stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("failed to close send: %w", err)
	}

	// Wait for acknowledgment
	ack, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("stream closed without acknowledgment")
		}
		return nil, fmt.Errorf("failed to receive acknowledgment: %w", err)
	}

	log.Debug().
		Int32("status", int32(ack.Status)).
		Uint64("rows_processed", ack.RowsProcessed).
		Msg("Received acknowledgment")

	return ack, nil
}

// HealthCheck checks the health of the CDC server.
func (c *StreamingClient) HealthCheck(ctx context.Context) (bool, error) {
	resp, err := c.client.HealthCheck(ctx, &proto.HealthCheckRequest{
		ServiceName: "cdc_sync",
	})
	if err != nil {
		return false, err
	}

	return resp.Status == proto.HealthCheckResponse_SERVING_STATUS_SERVING, nil
}
