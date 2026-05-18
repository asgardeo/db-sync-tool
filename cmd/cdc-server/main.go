// CDC Server - PostgreSQL CDC sink with gRPC streaming.
//
// This binary accepts CDC change streams from CDC clients and
// applies them to PostgreSQL, sending acknowledgments upon successful writes.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/wso2/db-sync-tool/internal/common"
	"github.com/wso2/db-sync-tool/internal/connector/postgres"
	"github.com/wso2/db-sync-tool/internal/server"
	"github.com/wso2/db-sync-tool/proto"
	"google.golang.org/grpc"
)

func main() {
	// Initialize logging
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	logLevel := zerolog.InfoLevel
	if os.Getenv("LOG_LEVEL") == "debug" {
		logLevel = zerolog.DebugLevel
	}
	zerolog.SetGlobalLevel(logLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	// Load environment variables from .env file
	_ = godotenv.Load()

	log.Info().Msg("Starting CDC Server")

	// Load configuration
	config, err := server.LoadConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}
	log.Info().Str("listen_addr", config.ListenAddr).Msg("Configuration loaded")

	// Run the server
	if err := runServer(config); err != nil {
		log.Fatal().Err(err).Msg("Server failed")
	}
}

func runServer(config *server.Config) error {
	// Create the SQL writer based on writer type
	var writer server.SQLWriter
	switch config.WriterType {
	case "postgres":
		schemaMappings := make([]postgres.SchemaMapping, len(config.SchemaMappings))
		for i, m := range config.SchemaMappings {
			schemaMappings[i] = postgres.SchemaMapping{
				SourceSchema: m.SourceSchema,
				TargetSchema: m.TargetSchema,
			}
		}
		pgWriter, err := postgres.NewPostgreSQLWriter(config.ConnectionString, schemaMappings)
		if err != nil {
			return err
		}
		writer = pgWriter
		log.Info().Msg("Connected to PostgreSQL")
	default:
		return fmt.Errorf("unsupported writer type: %s", config.WriterType)
	}
	defer writer.Close()

	// Create the gRPC service
	cdcService := server.NewCdcSyncService(writer, config)

	// Build the server
	var opts []grpc.ServerOption

	// Configure TLS if certificates are provided
	if config.TLS.CertPath != nil && *config.TLS.CertPath != "" {
		if config.TLS.KeyPath == nil || *config.TLS.KeyPath == "" {
			log.Fatal().Msg("TLS key path required when cert is provided")
		}

		tlsCreds, err := common.LoadServerTLSConfig(
			*config.TLS.CertPath,
			*config.TLS.KeyPath,
			config.TLS.CACertPath,
		)
		if err != nil {
			return err
		}
		opts = append(opts, grpc.Creds(tlsCreds))
		log.Info().Msg("TLS enabled")
	}

	grpcServer := grpc.NewServer(opts...)
	proto.RegisterCdcSyncServer(grpcServer, cdcService)

	// Start listening
	listener, err := net.Listen("tcp", config.ListenAddr)
	if err != nil {
		return err
	}

	// Set up graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Info().Str("signal", sig.String()).Msg("Received shutdown signal")
		cancel()
		grpcServer.GracefulStop()
	}()

	log.Info().Str("addr", config.ListenAddr).Msg("Starting gRPC server")

	errCh := make(chan error, 1)
	go func() {
		errCh <- grpcServer.Serve(listener)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		log.Info().Msg("Shutdown complete")
		return nil
	}
}
