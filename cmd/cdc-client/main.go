// CDC Client - MSSQL CDC Change Data Capture streamer.
//
// This binary polls MSSQL CDC tables and streams changes to the CDC Server
// over secure gRPC, implementing a robust acknowledgment-based cursor management.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/wso2/db-sync-tool/internal/client"
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

	log.Info().Msg("Starting CDC Client")

	// Load configuration
	config, err := client.LoadConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}
	log.Info().Str("server_url", config.ServerURL).Msg("Configuration loaded")

	// Create and run the sync orchestrator
	orchestrator, err := client.NewSyncOrchestrator(config)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create sync orchestrator")
	}
	defer orchestrator.Close()

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Info().Str("signal", sig.String()).Msg("Received shutdown signal")
		cancel()
	}()

	// Run the sync loop
	if err := orchestrator.Run(ctx); err != nil {
		if err == context.Canceled {
			log.Info().Msg("Shutdown complete")
		} else {
			log.Fatal().Err(err).Msg("Sync loop failed")
		}
	}
}
