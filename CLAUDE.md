# DB-Sync-Tool

## Overview

Go-based CDC (Change Data Capture) synchronization tool that streams database changes from MSSQL to PostgreSQL over gRPC/TLS. Uses a pluggable connector architecture with dedicated reader/writer interfaces.

**Module**: `github.com/wso2/db-sync-tool`
**Go version**: 1.23

## Architecture

```
MSSQL (CDC enabled) → [cdc-client] —— gRPC/TLS bidi stream —— [cdc-server] → PostgreSQL
```

- **Client** (`cmd/cdc-client`): Polls MSSQL CDC tables, streams changes via gRPC
- **Server** (`cmd/cdc-server`): Receives gRPC stream, applies changes to PostgreSQL
- **Connectors** (`internal/connector/`): Pluggable `CDCReader` and `SQLWriter` interfaces
- **State**: LSN cursor persisted to JSON file (atomic write via temp file + rename)

### Data Flow

1. Client polls MSSQL CDC via `cdc.fn_cdc_get_all_changes_*`
2. Changes filtered against `TrackedTables` config
3. Batched with UUID and streamed via gRPC bidirectional stream
4. Server applies to PostgreSQL in a transaction (deferred constraints, idempotent upserts)
5. Server sends `ChangeAck` (OK/PARTIAL/RETRY/FAILED/SCHEMA_MISMATCH)
6. Client persists LSN cursor on ACK_STATUS_OK

## Build & Run

```bash
make build              # Build both client and server → bin/
make build-client       # Build client only
make build-server       # Build server only
make test               # Run all tests
make test-coverage      # Tests with coverage report
make proto              # Regenerate protobuf (requires protoc)
make proto-deps         # Install protoc-gen-go and protoc-gen-go-grpc
make lint               # Run golangci-lint
make fmt                # Format code
make certs              # Generate dev TLS certs
make clean              # Remove build artifacts
```

## Project Structure

```
cmd/
  cdc-client/main.go         # Client entry point
  cdc-server/main.go         # Server entry point
internal/
  common/
    types.go                  # Lsn, TrackedTable, CdcCursorState, RetryConfig, SyncMetrics
    errors.go                 # SyncError with type categories → gRPC status mapping
    tls.go                    # TLS/mTLS certificate loading
  client/
    config.go                 # Client config (TOML + CLIENT__ env vars)
    state.go                  # StateManager - cursor persistence to JSON
    sync_runner.go            # SyncOrchestrator - main sync loop with retry
    grpc_client.go            # StreamingClient - gRPC bidi streaming + health check
  connector/
    types.go                  # ReaderType, WriterType, ReaderConfig, WriterConfig
    reader.go                 # CDCReader interface
    writer.go                 # SQLWriter interface
    mssql/reader.go           # MSSQL CDC reader (fn_cdc_get_all_changes, schema caching)
    postgres/writer.go        # PostgreSQL writer (pgx pool, ON CONFLICT upserts)
  server/
    config.go                 # Server config (TOML + SERVER__ env vars)
    grpc_server.go            # CdcSyncService - gRPC handler + batch processing
proto/
  cdc_sync.proto              # gRPC service + message definitions
  cdc_sync.pb.go              # Generated
  cdc_sync_grpc.pb.go         # Generated
config/
  client.toml                 # Production client config template
  client.debug.toml           # Debug: multi-source, fast polling (200ms)
  server.toml                 # Production server config template
  server.debug.toml           # Debug: single batch, local listen
test/
  docker-compose-mssql.yml    # MSSQL 2022 container (port 1433)
  docker-compose-postgres.yml # PostgreSQL 16 container (port 5432)
  setup.sh                    # Container orchestration
  run_cdc_test.sh             # CDC test data generator
  sql/
    mssql_setup.sql           # Schema + CDC enable (users, orders tables)
    postgres_setup.sql        # PostgreSQL schema only
    cdc_test_data.sql         # INSERT/UPDATE/DELETE test scenarios
scripts/
  generate-certs.sh           # Self-signed TLS cert generator
```

## Key Interfaces

```go
// CDCReader - source database connector (internal/connector/reader.go)
type CDCReader interface {
    PollChanges(ctx context.Context, fromPosition []byte, batchSize uint32) ([]*proto.ChangeRequest, error)
    Close() error
}

// SQLWriter - target database connector (internal/connector/writer.go)
type SQLWriter interface {
    ApplyChanges(ctx context.Context, changes []*proto.ChangeRequest, idempotent bool) (uint64, error)
    IsHealthy(ctx context.Context) bool
    Close()
}
```

Currently supported: `ReaderTypeMSSQL = "mssql"`, `WriterTypePostgres = "postgres"`

## Configuration

- **TOML files** in `config/` directory
- **Environment variable override**: `CLIENT__*` prefix (client), `SERVER__*` prefix (server)
- Supports legacy single-source config + new multi-source config
- See `.env.example` for env var templates

## Conventions

- **Errors**: Always wrap in `SyncError` (from `internal/common/errors.go`) with appropriate type category. Error types: `ErrDatabase`, `ErrConnection`, `ErrGrpc`, `ErrTransport`, `ErrSerialization`, `ErrTls`, `ErrConfig`, `ErrCdcCursor`, `ErrSchemaMismatch`, `ErrAckTimeout`, `ErrBatchFailed`, `ErrIO`, `ErrInternal`
- **Logging**: Use zerolog (structured). Import from `github.com/rs/zerolog`
- **Concurrency**: RWMutex for schema/PK caches in connectors
- **MSSQL CDC operations**: 1=DELETE, 2=INSERT, 3=UPDATE_OLD, 4=UPDATE_NEW
- **PostgreSQL writes**: `SET CONSTRAINTS ALL DEFERRED` + `ON CONFLICT DO UPDATE` for idempotent mode
- **State persistence**: JSON with atomic write (write temp file, then rename)
- **Connection pooling**: MSSQL: 4 max open, 2 idle; PostgreSQL: pgxpool

## Proto / gRPC

- Service: `CdcSync` with `StreamChanges` (bidi stream) and `HealthCheck` RPCs
- Key messages: `ChangeRequest`, `ChangeAck`, `ColumnMetadata`
- Ack statuses: OK, PARTIAL, RETRY, FAILED, SCHEMA_MISMATCH
- Regenerate with `make proto` after editing `proto/cdc_sync.proto`

## Test Infrastructure

- Docker Compose in `test/` for MSSQL 2022 and PostgreSQL 16
- Test database: `testdb` with `users` and `orders` tables (orders has FK to users)
- Run `test/setup.sh` to start containers, then `test/run_cdc_test.sh` for CDC test data

## Dependencies

grpc v1.69.2, protobuf v1.36.1, go-mssqldb v1.8.0, pgx v5.7.2, zerolog v1.33.0, go-toml v2.2.3, uuid v1.6.0, godotenv v1.5.1
