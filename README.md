# DB Sync Tool

A Go-based CDC (Change Data Capture) synchronization tool that streams database changes from MSSQL to PostgreSQL over gRPC.

## Architecture

```
┌─────────────────┐         gRPC/TLS          ┌─────────────────┐
│   MSSQL (CDC)   │◄────┐                ┌────►│   PostgreSQL    │
└─────────────────┘     │                │     └─────────────────┘
                        │                │
                  ┌─────┴────┐      ┌────┴─────┐
                  │  Client  │─────►│  Server  │
                  │(cdc-client)│ Stream │(cdc-server)│
                  └──────────┘  +Ack  └──────────┘
```

- **Client**: Polls MSSQL CDC tables and streams changes to the server
- **Server**: Receives the stream and applies changes to PostgreSQL
- **Acknowledgment**: Client only advances its CDC cursor after server confirms the write

## Prerequisites

- **Go 1.23+**
- **MSSQL** with CDC enabled on source tables
- **PostgreSQL** as the target database

### Installing Prerequisites

**Go:**
Download and install from [golang.org](https://golang.org/dl/)

Or using Homebrew on macOS:
```bash
brew install go
```

## Building

### Using Make

```bash
# Build all binaries
make build

# Build individual binaries
make build-client
make build-server

# Run tests
make test

# Clean build artifacts
make clean
```

### Using Go directly

```bash
# Build client
go build -o bin/cdc-client ./cmd/cdc-client

# Build server
go build -o bin/cdc-server ./cmd/cdc-server
```

Binaries will be located at:
- `bin/cdc-client`
- `bin/cdc-server`

## Configuration

### Client Configuration

Create `config/client.toml` or use environment variables with `CLIENT__` prefix:

```toml
# MSSQL connection string
mssql_connection_string = "Server=localhost;Database=mydb;User Id=sa;Password=secret;"

# gRPC server address
server_url = "localhost:50051"

# Polling interval in milliseconds
poll_interval_ms = 1000

# Changes per batch
batch_size = 1000

# Acknowledgment timeout in seconds
ack_timeout_secs = 30

# State file for cursor persistence
state_file_path = "./data/cdc_cursor_state.json"

# TLS settings (optional)
[tls]
ca_cert_path = "certs/ca.crt"
client_cert_path = "certs/client.crt"
client_key_path = "certs/client.key"
domain_name = "localhost"

# Tables to track
[[tracked_tables]]
source_schema = "dbo"
source_table = "users"
target_schema = "public"
target_table = "users"
```

Environment variable example:
```bash
export CLIENT__MSSQL_CONNECTION_STRING="Server=localhost;Database=mydb;..."
export CLIENT__SERVER_URL="localhost:50051"
```

### Server Configuration

Create `config/server.toml` or use environment variables with `SERVER__` prefix:

```toml
# PostgreSQL connection string
postgres_connection_string = "postgres://user:pass@localhost/targetdb"

# Listen address
listen_addr = "0.0.0.0:50051"

# Maximum concurrent batch processing
max_concurrent_batches = 4

# Enable idempotent upserts (recommended)
idempotent_writes = true

# Schema mappings (optional)
[[schema_mappings]]
source_schema = "dbo"
target_schema = "public"

# TLS settings (optional)
[tls]
cert_path = "certs/server.crt"
key_path = "certs/server.key"
ca_cert_path = "certs/ca.crt"  # For mTLS client verification
```

## TLS Setup

Generate certificates for secure communication:

```bash
chmod +x scripts/generate-certs.sh
./scripts/generate-certs.sh
```

This creates:
- `certs/ca.crt` - Certificate Authority
- `certs/server.crt` / `certs/server.key` - Server certificate
- `certs/client.crt` / `certs/client.key` - Client certificate

## Running

### Start the Server

```bash
./bin/cdc-server
```

Or with environment variables:
```bash
SERVER__POSTGRES_CONNECTION_STRING="postgres://..." ./bin/cdc-server
```

### Start the Client

```bash
./bin/cdc-client
```

Or with environment variables:
```bash
CLIENT__MSSQL_CONNECTION_STRING="Server=..." \
CLIENT__SERVER_URL="localhost:50051" \
./bin/cdc-client
```

## Project Structure

```
db-sync-tool/
├── go.mod                  # Go module definition
├── Makefile               # Build automation
├── proto/                  # Protocol Buffer definitions
│   ├── cdc_sync.proto     # gRPC service definitions
│   ├── cdc_sync.pb.go     # Generated protobuf code
│   └── cdc_sync_grpc.pb.go # Generated gRPC code
├── internal/
│   ├── common/            # Shared utilities
│   │   ├── types.go       # LSN, TrackedTable, etc.
│   │   ├── errors.go      # Error types
│   │   └── tls.go         # TLS helpers
│   ├── client/            # CDC client implementation
│   │   ├── config.go      # Configuration
│   │   ├── cdc.go         # MSSQL CDC polling
│   │   ├── grpc_client.go # gRPC streaming client
│   │   ├── state.go       # Cursor state management
│   │   └── sync_runner.go # Sync orchestration
│   └── server/            # CDC server implementation
│       ├── config.go      # Configuration
│       ├── grpc_server.go # gRPC service implementation
│       └── postgres.go    # PostgreSQL writer
├── cmd/
│   ├── cdc-client/        # Client entry point
│   │   └── main.go
│   └── cdc-server/        # Server entry point
│       └── main.go
├── config/                # Sample configuration files
└── scripts/               # Utility scripts
```

## MSSQL CDC Setup

Enable CDC on your source database and tables:

```sql
-- Enable CDC on database
EXEC sys.sp_cdc_enable_db;

-- Enable CDC on a table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'YourTable',
    @role_name = NULL,
    @supports_net_changes = 1;
```

## License

See LICENSE file for details.

---

## Debugging in VS Code

The repository includes `.vscode/launch.json` and `.vscode/tasks.json` for easy debugging:

- `Debug cdc-client` — debug the client with delve
- `Debug cdc-server` — debug the server with delve
- `Run cdc-client` — run client without debugger
- `Run cdc-server` — run server without debugger

How to use:

1. Install the Go extension for VS Code (search for "Go" in Extensions).
2. Open the Run and Debug side-bar (Ctrl+Shift+D / ⇧⌘D).
3. Choose a configuration from the drop-down.
4. Press F5 to start debugging.

### Debug configuration files

You can run the client and server with debug-specific configurations. Two debug files are included:

- `config/client.debug.toml` — debug variant for the client
- `config/server.debug.toml` — debug variant for the server

The code supports selecting an alternate config file via an environment variable:

- `CLIENT_CONFIG_FILE` — e.g. `config/client.debug` (no extension)
- `SERVER_CONFIG_FILE` — e.g. `config/server.debug` (no extension)

The provided `.vscode/launch.json` debug profiles set these env vars so the debug variants are used automatically when launching from the Run & Debug panel.
