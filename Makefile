.PHONY: all build build-client build-server clean test proto deps run-client run-server lint

# Build configuration
BINARY_DIR := bin
CLIENT_BINARY := $(BINARY_DIR)/cdc-client
SERVER_BINARY := $(BINARY_DIR)/cdc-server
GO := go
GOFLAGS := -v

# Proto configuration
PROTO_DIR := proto
PROTO_FILES := $(wildcard $(PROTO_DIR)/*.proto)

all: deps build

# Install dependencies
deps:
	$(GO) mod download
	$(GO) mod tidy

# Build all binaries
build: build-client build-server

# Build client
build-client:
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) -o $(CLIENT_BINARY) ./cmd/cdc-client

# Build server
build-server:
	@mkdir -p $(BINARY_DIR)
	$(GO) build $(GOFLAGS) -o $(SERVER_BINARY) ./cmd/cdc-server

# Clean build artifacts
clean:
	rm -rf $(BINARY_DIR)
	$(GO) clean

# Run tests
test:
	$(GO) test -v ./...

# Run tests with coverage
test-coverage:
	$(GO) test -v -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

# Generate protobuf code (requires protoc and plugins)
proto:
	@echo "Generating protobuf code..."
	@echo "Ensure you have installed:"
	@echo "  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest"
	@echo "  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest"
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/cdc_sync.proto

# Install proto compiler plugins
proto-deps:
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Run client
run-client: build-client
	$(CLIENT_BINARY)

# Run server
run-server: build-server
	$(SERVER_BINARY)

# Run linter
lint:
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
	golangci-lint run ./...

# Format code
fmt:
	$(GO) fmt ./...

# Docker build
docker-build:
	docker build -t cdc-client:latest -f Dockerfile.client .
	docker build -t cdc-server:latest -f Dockerfile.server .

# Generate certificates for development
certs:
	@mkdir -p certs
	./scripts/generate-certs.sh

# Help
help:
	@echo "Available targets:"
	@echo "  all           - Download dependencies and build all binaries"
	@echo "  build         - Build both client and server"
	@echo "  build-client  - Build only the client"
	@echo "  build-server  - Build only the server"
	@echo "  clean         - Remove build artifacts"
	@echo "  deps          - Download and tidy dependencies"
	@echo "  test          - Run tests"
	@echo "  test-coverage - Run tests with coverage report"
	@echo "  proto         - Generate protobuf code"
	@echo "  proto-deps    - Install protobuf compiler plugins"
	@echo "  run-client    - Build and run the client"
	@echo "  run-server    - Build and run the server"
	@echo "  lint          - Run linter"
	@echo "  fmt           - Format code"
	@echo "  certs         - Generate development certificates"
	@echo "  help          - Show this help"
