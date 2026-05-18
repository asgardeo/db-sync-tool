#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="$SCRIPT_DIR/sql"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to wait for MSSQL to be ready
wait_for_mssql() {
    log_info "Waiting for MSSQL to be ready..."
    local max_attempts=30
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if docker exec mssql-test-db /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "StrongPassword123!" -C -Q "SELECT 1" &> /dev/null; then
            log_info "MSSQL is ready!"
            return 0
        fi
        log_info "Attempt $attempt/$max_attempts - MSSQL not ready yet, waiting..."
        sleep 5
        ((attempt++))
    done

    log_error "MSSQL failed to become ready after $max_attempts attempts"
    return 1
}

# Function to wait for PostgreSQL to be ready
wait_for_postgres() {
    log_info "Waiting for PostgreSQL to be ready..."
    local max_attempts=30
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if docker exec postgres-test-db pg_isready -U postgres -d testdb &> /dev/null; then
            log_info "PostgreSQL is ready!"
            return 0
        fi
        log_info "Attempt $attempt/$max_attempts - PostgreSQL not ready yet, waiting..."
        sleep 2
        ((attempt++))
    done

    log_error "PostgreSQL failed to become ready after $max_attempts attempts"
    return 1
}

# Function to stop and remove existing containers
cleanup_containers() {
    log_info "Cleaning up existing containers..."

    cd "$SCRIPT_DIR"

    docker-compose -f docker-compose-mssql.yml down -v 2>/dev/null || true
    docker-compose -f docker-compose-postgres.yml down -v 2>/dev/null || true

    log_info "Cleanup completed"
}

# Function to start MSSQL container
start_mssql() {
    log_info "Starting MSSQL container..."
    cd "$SCRIPT_DIR"
    docker-compose -f docker-compose-mssql.yml up -d

    wait_for_mssql
}

# Function to start PostgreSQL container
start_postgres() {
    log_info "Starting PostgreSQL container..."
    cd "$SCRIPT_DIR"
    docker-compose -f docker-compose-postgres.yml up -d

    wait_for_postgres
}

# Function to setup MSSQL database
setup_mssql_db() {
    log_info "Setting up MSSQL database with tables and CDC..."

    # Copy SQL script to container and fix permissions (container runs as mssql user)
    docker cp "$SQL_DIR/mssql_setup.sql" mssql-test-db:/tmp/mssql_setup.sql
    docker exec -u root mssql-test-db chmod 644 /tmp/mssql_setup.sql

    docker exec mssql-test-db /opt/mssql-tools18/bin/sqlcmd \
        -S localhost \
        -U sa \
        -P "StrongPassword123!" \
        -C \
        -i /tmp/mssql_setup.sql

    log_info "MSSQL setup completed!"
}

# Function to setup PostgreSQL database
setup_postgres_db() {
    log_info "Setting up PostgreSQL database with tables..."

    # Copy SQL script to container and execute
    docker cp "$SQL_DIR/postgres_setup.sql" postgres-test-db:/tmp/postgres_setup.sql

    docker exec postgres-test-db psql \
        -U postgres \
        -d testdb \
        -f /tmp/postgres_setup.sql

    log_info "PostgreSQL setup completed!"
}

# Function to display connection info
display_connection_info() {
    echo ""
    echo "=============================================="
    echo "         DATABASE CONNECTION INFO            "
    echo "=============================================="
    echo ""
    echo "MSSQL Server:"
    echo "  Host: localhost"
    echo "  Port: 1433"
    echo "  Database: testdb"
    echo "  Username: sa"
    echo "  Password: StrongPassword123!"
    echo "  CDC: Enabled on 'users' and 'orders' tables"
    echo ""
    echo "PostgreSQL:"
    echo "  Host: localhost"
    echo "  Port: 5432"
    echo "  Database: testdb"
    echo "  Username: postgres"
    echo "  Password: StrongPassword123!"
    echo ""
    echo "=============================================="
}

# Main execution
main() {
    log_info "Starting database setup..."

    # Check if Docker is running
    if ! docker info &> /dev/null; then
        log_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi

    # Parse command line arguments
    case "${1:-all}" in
        mssql)
            cleanup_containers
            start_mssql
            setup_mssql_db
            ;;
        postgres)
            cleanup_containers
            start_postgres
            setup_postgres_db
            ;;
        all)
            cleanup_containers
            start_mssql
            start_postgres
            setup_mssql_db
            setup_postgres_db
            ;;
        clean)
            cleanup_containers
            log_info "All containers and volumes have been removed."
            exit 0
            ;;
        *)
            echo "Usage: $0 [mssql|postgres|all|clean]"
            echo "  mssql    - Setup only MSSQL"
            echo "  postgres - Setup only PostgreSQL"
            echo "  all      - Setup both databases (default)"
            echo "  clean    - Remove all containers and volumes"
            exit 1
            ;;
    esac

    display_connection_info

    log_info "Setup completed successfully!"
}

main "$@"
