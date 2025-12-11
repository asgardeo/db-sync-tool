#!/bin/bash

# CDC Test Script
# Executes SQL commands against MSSQL to test CDC functionality
# Usage: ./run_cdc_test.sh [section]
#
# Sections:
#   inserts  - Run INSERT operations only
#   updates  - Run UPDATE operations only
#   deletes  - Run DELETE operations only
#   mixed    - Run mixed operations (insert, update, delete in sequence)
#   cleanup  - Remove all test data
#   all      - Run all sections (inserts, updates, deletes, mixed)
#   full     - Run all sections including cleanup at the end

set -e

# Configuration
CONTAINER_NAME="${MSSQL_CONTAINER:-mssql-test-db}"
SA_PASSWORD="${SA_PASSWORD:-StrongPassword123!}"
DATABASE="testdb"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Execute SQL command
exec_sql() {
    local sql="$1"
    docker exec -i "$CONTAINER_NAME" /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P "$SA_PASSWORD" \
        -d "$DATABASE" \
        -C \
        -Q "$sql"
}

# Run INSERTS section
run_inserts() {
    print_info "Running INSERT operations..."

    exec_sql "
        INSERT INTO users (username, email, first_name, last_name, is_active)
        VALUES
            ('testuser1', 'testuser1@example.com', 'Test', 'User1', 1),
            ('testuser2', 'testuser2@example.com', 'Test', 'User2', 1),
            ('testuser3', 'testuser3@example.com', 'Test', 'User3', 0);

        INSERT INTO orders (user_id, order_number, total_amount, status)
        VALUES
            (1, 'ORD-TEST-001', 99.99, 'pending'),
            (2, 'ORD-TEST-002', 149.50, 'pending'),
            (1, 'ORD-TEST-003', 250.00, 'pending');

        PRINT 'INSERTS completed';
    "

    print_info "INSERT operations completed"
}

# Run UPDATES section
run_updates() {
    print_info "Running UPDATE operations..."

    exec_sql "
        UPDATE users
        SET first_name = 'Updated', last_name = 'Name', updated_at = GETDATE()
        WHERE username = 'testuser1';

        UPDATE users
        SET is_active = 0, updated_at = GETDATE()
        WHERE username = 'testuser2';

        UPDATE users
        SET email = 'newemail@example.com', updated_at = GETDATE()
        WHERE username = 'testuser3';

        UPDATE orders
        SET status = 'shipped', updated_at = GETDATE()
        WHERE order_number = 'ORD-TEST-001';

        UPDATE orders
        SET status = 'completed', total_amount = 155.00, updated_at = GETDATE()
        WHERE order_number = 'ORD-TEST-002';

        PRINT 'UPDATES completed';
    "

    print_info "UPDATE operations completed"
}

# Run DELETES section
run_deletes() {
    print_info "Running DELETE operations..."

    exec_sql "
        DELETE FROM orders WHERE order_number = 'ORD-TEST-003';
        DELETE FROM users WHERE username = 'testuser3';

        PRINT 'DELETES completed';
    "

    print_info "DELETE operations completed"
}

# Run MIXED section
run_mixed() {
    print_info "Running MIXED operations..."

    exec_sql "
        -- Insert a new user
        INSERT INTO users (username, email, first_name, last_name, is_active)
        VALUES ('mixeduser', 'mixed@example.com', 'Mixed', 'User', 1);
    "

    exec_sql "
        -- Update the same user
        UPDATE users SET first_name = 'MixedUpdated' WHERE username = 'mixeduser';
    "

    exec_sql "
        -- Insert an order for this user
        DECLARE @mixedUserId INT;
        SELECT @mixedUserId = id FROM users WHERE username = 'mixeduser';
        INSERT INTO orders (user_id, order_number, total_amount, status)
        VALUES (@mixedUserId, 'ORD-MIXED-001', 500.00, 'pending');
    "

    exec_sql "
        -- Update the order
        UPDATE orders SET status = 'processing' WHERE order_number = 'ORD-MIXED-001';
    "

    exec_sql "
        -- Delete the order
        DELETE FROM orders WHERE order_number = 'ORD-MIXED-001';
    "

    exec_sql "
        -- Delete the user
        DELETE FROM users WHERE username = 'mixeduser';
    "

    print_info "MIXED operations completed"
}

# Run CLEANUP section
run_cleanup() {
    print_info "Running CLEANUP operations..."

    exec_sql "
        DELETE FROM orders WHERE order_number LIKE 'ORD-TEST-%';
        DELETE FROM orders WHERE order_number LIKE 'ORD-MIXED-%';
        DELETE FROM users WHERE username LIKE 'testuser%';
        DELETE FROM users WHERE username = 'mixeduser';

        PRINT 'CLEANUP completed';
    "

    print_info "CLEANUP operations completed"
}

# Show current data
show_data() {
    print_info "Current data in tables:"

    echo ""
    echo "=== USERS ==="
    exec_sql "SELECT id, username, email, first_name, last_name, is_active FROM users ORDER BY id;"

    echo ""
    echo "=== ORDERS ==="
    exec_sql "SELECT id, user_id, order_number, total_amount, status FROM orders ORDER BY id;"
}

# Show usage
usage() {
    echo "Usage: $0 [section]"
    echo ""
    echo "Sections:"
    echo "  inserts  - Run INSERT operations only"
    echo "  updates  - Run UPDATE operations only"
    echo "  deletes  - Run DELETE operations only"
    echo "  mixed    - Run mixed operations (insert, update, delete in sequence)"
    echo "  cleanup  - Remove all test data"
    echo "  all      - Run all sections (inserts, updates, deletes, mixed)"
    echo "  full     - Run all sections including cleanup at the end"
    echo "  show     - Show current data in tables"
    echo ""
    echo "Environment variables:"
    echo "  MSSQL_CONTAINER - Docker container name (default: mssql)"
    echo "  SA_PASSWORD     - SA password (default: YourStrong@Passw0rd)"
}

# Main
case "${1:-}" in
    inserts)
        run_inserts
        ;;
    updates)
        run_updates
        ;;
    deletes)
        run_deletes
        ;;
    mixed)
        run_mixed
        ;;
    cleanup)
        run_cleanup
        ;;
    all)
        run_inserts
        echo ""
        run_updates
        echo ""
        run_deletes
        echo ""
        run_mixed
        ;;
    full)
        run_inserts
        echo ""
        run_updates
        echo ""
        run_deletes
        echo ""
        run_mixed
        echo ""
        run_cleanup
        ;;
    show)
        show_data
        ;;
    help|--help|-h)
        usage
        ;;
    "")
        print_error "No section specified"
        echo ""
        usage
        exit 1
        ;;
    *)
        print_error "Unknown section: $1"
        echo ""
        usage
        exit 1
        ;;
esac
