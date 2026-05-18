# Database Test Environment Setup

This folder contains Docker Compose configurations and SQL scripts to quickly set up MSSQL and PostgreSQL test databases with pre-configured schemas and sample data.

## Prerequisites

- Docker and Docker Compose installed on your system
- Bash shell (for running the setup script)
- Basic familiarity with SQL databases

## Folder Structure

```
test/
├── docker-compose-mssql.yml    # Docker Compose configuration for MSSQL Server 2022
├── docker-compose-postgres.yml # Docker Compose configuration for PostgreSQL 16
├── sql/
│   ├── mssql_setup.sql         # SQL script to create tables, insert data, and enable CDC
│   └── postgres_setup.sql      # SQL script to create empty tables with the same schema
├── setup.sh                    # Main orchestration script
└── README.md                   # This file
```

## Database Schemas

Both databases contain the following tables with identical schemas:

### Users Table
- `id` - Primary key (auto-increment)
- `username` - Username (varchar)
- `email` - Email address (varchar)
- `first_name` - First name (varchar)
- `last_name` - Last name (varchar)
- `created_at` - Timestamp created
- `updated_at` - Timestamp updated
- `is_active` - Boolean flag for active status

### Orders Table
- `id` - Primary key (auto-increment)
- `user_id` - Foreign key to users table
- `order_number` - Order reference number (varchar)
- `total_amount` - Order total (decimal)
- `status` - Order status (varchar) - values: pending, completed, shipped, cancelled
- `created_at` - Timestamp created
- `updated_at` - Timestamp updated

## MSSQL-Specific Features

The MSSQL setup includes:

- **Sample Data**: 5 pre-inserted users and 7 pre-inserted orders
- **Change Data Capture (CDC)**: Enabled on both `users` and `orders` tables
- **SQL Server Agent**: Enabled (required for CDC)
- **Database Edition**: Developer Edition

## Quick Start

### Run All Setup (Both Databases)

```bash
cd test
./setup.sh
```

This will:
1. Clean up any existing containers
2. Start both MSSQL and PostgreSQL containers
3. Wait for both databases to be ready
4. Execute the SQL setup scripts
5. Display connection details

### Setup Only MSSQL

```bash
./setup.sh mssql
```

### Setup Only PostgreSQL

```bash
./setup.sh postgres
```

### Clean Up All Containers and Volumes

```bash
./setup.sh clean
```

## Connection Details

### MSSQL Server

| Property | Value |
|----------|-------|
| Host | localhost |
| Port | 1433 |
| Database | testdb |
| Username | sa |
| Password | StrongPassword123! |
| Container Name | mssql-test-db |

**Connection String Examples:**

- SQL Server Management Studio: `Server=localhost,1433;User Id=sa;Password=StrongPassword123!;Database=testdb;TrustServerCertificate=true;`
- Python: `pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=localhost,1433;Database=testdb;UID=sa;PWD=StrongPassword123!;TrustServerCertificate=yes;')`
- Node.js: `Server=localhost;Database=testdb;UID=sa;PWD=StrongPassword123!;TrustServerCertificate=true;`

### PostgreSQL

| Property | Value |
|----------|-------|
| Host | localhost |
| Port | 5432 |
| Database | testdb |
| Username | postgres |
| Password | StrongPassword123! |
| Container Name | postgres-test-db |

**Connection String Examples:**

- psql: `psql -h localhost -U postgres -d testdb`
- Python: `psycopg2.connect("dbname=testdb user=postgres password=StrongPassword123! host=localhost port=5432")`
- Node.js: `postgres://postgres:StrongPassword123!@localhost:5432/testdb`

## Accessing the Databases

### Using Command Line

#### MSSQL
```bash
# Using sqlcmd inside container
docker exec -it mssql-test-db /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "StrongPassword123!" -C

# Once inside, run SQL queries:
# > USE testdb;
# > GO
# > SELECT * FROM users;
# > GO
```

#### PostgreSQL
```bash
# Using psql inside container
docker exec -it postgres-test-db psql -U postgres -d testdb

# Once inside, run SQL queries:
# testdb=# SELECT * FROM users;
# testdb=# SELECT * FROM orders;
```

### Using GUI Tools

#### MSSQL
- **SQL Server Management Studio** (SSMS) - Microsoft's official tool
- **DBeaver** - Free, cross-platform
- **VS Code** with mssql extension

#### PostgreSQL
- **pgAdmin** - Web-based PostgreSQL management tool
- **DBeaver** - Free, cross-platform
- **VS Code** with PostgreSQL extension
- **TablePlus** - Modern database client

## Verifying CDC is Enabled (MSSQL Only)

After setup, you can verify CDC is enabled:

```bash
docker exec mssql-test-db /opt/mssql-tools18/bin/sqlcmd \
  -S localhost \
  -U sa \
  -P "StrongPassword123!" \
  -C \
  -Q "USE testdb; SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'testdb'; SELECT name, is_tracked_by_cdc FROM sys.tables WHERE is_tracked_by_cdc = 1;"
```

Expected output:
- Database `testdb` should have `is_cdc_enabled = 1`
- Tables `users` and `orders` should have `is_tracked_by_cdc = 1`

## Sample Data Details

### Users (MSSQL Only)

| id | username | email | first_name | last_name | is_active |
|----|----------|-------|------------|-----------|-----------|
| 1 | johndoe | john.doe@example.com | John | Doe | 1 |
| 2 | janedoe | jane.doe@example.com | Jane | Doe | 1 |
| 3 | bobsmith | bob.smith@example.com | Bob | Smith | 1 |
| 4 | alicejones | alice.jones@example.com | Alice | Jones | 0 |
| 5 | charliebrwn | charlie.brown@example.com | Charlie | Brown | 1 |

### Orders (MSSQL Only)

| id | user_id | order_number | total_amount | status |
|----|---------|--------------|--------------|--------|
| 1 | 1 | ORD-001 | 150.00 | completed |
| 2 | 1 | ORD-002 | 75.50 | pending |
| 3 | 2 | ORD-003 | 200.00 | completed |
| 4 | 3 | ORD-004 | 45.99 | shipped |
| 5 | 3 | ORD-005 | 320.00 | pending |
| 6 | 4 | ORD-006 | 89.99 | cancelled |
| 7 | 5 | ORD-007 | 560.00 | completed |

## Troubleshooting

### Containers fail to start
- Ensure Docker is running: `docker info`
- Check available disk space: `docker system df`
- Check system resources (CPU, memory)

### MSSQL connection timeout
- Wait longer for MSSQL to initialize (first start can take 30+ seconds)
- Check container logs: `docker logs mssql-test-db`
- Verify port 1433 is not in use: `lsof -i :1433`

### PostgreSQL connection timeout
- Check container logs: `docker logs postgres-test-db`
- Verify port 5432 is not in use: `lsof -i :5432`

### CDC not enabled in MSSQL
- Ensure SQL Server Agent is running (it starts automatically in the container)
- Verify the setup script executed without errors
- Check the container logs for any SQL errors

### Permission denied when running setup.sh
- Make the script executable: `chmod +x test/setup.sh`

## Cleanup

To remove all containers, volumes, and data:

```bash
./setup.sh clean
```

This will stop and remove both database containers and their volumes.

## Notes

- The databases are configured for development/testing purposes only
- Credentials are hardcoded and should never be used in production
- CDC is enabled on MSSQL for database change tracking scenarios
- PostgreSQL tables are created empty (no sample data) to demonstrate the schema only
- All timestamps use server time in both databases

## Additional Commands

### View container status
```bash
docker-compose -f docker-compose-mssql.yml ps
docker-compose -f docker-compose-postgres.yml ps
```

### View container logs
```bash
docker logs mssql-test-db
docker logs postgres-test-db
```

### Stop containers (without removing them)
```bash
docker-compose -f docker-compose-mssql.yml stop
docker-compose -f docker-compose-postgres.yml stop
```

### Start stopped containers
```bash
docker-compose -f docker-compose-mssql.yml start
docker-compose -f docker-compose-postgres.yml start
```

## Support

For issues or questions, refer to:
- Docker documentation: https://docs.docker.com/
- MSSQL documentation: https://learn.microsoft.com/en-us/sql/
- PostgreSQL documentation: https://www.postgresql.org/docs/
