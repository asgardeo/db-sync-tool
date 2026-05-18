-- Create the test database
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'testdb')
BEGIN
    CREATE DATABASE testdb;
END
GO

USE testdb;
GO

-- Create Users table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'users')
BEGIN
    CREATE TABLE users (
        id INT IDENTITY(1,1) PRIMARY KEY,
        username NVARCHAR(100) NOT NULL,
        email NVARCHAR(255) NOT NULL,
        first_name NVARCHAR(100),
        last_name NVARCHAR(100),
        created_at DATETIME2 DEFAULT GETDATE(),
        updated_at DATETIME2 DEFAULT GETDATE(),
        is_active BIT DEFAULT 1
    );
END
GO

-- Create Orders table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'orders')
BEGIN
    CREATE TABLE orders (
        id INT IDENTITY(1,1) PRIMARY KEY,
        user_id INT NOT NULL,
        order_number NVARCHAR(50) NOT NULL,
        total_amount DECIMAL(10, 2) NOT NULL,
        status NVARCHAR(50) DEFAULT 'pending',
        created_at DATETIME2 DEFAULT GETDATE(),
        updated_at DATETIME2 DEFAULT GETDATE(),
        CONSTRAINT FK_orders_users FOREIGN KEY (user_id) REFERENCES users(id)
    );
END
GO

-- Insert dummy data into Users table
INSERT INTO users (username, email, first_name, last_name, is_active)
VALUES
    ('johndoe', 'john.doe@example.com', 'John', 'Doe', 1),
    ('janedoe', 'jane.doe@example.com', 'Jane', 'Doe', 1),
    ('bobsmith', 'bob.smith@example.com', 'Bob', 'Smith', 1),
    ('alicejones', 'alice.jones@example.com', 'Alice', 'Jones', 0),
    ('charliebrwn', 'charlie.brown@example.com', 'Charlie', 'Brown', 1);
GO

-- Insert dummy data into Orders table
INSERT INTO orders (user_id, order_number, total_amount, status)
VALUES
    (1, 'ORD-001', 150.00, 'completed'),
    (1, 'ORD-002', 75.50, 'pending'),
    (2, 'ORD-003', 200.00, 'completed'),
    (3, 'ORD-004', 45.99, 'shipped'),
    (3, 'ORD-005', 320.00, 'pending'),
    (4, 'ORD-006', 89.99, 'cancelled'),
    (5, 'ORD-007', 560.00, 'completed');
GO

-- Enable CDC on the database
EXEC sys.sp_cdc_enable_db;
GO

-- Enable CDC on the Users table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'users',
    @role_name = NULL,
    @supports_net_changes = 1;
GO

-- Enable CDC on the Orders table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'orders',
    @role_name = NULL,
    @supports_net_changes = 1;
GO

-- Verify CDC is enabled
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'testdb';
GO

SELECT name, is_tracked_by_cdc FROM sys.tables WHERE is_tracked_by_cdc = 1;
GO

PRINT 'MSSQL setup completed successfully with CDC enabled!';
GO
