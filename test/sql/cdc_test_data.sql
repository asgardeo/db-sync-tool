-- CDC Test Data Script
-- This script contains INSERT, UPDATE, and DELETE operations to test CDC functionality
-- Each section is marked with comments for selective execution

USE testdb;
GO

-- ============================================================================
-- SECTION: INSERTS
-- ============================================================================

-- Insert new users
INSERT INTO users (username, email, first_name, last_name, is_active)
VALUES
    ('testuser1', 'testuser1@example.com', 'Test', 'User1', 1),
    ('testuser2', 'testuser2@example.com', 'Test', 'User2', 1),
    ('testuser3', 'testuser3@example.com', 'Test', 'User3', 0);
GO

-- Insert new orders
INSERT INTO orders (user_id, order_number, total_amount, status)
VALUES
    (1, 'ORD-TEST-001', 99.99, 'pending'),
    (2, 'ORD-TEST-002', 149.50, 'pending'),
    (1, 'ORD-TEST-003', 250.00, 'pending');
GO

PRINT 'INSERTS completed';
GO

-- ============================================================================
-- SECTION: UPDATES
-- ============================================================================

-- Update user information
UPDATE users
SET first_name = 'Updated',
    last_name = 'Name',
    updated_at = GETDATE()
WHERE username = 'testuser1';
GO

UPDATE users
SET is_active = 0,
    updated_at = GETDATE()
WHERE username = 'testuser2';
GO

UPDATE users
SET email = 'newemail@example.com',
    updated_at = GETDATE()
WHERE username = 'testuser3';
GO

-- Update order status
UPDATE orders
SET status = 'shipped',
    updated_at = GETDATE()
WHERE order_number = 'ORD-TEST-001';
GO

UPDATE orders
SET status = 'completed',
    total_amount = 155.00,
    updated_at = GETDATE()
WHERE order_number = 'ORD-TEST-002';
GO

PRINT 'UPDATES completed';
GO

-- ============================================================================
-- SECTION: DELETES
-- ============================================================================

-- Delete orders first (due to foreign key constraint)
DELETE FROM orders WHERE order_number = 'ORD-TEST-003';
GO

-- Delete users (only those without orders)
DELETE FROM users WHERE username = 'testuser3';
GO

PRINT 'DELETES completed';
GO

-- ============================================================================
-- SECTION: MIXED OPERATIONS (for testing ordering)
-- ============================================================================

-- Insert a new user
INSERT INTO users (username, email, first_name, last_name, is_active)
VALUES ('mixeduser', 'mixed@example.com', 'Mixed', 'User', 1);
GO

-- Immediately update the same user
UPDATE users
SET first_name = 'MixedUpdated'
WHERE username = 'mixeduser';
GO

-- Insert an order for this user
DECLARE @mixedUserId INT;
SELECT @mixedUserId = id FROM users WHERE username = 'mixeduser';
INSERT INTO orders (user_id, order_number, total_amount, status)
VALUES (@mixedUserId, 'ORD-MIXED-001', 500.00, 'pending');
GO

-- Update the order
UPDATE orders
SET status = 'processing'
WHERE order_number = 'ORD-MIXED-001';
GO

-- Delete the order
DELETE FROM orders WHERE order_number = 'ORD-MIXED-001';
GO

-- Delete the user
DELETE FROM users WHERE username = 'mixeduser';
GO

PRINT 'MIXED OPERATIONS completed';
GO

-- ============================================================================
-- SECTION: CLEANUP (removes all test data)
-- ============================================================================

-- Remove test orders
DELETE FROM orders WHERE order_number LIKE 'ORD-TEST-%';
GO

-- Remove test users
DELETE FROM users WHERE username LIKE 'testuser%';
GO

PRINT 'CLEANUP completed';
GO
