-- init-scripts/cassandra/01-init-keyspace.cql
-- Cassandra initialization script for Kedai Kopi system

-- Create keyspace with simple replication strategy
CREATE KEYSPACE IF NOT EXISTS cafe_analytics
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
AND DURABLE_WRITES = true;

-- Use the keyspace
USE cafe_analytics;

-- Main transactions table (for general queries with ALLOW FILTERING)
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id int PRIMARY KEY,
    timestamp timestamp,
    customer_id int,
    employee_id int,
    total_amount decimal,
    payment_method text
);

-- Optimized table for employee-based queries (partition by employee_id)
CREATE TABLE IF NOT EXISTS transactions_by_employee (
    employee_id int,
    timestamp timestamp,
    transaction_id int,
    customer_id int,
    total_amount decimal,
    payment_method text,
    PRIMARY KEY (employee_id, timestamp, transaction_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, transaction_id ASC)
AND COMMENT = 'Optimized for queries filtering by employee_id - enables direct node access';

-- Optimized table for payment method queries (partition by payment_method)
CREATE TABLE IF NOT EXISTS transactions_by_payment (
    payment_method text,
    timestamp timestamp,
    transaction_id int,
    customer_id int,
    employee_id int,
    total_amount decimal,
    PRIMARY KEY (payment_method, timestamp, transaction_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, transaction_id ASC)
AND COMMENT = 'Optimized for queries filtering by payment_method - fast payment analysis';

-- Optimized table for date-based queries (partition by date)
CREATE TABLE IF NOT EXISTS transactions_by_date (
    date date,
    timestamp timestamp,
    transaction_id int,
    customer_id int,
    employee_id int,
    total_amount decimal,
    payment_method text,
    PRIMARY KEY (date, timestamp, transaction_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, transaction_id ASC)
AND COMMENT = 'Optimized for daily reporting and time-series analysis';

-- Optimized table for customer-based queries (partition by customer_id)
CREATE TABLE IF NOT EXISTS transactions_by_customer (
    customer_id int,
    timestamp timestamp,
    transaction_id int,
    employee_id int,
    total_amount decimal,
    payment_method text,
    PRIMARY KEY (customer_id, timestamp, transaction_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, transaction_id ASC)
AND COMMENT = 'Optimized for customer transaction history and analytics';

-- Create materialized views for additional query patterns (optional)
CREATE MATERIALIZED VIEW IF NOT EXISTS high_value_transactions AS
    SELECT transaction_id, timestamp, customer_id, employee_id, total_amount, payment_method
    FROM transactions
    WHERE total_amount > 100000
    AND transaction_id IS NOT NULL
    AND timestamp IS NOT NULL
    PRIMARY KEY (total_amount, timestamp, transaction_id)
    WITH CLUSTERING ORDER BY (timestamp DESC, transaction_id ASC);

-- Insert some sample data for immediate testing (will be replaced by data loader)
INSERT INTO transactions (transaction_id, timestamp, customer_id, employee_id, total_amount, payment_method)
VALUES (1, '2025-01-01 09:00:00', 100, 1, 75000, 'Credit Card');

INSERT INTO transactions_by_employee (employee_id, timestamp, transaction_id, customer_id, total_amount, payment_method)
VALUES (1, '2025-01-01 09:00:00', 1, 100, 75000, 'Credit Card');

INSERT INTO transactions_by_payment (payment_method, timestamp, transaction_id, customer_id, employee_id, total_amount)
VALUES ('Credit Card', '2025-01-01 09:00:00', 1, 100, 1, 75000);

INSERT INTO transactions_by_date (date, timestamp, transaction_id, customer_id, employee_id, total_amount, payment_method)
VALUES ('2025-01-01', '2025-01-01 09:00:00', 1, 100, 1, 75000, 'Credit Card');

INSERT INTO transactions_by_customer (customer_id, timestamp, transaction_id, employee_id, total_amount, payment_method)
VALUES (100, '2025-01-01 09:00:00', 1, 1, 75000, 'Credit Card');
