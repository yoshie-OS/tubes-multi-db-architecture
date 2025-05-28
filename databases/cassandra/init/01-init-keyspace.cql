-- databases/cassandra/init/01-init-keyspace.cql
-- Cassandra initialization script for Kedai Kopi Nusantara

-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS cafe_analytics
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE cafe_analytics;

-- 1. Main transactions table (by transaction_id)
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id INT PRIMARY KEY,
    timestamp TIMESTAMP,
    customer_id INT,
    employee_id INT,
    total_amount DECIMAL,
    payment_method TEXT
) WITH comment = 'Main transactions table';

-- 2. Transactions by employee (untuk query employee performance)
CREATE TABLE IF NOT EXISTS transactions_by_employee (
    employee_id INT,
    timestamp TIMESTAMP,
    transaction_id INT,
    customer_id INT,
    total_amount DECIMAL,
    payment_method TEXT,
    PRIMARY KEY (employee_id, timestamp, transaction_id)
) WITH CLUSTERING ORDER BY (timestamp DESC)
AND comment = 'Transactions partitioned by employee_id for employee analytics';

-- 3. Transactions by payment method (untuk query payment analysis)
CREATE TABLE IF NOT EXISTS transactions_by_payment (
    payment_method TEXT,
    timestamp TIMESTAMP,
    transaction_id INT,
    customer_id INT,
    employee_id INT,
    total_amount DECIMAL,
    PRIMARY KEY (payment_method, timestamp, transaction_id)
) WITH CLUSTERING ORDER BY (timestamp DESC)
AND comment = 'Transactions partitioned by payment_method for payment analytics';

-- 4. Transactions by date (untuk query daily/monthly analytics)
CREATE TABLE IF NOT EXISTS transactions_by_date (
    date DATE,
    timestamp TIMESTAMP,
    transaction_id INT,
    customer_id INT,
    employee_id INT,
    total_amount DECIMAL,
    payment_method TEXT,
    PRIMARY KEY (date, timestamp, transaction_id)
) WITH CLUSTERING ORDER BY (timestamp DESC)
AND comment = 'Transactions partitioned by date for time-series analytics';

-- 5. Customer transactions (untuk query customer behavior)
CREATE TABLE IF NOT EXISTS transactions_by_customer (
    customer_id INT,
    timestamp TIMESTAMP,
    transaction_id INT,
    employee_id INT,
    total_amount DECIMAL,
    payment_method TEXT,
    PRIMARY KEY (customer_id, timestamp, transaction_id)
) WITH CLUSTERING ORDER BY (timestamp DESC)
AND comment = 'Transactions partitioned by customer_id for customer analytics';

DESCRIBE KEYSPACE cafe_analytics;
