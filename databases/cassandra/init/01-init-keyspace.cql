-- databases/cassandra/init/01-init-keyspace.cql
-- Cassandra initialization script for e-commerce analytics

-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS cafe_analytics
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE ecommerce_analytics;

-- Transactions table (optimized for time-series queries)
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id UUID,
    timestamp,
    customer_id,
    employee_id,
    total_amount,
    payment_method,
    PRIMARY KEY (user_id, , transaction_id)
) WITH CLUSTERING ORDER BY (transaction_date DESC)
AND comment = 'Transactions partitioned by user_id, clustered by transaction_date';

DESCRIBE KEYSPACE ecommerce_analytics;
