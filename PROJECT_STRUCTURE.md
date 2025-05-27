# Project Structure

This document outlines the organization of the **tubes-multi-db-architecture** project - a multi-database system that demonstrates data distribution patterns across MongoDB and Cassandra with a unified aggregation layer.

## Directory Layout

```
tubes-multi-db-architecture/

├── data/
│   ├── raw/                 # Original synthetic data files
│   ├── processed/           # Cleaned and transformed data ready for DB insertion
│   └── scripts/
│       ├── generate_data.py # LLM-powered synthetic data generation
│       └── preprocess.py    # Data cleaning and transformation pipeline

├── databases/
│   ├── mongodb/
│   │   ├── init/            # Database initialization scripts and seed data
│   │   ├── schemas/         # MongoDB collection schemas and validation rules
│   │   ├── indexes/         # Index definitions for query optimization
│   │   └── queries/         # Common MongoDB queries and aggregations
│   │
│   └── cassandra/
│       ├── init/            # Keyspace setup and initial table creation
│       ├── schemas/         # CQL table definitions and data models
│       ├── indexes/         # Secondary indexes and materialized views
│       └── queries/         # CQL queries and prepared statements

└── aggregator/
    ├── app.py              # Main Flask application entry point
    ├── models/             # Data models and database connection handlers
    ├── routes/             # API endpoints for cross-database operations
    └── utils/              # Helper functions and shared utilities
```

## Purpose

This architecture allows for:
- **Data Distribution**: Different data types optimized for their respective database strengths
- **Cross-Database Queries**: Unified API layer for querying across both systems
- **Performance Testing**: Benchmarking MongoDB vs Cassandra for various workloads
- **Learning**: Hands-on experience with polyglot persistence patterns
