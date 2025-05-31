# src/core/__init__.py
"""
Core system components for the Dynamic Query System

This package contains the fundamental building blocks:
- DatabaseManager: Connection management for MongoDB, Cassandra, Redis
- SchemaInspector: Dynamic field discovery and schema analysis
- QueryBuilder: Dynamic query construction for any field combination
- PerformanceAnalyzer: Query timing and optimization comparisons
"""

from .database_manager import DatabaseManager
from .schema_inspector import SchemaInspector
from .query_builder import QueryBuilder, QueryFilter, create_filter
from .performance_analyzer import PerformanceAnalyzer, QueryResult, PerformanceComparison

__all__ = [
    'DatabaseManager',
    'SchemaInspector',
    'QueryBuilder',
    'QueryFilter',
    'create_filter',
    'PerformanceAnalyzer',
    'QueryResult',
    'PerformanceComparison'
]
