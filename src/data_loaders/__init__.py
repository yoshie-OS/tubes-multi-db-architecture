# src/data_loaders/__init__.py
"""
Data loading and management components

This package handles:
- DataLoader: Sample data initialization for MongoDB and Cassandra
- Future: DataValidator, DataGenerator for extended functionality
"""

from .data_loader import DataLoader

__all__ = [
    'DataLoader'
]
