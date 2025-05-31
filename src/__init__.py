# src/__init__.py
"""
Kedai Kopi Nusantara - Dynamic Multi-Database Query System
Source code package initialization

ROBD Assignment - Spring 2025 | Group 3
"""

# Import all core modules
from . import core
from . import interfaces
from . import data_loaders
from . import utils  # Add this line

__all__ = ['core', 'interfaces', 'data_loaders', 'utils']
__version__ = "1.0.0"
__author__ = "Group 3"
__description__ = "Dynamic Multi-Database Query System for NoSQL Optimization Demonstration"
