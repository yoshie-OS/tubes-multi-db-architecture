# src/interfaces/__init__.py
"""
User interface components for the Dynamic Query System

This package contains user-facing interfaces:
- CLIInterface: Interactive command-line interface for demonstrations
- Future: WebInterface, APIInterface for extended functionality
"""

from .cli_interface import CLIInterface

__all__ = [
    'CLIInterface'
]
