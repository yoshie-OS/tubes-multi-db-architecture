#!/usr/bin/env python3
"""
Kedai Kopi Aggregator Service - REST API for Dynamic Query System

This Flask service provides HTTP endpoints for the dynamic query system,
allowing web-based access to the multi-database query capabilities.

ROBD Assignment - Spring 2025 | Group 3
"""

import os
import sys
import logging
from flask import Flask, request, jsonify
from flask_cors import CORS
import time
from datetime import datetime
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add src to path for imports
sys.path.append('/app')
sys.path.append('/app/src')

try:
    from src.core.database_manager import DatabaseManager
    from src.core.schema_inspector import SchemaInspector
    from src.core.query_builder import QueryBuilder, create_filter
    from src.core.performance_analyzer import PerformanceAnalyzer
    from src.data_loaders.data_loader import DataLoader
except ImportError as e:
    logger.error(f"Import error: {e}")
    # Fallback for development
    sys.path.append('../src')
    try:
        from core.database_manager import DatabaseManager
        from core.schema_inspector import SchemaInspector
        from core.query_builder import QueryBuilder, create_filter
        from core.performance_analyzer import PerformanceAnalyzer
        from data_loaders.data_loader import DataLoader
    except ImportError as e2:
        logger.error(f"Failed to import even with fallback: {e2}")
        raise

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Global components
db_manager = None
schema_inspector = None
query_builder = None
performance_analyzer = None
data_loader = None

def initialize_system():
    """Initialize all system components"""
    global db_manager, schema_inspector, query_builder, performance_analyzer, data_loader

    logger.info("🚀 Initializing Cafe Aggregator Service...")

    try:
        # Initialize database manager
        db_manager = DatabaseManager()
        if not db_manager.connect_all():
            raise Exception("Failed to connect to databases")

        # Initialize other components
        schema_inspector = SchemaInspector(db_manager)
        query_builder = QueryBuilder(db_manager, schema_inspector)
        performance_analyzer = PerformanceAnalyzer(db_manager, schema_inspector)
        data_loader = DataLoader(db_manager)

        # Inspect schemas
        schema_inspector.inspect_all_schemas()

        logger.info("✅ Cafe Aggregator Service initialized successfully")
        return True

    except Exception as e:
        logger.error(f"❌ Failed to initialize system: {e}")
        return False

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        status = {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'service': 'Kedai Kopi Aggregator',
            'version': '1.0.0'
        }

        if db_manager:
            db_status = db_manager.get_connection_status()
            status['databases'] = db_status['databases']

        return jsonify(status), 200

    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/schemas', methods=['GET'])
def get_schemas():
    """Get database schemas and available fields"""
    try:
        if not schema_inspector:
            return jsonify({'error': 'System not initialized'}), 500

        schemas = schema_inspector.inspect_all_schemas()
        return jsonify({
            'status': 'success',
            'schemas': schemas,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Schema inspection error: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/data/status', methods=['GET'])
def get_data_status():
    """Get current data counts and status"""
    try:
        if not db_manager:
            return jsonify({'error': 'System not initialized'}), 500

        counts = db_manager.get_data_counts()
        status = db_manager.get_connection_status()

        return jsonify({
            'status': 'success',
            'data_counts': counts,
            'connection_status': status,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Data status error: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/query/build', methods=['POST'])
def build_query():
    """Build a query configuration from user input"""
    try:
        if not query_builder:
            return jsonify({'error': 'System not initialized'}), 500

        query_spec = request.json
        if not query_spec:
            return jsonify({'error': 'No query specification provided'}), 400

        # Build query configuration
        query_config = query_builder.build_query_from_user_input(query_spec)

        # Validate query
        validation = query_builder.validate_query_config(query_config)

        return jsonify({
            'status': 'success',
            'query_config': query_config,
            'validation': validation,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Query building error: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'trace': traceback.format_exc(),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/query/execute', methods=['POST'])
def execute_query():
    """Execute a query with performance analysis"""
    try:
        if not performance_analyzer:
            return jsonify({'error': 'System not initialized'}), 500

        query_config = request.json
        if not query_config:
            return jsonify({'error': 'No query configuration provided'}), 400

        # Execute query with performance comparison
        start_time = time.time()
        comparison = performance_analyzer.compare_optimization_scenarios(query_config)
        execution_time = time.time() - start_time

        # Get performance summary
        summary = performance_analyzer.get_performance_summary(comparison)

        return jsonify({
            'status': 'success',
            'performance_comparison': {
                'optimized': {
                    'execution_time_ms': comparison.optimized_result.execution_time_ms,
                    'result_count': comparison.optimized_result.result_count,
                    'success': comparison.optimized_result.success,
                    'results': comparison.optimized_result.results[:10]  # Limit results
                },
                'unoptimized': {
                    'execution_time_ms': comparison.unoptimized_result.execution_time_ms,
                    'result_count': comparison.unoptimized_result.result_count,
                    'success': comparison.unoptimized_result.success
                },
                'performance_improvement': comparison.performance_improvement,
                'winner': comparison.winner,
                'analysis': comparison.analysis,
                'recommendations': comparison.recommendations
            },
            'total_api_time_ms': round(execution_time * 1000, 2),
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Query execution error: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'trace': traceback.format_exc(),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/demo/scenarios', methods=['GET'])
def get_demo_scenarios():
    """Get predefined demo scenarios for quick testing"""
    scenarios = {
        'cassandra_payment': {
            'name': 'Cassandra Payment Method Query',
            'description': 'Query transactions by payment method using partition key optimization',
            'query_spec': {
                'type': 'single',
                'database': 'cassandra',
                'target': 'transactions_by_payment',
                'filters': [
                    {'field': 'payment_method', 'operator': '=', 'value': 'Credit Card'}
                ],
                'use_optimization': True
            }
        },
        'mongodb_employees': {
            'name': 'MongoDB Employee Query',
            'description': 'Query employees by position with performance rating sort',
            'query_spec': {
                'type': 'single',
                'database': 'mongodb',
                'target': 'employees',
                'filters': [
                    {'field': 'position', 'operator': '=', 'value': 'Barista'}
                ],
                'sort_field': 'performance_rating',
                'sort_order': -1,
                'limit': 5,
                'use_optimization': True
            }
        },
        'cross_database': {
            'name': 'Cross-Database Employee-Transaction Join',
            'description': 'Join employee data with transaction data',
            'query_spec': {
                'type': 'cross',
                'mongodb': {
                    'collection': 'employees',
                    'filters': [
                        {'field': 'position', 'operator': '=', 'value': 'Barista'}
                    ]
                },
                'cassandra': {
                    'table': 'transactions_by_employee',
                    'filters': [
                        {'field': 'total_amount', 'operator': '>', 'value': 50000}
                    ]
                },
                'join_field': 'employee_id',
                'use_optimization': True
            }
        }
    }

    return jsonify({
        'status': 'success',
        'scenarios': scenarios,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/data/load', methods=['POST'])
def load_sample_data():
    """Load sample data into databases"""
    try:
        if not data_loader:
            return jsonify({'error': 'System not initialized'}), 500

        # Load all sample data
        results = data_loader.load_all_sample_data()

        return jsonify({
            'status': 'success',
            'load_results': results,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Data loading error: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/fields/<database>', methods=['GET'])
def get_available_fields(database):
    """Get available fields for a specific database"""
    try:
        if not schema_inspector:
            return jsonify({'error': 'System not initialized'}), 500

        table_or_collection = request.args.get('target')
        suggestions = schema_inspector.get_field_suggestions(database, table_or_collection)

        return jsonify({
            'status': 'success',
            'database': database,
            'target': table_or_collection,
            'field_suggestions': suggestions,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Field suggestions error: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'status': 'error',
        'error': 'Endpoint not found',
        'available_endpoints': [
            '/health',
            '/api/schemas',
            '/api/data/status',
            '/api/query/build',
            '/api/query/execute',
            '/api/demo/scenarios',
            '/api/data/load',
            '/api/fields/<database>'
        ],
        'timestamp': datetime.now().isoformat()
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'status': 'error',
        'error': 'Internal server error',
        'timestamp': datetime.now().isoformat()
    }), 500

if __name__ == '__main__':
    # Initialize system on startup
    if initialize_system():
        logger.info("🌐 Starting Flask Aggregator Service on port 5000...")
        app.run(host='0.0.0.0', port=5000, debug=True)
    else:
        logger.error("❌ Failed to initialize system, exiting...")
        sys.exit(1)
