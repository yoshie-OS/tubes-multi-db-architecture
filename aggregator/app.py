# aggregator/app.py
from flask import Flask, jsonify
import os
from pymongo import MongoClient
from cassandra.cluster import Cluster
import redis
import logging

# Import scenarios and data blueprints
from query_scenarios import scenarios_bp, data_bp, init_scenarios

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Register blueprints
app.register_blueprint(scenarios_bp)
app.register_blueprint(data_bp)

# Database connections (will be initialized in main)
mongo_client = None
mongo_db = None
cassandra_session = None
redis_client = None

@app.route('/')
def health_check():
    """Basic health check endpoint"""
    return jsonify({
        "status": "healthy",
        "message": "Kedai Kopi Nusantara - Big Data Aggregator API is running",
        "databases": {
            "mongodb": "connected" if mongo_db is not None else "disconnected",
            "cassandra": "connected" if cassandra_session is not None else "disconnected",
            "redis": "connected" if redis_client is not None else "disconnected"
        }
    })

@app.route('/api/status')
def api_status():
    """API status endpoint with database connectivity check"""
    status = {"api": "running"}

    # Check MongoDB
    try:
        if mongo_db is not None:
            mongo_db.admin.command('ping')
            status["mongodb"] = "connected"
            status["mongodb_collections"] = mongo_db.list_collection_names()
        else:
            status["mongodb"] = "not_initialized"
    except Exception as e:
        status["mongodb"] = f"error: {str(e)}"

    # Check Cassandra
    try:
        if cassandra_session is not None:
            cassandra_session.execute("SELECT now() FROM system.local")
            status["cassandra"] = "connected"
            # Get table info
            tables_result = cassandra_session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'cafe_analytics'")
            status["cassandra_tables"] = [row.table_name for row in tables_result]
        else:
            status["cassandra"] = "not_initialized"
    except Exception as e:
        status["cassandra"] = f"error: {str(e)}"

    # Check Redis
    try:
        if redis_client is not None:
            redis_client.ping()
            status["redis"] = "connected"
        else:
            status["redis"] = "not_initialized"
    except Exception as e:
        status["redis"] = f"error: {str(e)}"

    return jsonify(status)

@app.route('/api/scenarios')
def scenarios_info():
    """Information about available scenarios"""
    return jsonify({
        "scenarios": {
            "without_optimization": {
                "description": "Queries without indexing and optimization",
                "characteristics": [
                    "Full table scans",
                    "No index usage",
                    "ALLOW FILTERING in Cassandra",
                    "Collection scans in MongoDB",
                    "N+1 query problems"
                ]
            },
            "with_optimization": {
                "description": "Queries with indexing and optimization",
                "characteristics": [
                    "Index utilization",
                    "Compound indexes",
                    "Batch processing",
                    "Optimized query plans",
                    "Efficient data joining"
                ]
            }
        },
        "query_types": {
            "db1_only": "Query to Cassandra only (transaction analytics)",
            "db2_only": "Query to MongoDB only (employee/menu data)",
            "combined": "Query combining data from both databases"
        },
        "available_endpoints": [
            "/api/scenarios/compare-all",
            "/api/scenarios/db1/no-optimization",
            "/api/scenarios/db1/with-optimization",
            "/api/scenarios/db2/no-optimization",
            "/api/scenarios/db2/with-optimization",
            "/api/scenarios/combined/no-optimization",
            "/api/scenarios/combined/with-optimization"
        ]
    })

def init_databases():
    """Initialize database connections"""
    global mongo_client, mongo_db, cassandra_session, redis_client

    try:
        # MongoDB connection
        mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://admin:password123@mongodb:27017/cafe_analytics?authSource=admin')
        logger.info(f"Connecting to MongoDB: {mongodb_uri}")
        mongo_client = MongoClient(mongodb_uri)
        mongo_db = mongo_client.cafe_analytics
        logger.info("MongoDB connected successfully")

        # Cassandra connection
        cassandra_hosts = os.getenv('CASSANDRA_HOSTS', 'cassandra').split(',')
        logger.info(f"Connecting to Cassandra: {cassandra_hosts}")
        cluster = Cluster(cassandra_hosts, port=9042)
        cassandra_session = cluster.connect('cafe_analytics')
        logger.info("Cassandra connected successfully")

        # Redis connection
        redis_host = os.getenv('REDIS_HOST', 'redis')
        redis_port = int(os.getenv('REDIS_PORT', '6379'))
        logger.info(f"Connecting to Redis: {redis_host}:{redis_port}")
        redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        logger.info("Redis connected successfully")

        # Initialize scenarios with database connections
        init_scenarios(mongo_db, cassandra_session)
        logger.info("Query scenarios initialized successfully")

    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        raise

if __name__ == '__main__':
    # Initialize databases
    init_databases()

    # Run Flask app
    app.run(
        host='0.0.0.0',
        port=int(os.getenv('PORT', 5000)),
        debug=os.getenv('FLASK_ENV') == 'development'
    )
