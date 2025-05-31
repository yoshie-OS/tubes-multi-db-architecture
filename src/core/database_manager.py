# src/core/database_manager.py
import pymongo
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import redis
import logging
from typing import Optional, Dict, Any
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Centralized database connection manager for MongoDB, Cassandra, and Redis.
    Handles all connection logic, health checks, and provides clean interfaces.
    """

    def __init__(self):
        # Connection objects
        self.mongo_client: Optional[pymongo.MongoClient] = None
        self.mongo_db: Optional[pymongo.database.Database] = None
        self.cassandra_cluster: Optional[Cluster] = None
        self.cassandra_session: Optional[object] = None
        self.redis_client: Optional[redis.Redis] = None

        # Connection status tracking
        self.connections_status = {
            'mongodb': False,
            'cassandra': False,
            'redis': False
        }

        # Configuration
        self.config = {
            'mongodb': {
                'uri': 'mongodb://admin:password123@localhost:27017/cafe_analytics?authSource=admin',
                'database': 'cafe_analytics'
            },
            'cassandra': {
                'hosts': ['localhost'],
                'port': 9042,
                'keyspace': 'cafe_analytics'
            },
            'redis': {
                'host': 'localhost',
                'port': 6379,
                'decode_responses': True
            }
        }

    def connect_mongodb(self) -> bool:
        """Connect to MongoDB and verify connection"""
        try:
            logger.info("🍃 Connecting to MongoDB...")

            # Create client with connection timeout
            self.mongo_client = pymongo.MongoClient(
                self.config['mongodb']['uri'],
                serverSelectionTimeoutMS=5000  # 5 second timeout
            )

            # Get database reference
            self.mongo_db = self.mongo_client[self.config['mongodb']['database']]

            # Test connection with server info (more reliable than ping)
            try:
                server_info = self.mongo_client.server_info()
                logger.info(f"   MongoDB version: {server_info.get('version', 'unknown')}")
            except Exception:
                # Fallback: try a simple operation
                self.mongo_db.test_collection.find_one()

            # Verify collections exist
            try:
                collections = self.mongo_db.list_collection_names()
                logger.info(f"   Available collections: {collections}")
            except Exception as e:
                logger.warning(f"   Could not list collections: {e}")
                collections = []

            self.connections_status['mongodb'] = True
            logger.info("✅ MongoDB connected successfully")
            return True

        except Exception as e:
            logger.error(f"❌ MongoDB connection failed: {e}")
            self.connections_status['mongodb'] = False
            return False

    def connect_cassandra(self) -> bool:
        """Connect to Cassandra and verify connection"""
        try:
            logger.info("🏛️ Connecting to Cassandra...")

            # Create cluster connection
            self.cassandra_cluster = Cluster(
                self.config['cassandra']['hosts'],
                port=self.config['cassandra']['port']
            )

            # Get session
            self.cassandra_session = self.cassandra_cluster.connect()

            # Test connection
            result = self.cassandra_session.execute("SELECT release_version FROM system.local")
            version = result.one().release_version
            logger.info(f"   Cassandra version: {version}")

            # Connect to keyspace
            try:
                self.cassandra_session.set_keyspace(self.config['cassandra']['keyspace'])

                # Get available tables
                tables_query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s"
                tables_result = self.cassandra_session.execute(tables_query, [self.config['cassandra']['keyspace']])
                tables = [row.table_name for row in tables_result]
                logger.info(f"   Available tables: {tables}")

            except Exception as e:
                logger.warning(f"   Keyspace {self.config['cassandra']['keyspace']} not found: {e}")
                logger.info("   This is normal if data hasn't been loaded yet")

            self.connections_status['cassandra'] = True
            logger.info("✅ Cassandra connected successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Cassandra connection failed: {e}")
            self.connections_status['cassandra'] = False
            return False

    def connect_redis(self) -> bool:
        """Connect to Redis and verify connection"""
        try:
            logger.info("🔴 Connecting to Redis...")

            # Create Redis client
            self.redis_client = redis.Redis(
                host=self.config['redis']['host'],
                port=self.config['redis']['port'],
                decode_responses=self.config['redis']['decode_responses'],
                socket_connect_timeout=5  # 5 second timeout
            )

            # Test connection
            self.redis_client.ping()

            # Get Redis info
            info = self.redis_client.info()
            logger.info(f"   Redis version: {info.get('redis_version', 'unknown')}")

            self.connections_status['redis'] = True
            logger.info("✅ Redis connected successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e}")
            self.connections_status['redis'] = False
            return False

    def connect_all(self) -> bool:
        """Connect to all databases and return overall success status"""
        logger.info("🚀 Initializing all database connections...")

        # Connect to each database
        mongodb_ok = self.connect_mongodb()
        cassandra_ok = self.connect_cassandra()
        redis_ok = self.connect_redis()

        # Overall status
        all_connected = mongodb_ok and cassandra_ok and redis_ok

        if all_connected:
            logger.info("🎉 All databases connected successfully!")
        else:
            failed_dbs = [db for db, status in self.connections_status.items() if not status]
            logger.error(f"💥 Failed connections: {failed_dbs}")

        return all_connected

    def get_connection_status(self) -> Dict[str, Any]:
        """Get detailed connection status for all databases"""
        status = {
            'overall': all(self.connections_status.values()),
            'databases': self.connections_status.copy()
        }

        # Add detailed info if connected
        if self.connections_status['mongodb'] and self.mongo_db is not None:
            try:
                status['mongodb_details'] = {
                    'collections': self.mongo_db.list_collection_names(),
                    'server_status': 'connected'
                }
            except Exception as e:
                status['mongodb_details'] = {'error': str(e)}

        if self.connections_status['cassandra'] and self.cassandra_session is not None:
            try:
                # Get keyspace tables
                tables_query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s"
                tables_result = self.cassandra_session.execute(tables_query, [self.config['cassandra']['keyspace']])
                tables = [row.table_name for row in tables_result]

                status['cassandra_details'] = {
                    'keyspace': self.config['cassandra']['keyspace'],
                    'tables': tables,
                    'server_status': 'connected'
                }
            except Exception as e:
                status['cassandra_details'] = {'error': str(e)}

        if self.connections_status['redis'] and self.redis_client is not None:
            try:
                info = self.redis_client.info()
                status['redis_details'] = {
                    'version': info.get('redis_version'),
                    'used_memory': info.get('used_memory_human'),
                    'server_status': 'connected'
                }
            except Exception as e:
                status['redis_details'] = {'error': str(e)}

        return status

    def get_data_counts(self) -> Dict[str, Any]:
        """Get record counts from all databases"""
        counts = {}

        # MongoDB counts
        if self.mongo_db is not None:
            try:
                counts['mongodb'] = {}
                for collection_name in self.mongo_db.list_collection_names():
                    count = self.mongo_db[collection_name].count_documents({})
                    counts['mongodb'][collection_name] = count
            except Exception as e:
                counts['mongodb'] = {'error': str(e)}

        # Cassandra counts
        if self.cassandra_session is not None:
            try:
                counts['cassandra'] = {}

                # Get all tables in keyspace
                tables_query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s"
                tables_result = self.cassandra_session.execute(tables_query, [self.config['cassandra']['keyspace']])

                for table_row in tables_result:
                    table_name = table_row.table_name
                    try:
                        # Count records (this will generate warnings for non-partitioned counts)
                        count_query = f"SELECT COUNT(*) FROM {table_name}"
                        count_result = self.cassandra_session.execute(count_query)
                        count = count_result.one().count
                        counts['cassandra'][table_name] = count
                    except Exception as e:
                        counts['cassandra'][table_name] = f"Error: {str(e)}"

            except Exception as e:
                counts['cassandra'] = {'error': str(e)}

        return counts

    def close_all_connections(self):
        """Gracefully close all database connections"""
        logger.info("🔌 Closing all database connections...")

        if self.mongo_client is not None:
            self.mongo_client.close()
            logger.info("   MongoDB connection closed")

        if self.cassandra_cluster is not None:
            self.cassandra_cluster.shutdown()
            logger.info("   Cassandra connection closed")

        if self.redis_client is not None:
            self.redis_client.close()
            logger.info("   Redis connection closed")

        # Reset status
        self.connections_status = {db: False for db in self.connections_status}
        logger.info("✅ All connections closed")

    def __enter__(self):
        """Context manager entry"""
        self.connect_all()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close_all_connections()


# Example usage and testing
if __name__ == "__main__":
    # Test the database manager
    db_manager = DatabaseManager()

    # Test connections
    if db_manager.connect_all():
        print("\n📊 Connection Status:")
        status = db_manager.get_connection_status()
        for db, connected in status['databases'].items():
            print(f"   {db}: {'✅' if connected else '❌'}")

        print("\n📈 Data Counts:")
        counts = db_manager.get_data_counts()
        for db, db_counts in counts.items():
            print(f"   {db}:")
            if isinstance(db_counts, dict) and 'error' not in db_counts:
                for table, count in db_counts.items():
                    print(f"     {table}: {count}")
            else:
                print(f"     {db_counts}")

    # Clean up
    db_manager.close_all_connections()
