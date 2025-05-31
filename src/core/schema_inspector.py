# src/core/schema_inspector.py
from typing import Dict, List, Any, Optional, Set, Union
import logging
from datetime import datetime
from decimal import Decimal
from .database_manager import DatabaseManager

logger = logging.getLogger(__name__)

class SchemaInspector:
    """
    Dynamic schema discovery for MongoDB and Cassandra databases.
    Automatically detects available fields, data types, and suggests appropriate operators.
    """

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.mongodb_schema = {}
        self.cassandra_schema = {}
        self.field_operators = {}

    def inspect_all_schemas(self) -> Dict[str, Any]:
        """Inspect schemas for all databases and return comprehensive field information"""
        logger.info("🔍 Starting comprehensive schema inspection...")

        schema_info = {
            'mongodb': {},
            'cassandra': {},
            'cross_database_fields': [],
            'inspection_time': datetime.now().isoformat()
        }

        # Inspect MongoDB if connected
        if self.db_manager.connections_status['mongodb']:
            schema_info['mongodb'] = self.inspect_mongodb_schema()

        # Inspect Cassandra if connected
        if self.db_manager.connections_status['cassandra']:
            schema_info['cassandra'] = self.inspect_cassandra_schema()

        # Find common fields for cross-database queries
        schema_info['cross_database_fields'] = self.find_cross_database_fields()

        # Store for later use
        self.mongodb_schema = schema_info['mongodb']
        self.cassandra_schema = schema_info['cassandra']

        logger.info("✅ Schema inspection complete")
        return schema_info

    def inspect_mongodb_schema(self) -> Dict[str, Any]:
        """Inspect MongoDB collections and discover all fields and types"""
        logger.info("🍃 Inspecting MongoDB schema...")

        mongodb_info = {}

        try:
            # Get all collections
            collections = self.db_manager.mongo_db.list_collection_names()

            for collection_name in collections:
                logger.info(f"   Analyzing collection: {collection_name}")

                collection = self.db_manager.mongo_db[collection_name]

                # Sample documents to understand schema
                sample_docs = list(collection.find().limit(100))

                if not sample_docs:
                    mongodb_info[collection_name] = {
                        'fields': {},
                        'document_count': 0,
                        'sample_document': None
                    }
                    continue

                # Analyze field structure
                field_analysis = self._analyze_mongodb_fields(sample_docs)

                mongodb_info[collection_name] = {
                    'fields': field_analysis,
                    'document_count': collection.count_documents({}),
                    'sample_document': sample_docs[0],
                    'total_unique_fields': len(field_analysis)
                }

                logger.info(f"     Found {len(field_analysis)} unique fields")

        except Exception as e:
            logger.error(f"❌ MongoDB schema inspection failed: {e}")
            mongodb_info['error'] = str(e)

        return mongodb_info

    def inspect_cassandra_schema(self) -> Dict[str, Any]:
        """Inspect Cassandra tables and discover all columns and types"""
        logger.info("🏛️ Inspecting Cassandra schema...")

        cassandra_info = {}

        try:
            # Get keyspace name
            keyspace = self.db_manager.config['cassandra']['keyspace']

            # Get all tables in keyspace
            tables_query = """
                SELECT table_name FROM system_schema.tables
                WHERE keyspace_name = %s
            """
            tables_result = self.db_manager.cassandra_session.execute(tables_query, [keyspace])
            tables = [row.table_name for row in tables_result]

            for table_name in tables:
                logger.info(f"   Analyzing table: {table_name}")

                # Get column information
                columns_query = """
                    SELECT column_name, type, kind FROM system_schema.columns
                    WHERE keyspace_name = %s AND table_name = %s
                """
                columns_result = self.db_manager.cassandra_session.execute(
                    columns_query, [keyspace, table_name]
                )

                columns_info = {}
                partition_keys = []
                clustering_keys = []
                regular_columns = []

                for col in columns_result:
                    col_name = col.column_name
                    col_type = col.type
                    col_kind = col.kind

                    # Categorize columns
                    if col_kind == 'partition_key':
                        partition_keys.append(col_name)
                    elif col_kind == 'clustering':
                        clustering_keys.append(col_name)
                    else:
                        regular_columns.append(col_name)

                    # Map Cassandra types to Python types and operators
                    python_type, operators = self._map_cassandra_type(col_type)

                    columns_info[col_name] = {
                        'cassandra_type': col_type,
                        'python_type': python_type,
                        'kind': col_kind,
                        'available_operators': operators,
                        'is_partition_key': col_kind == 'partition_key',
                        'is_clustering_key': col_kind == 'clustering'
                    }

                # Get sample data
                try:
                    sample_query = f"SELECT * FROM {table_name} LIMIT 5"
                    sample_result = self.db_manager.cassandra_session.execute(sample_query)
                    sample_rows = list(sample_result)
                except Exception as e:
                    sample_rows = []
                    logger.warning(f"     Could not get sample data: {e}")

                cassandra_info[table_name] = {
                    'columns': columns_info,
                    'partition_keys': partition_keys,
                    'clustering_keys': clustering_keys,
                    'regular_columns': regular_columns,
                    'total_columns': len(columns_info),
                    'sample_rows': sample_rows[:3] if sample_rows else [],
                    'optimization_hint': f"Best performance: filter by {', '.join(partition_keys)} first" if partition_keys else "No partition key optimization available"
                }

                logger.info(f"     Found {len(columns_info)} columns")

        except Exception as e:
            logger.error(f"❌ Cassandra schema inspection failed: {e}")
            cassandra_info['error'] = str(e)

        return cassandra_info

    def _analyze_mongodb_fields(self, documents: List[Dict]) -> Dict[str, Any]:
        """Analyze MongoDB documents to extract field information"""
        field_info = {}

        for doc in documents:
            self._extract_fields_recursive(doc, field_info, prefix="")

        # Post-process to determine dominant types and operators
        for field_name, type_counts in field_info.items():
            # Find most common type
            dominant_type = max(type_counts, key=type_counts.get)

            # Determine available operators
            operators = self._get_mongodb_operators(dominant_type)

            field_info[field_name] = {
                'dominant_type': dominant_type,
                'type_distribution': type_counts,
                'available_operators': operators,
                'is_nested': '.' in field_name,
                'sample_values': []  # Could add sample values here
            }

        return field_info

    def _extract_fields_recursive(self, obj: Any, field_info: Dict, prefix: str = ""):
        """Recursively extract fields from nested MongoDB documents"""
        if isinstance(obj, dict):
            for key, value in obj.items():
                field_path = f"{prefix}.{key}" if prefix else key

                # Skip MongoDB internal fields
                if key.startswith('_') and key != '_id':
                    continue

                value_type = type(value).__name__

                # Initialize field tracking
                if field_path not in field_info:
                    field_info[field_path] = {}

                # Count type occurrences
                if value_type not in field_info[field_path]:
                    field_info[field_path][value_type] = 0
                field_info[field_path][value_type] += 1

                # Recurse into nested objects (but limit depth)
                if isinstance(value, dict) and prefix.count('.') < 2:  # Limit nesting depth
                    self._extract_fields_recursive(value, field_info, field_path)
                elif isinstance(value, list) and value and prefix.count('.') < 2:
                    # Analyze first element of arrays
                    self._extract_fields_recursive(value[0], field_info, field_path)

    def _get_mongodb_operators(self, python_type: str) -> List[str]:
        """Get appropriate operators for MongoDB field types"""
        operator_map = {
            'str': ['=', '!=', 'contains', 'starts_with', 'ends_with', 'regex'],
            'int': ['=', '!=', '>', '<', '>=', '<=', 'in', 'range'],
            'float': ['=', '!=', '>', '<', '>=', '<=', 'in', 'range'],
            'bool': ['=', '!='],
            'datetime': ['=', '!=', '>', '<', '>=', '<=', 'range'],
            'list': ['contains', 'size', 'all', 'in'],
            'dict': ['exists', 'field_exists'],
            'NoneType': ['exists', 'is_null']
        }

        return operator_map.get(python_type, ['=', '!=', 'exists'])

    def _map_cassandra_type(self, cassandra_type: str) -> tuple:
        """Map Cassandra types to Python types and appropriate operators"""
        type_mapping = {
            'text': ('str', ['=', '!=', '>', '<', '>=', '<=', 'in']),
            'varchar': ('str', ['=', '!=', '>', '<', '>=', '<=', 'in']),
            'int': ('int', ['=', '!=', '>', '<', '>=', '<=', 'in', 'range']),
            'bigint': ('int', ['=', '!=', '>', '<', '>=', '<=', 'in', 'range']),
            'decimal': ('float', ['=', '!=', '>', '<', '>=', '<=', 'in', 'range']),
            'double': ('float', ['=', '!=', '>', '<', '>=', '<=', 'in', 'range']),
            'boolean': ('bool', ['=', '!=']),
            'timestamp': ('datetime', ['=', '!=', '>', '<', '>=', '<=', 'range']),
            'date': ('date', ['=', '!=', '>', '<', '>=', '<=', 'range']),
            'uuid': ('str', ['=', '!=']),
            'timeuuid': ('str', ['=', '!=', '>', '<', '>=', '<=']),
        }

        # Handle complex types
        if cassandra_type.startswith('frozen'):
            return ('dict', ['=', '!=', 'contains'])
        elif cassandra_type.startswith('list'):
            return ('list', ['contains', 'size'])
        elif cassandra_type.startswith('set'):
            return ('set', ['contains', 'size'])
        elif cassandra_type.startswith('map'):
            return ('dict', ['contains_key', 'contains_value'])

        return type_mapping.get(cassandra_type, ('str', ['=', '!=']))

    def find_cross_database_fields(self) -> List[Dict[str, Any]]:
        """Find fields that exist in both MongoDB and Cassandra for cross-database queries"""
        cross_fields = []

        if not self.mongodb_schema or not self.cassandra_schema:
            return cross_fields

        # Extract all MongoDB field names
        mongodb_fields = set()
        for collection, info in self.mongodb_schema.items():
            if 'fields' in info:
                mongodb_fields.update(info['fields'].keys())

        # Extract all Cassandra column names
        cassandra_fields = set()
        for table, info in self.cassandra_schema.items():
            if 'columns' in info:
                cassandra_fields.update(info['columns'].keys())

        # Find common fields
        common_fields = mongodb_fields.intersection(cassandra_fields)

        for field_name in common_fields:
            # Find which collections/tables contain this field
            mongodb_locations = []
            cassandra_locations = []

            for collection, info in self.mongodb_schema.items():
                if 'fields' in info and field_name in info['fields']:
                    mongodb_locations.append({
                        'collection': collection,
                        'field_info': info['fields'][field_name]
                    })

            for table, info in self.cassandra_schema.items():
                if 'columns' in info and field_name in info['columns']:
                    cassandra_locations.append({
                        'table': table,
                        'column_info': info['columns'][field_name]
                    })

            cross_fields.append({
                'field_name': field_name,
                'mongodb_locations': mongodb_locations,
                'cassandra_locations': cassandra_locations,
                'join_potential': 'high' if any(loc['column_info'].get('is_partition_key') for loc in cassandra_locations) else 'medium'
            })

        return cross_fields

    def get_field_suggestions(self, database: str, collection_or_table: str = None) -> Dict[str, Any]:
        """Get field suggestions for building queries"""
        suggestions = {
            'available_fields': [],
            'recommended_filters': [],
            'optimization_tips': []
        }

        if database == 'mongodb' and self.mongodb_schema:
            if collection_or_table and collection_or_table in self.mongodb_schema:
                collection_info = self.mongodb_schema[collection_or_table]
                if 'fields' in collection_info:
                    for field_name, field_info in collection_info['fields'].items():
                        suggestions['available_fields'].append({
                            'name': field_name,
                            'type': field_info.get('dominant_type', 'unknown'),
                            'operators': field_info.get('available_operators', []),
                            'is_nested': field_info.get('is_nested', False)
                        })
            else:
                # Return all fields from all collections
                for collection, info in self.mongodb_schema.items():
                    if 'fields' in info:
                        for field_name, field_info in info['fields'].items():
                            suggestions['available_fields'].append({
                                'name': field_name,
                                'type': field_info.get('dominant_type', 'unknown'),
                                'operators': field_info.get('available_operators', []),
                                'collection': collection,
                                'is_nested': field_info.get('is_nested', False)
                            })

        elif database == 'cassandra' and self.cassandra_schema:
            if collection_or_table and collection_or_table in self.cassandra_schema:
                table_info = self.cassandra_schema[collection_or_table]
                if 'columns' in table_info:
                    for column_name, column_info in table_info['columns'].items():
                        suggestions['available_fields'].append({
                            'name': column_name,
                            'type': column_info.get('python_type', 'unknown'),
                            'operators': column_info.get('available_operators', []),
                            'is_partition_key': column_info.get('is_partition_key', False),
                            'is_clustering_key': column_info.get('is_clustering_key', False)
                        })

                    # Add optimization tips
                    if table_info.get('partition_keys'):
                        suggestions['optimization_tips'].append(
                            f"For best performance, filter by partition key(s): {', '.join(table_info['partition_keys'])}"
                        )

        return suggestions

    def validate_field_access(self, database: str, collection_or_table: str, field_name: str) -> Dict[str, Any]:
        """Validate if a field can be accessed and suggest optimal access patterns"""
        validation = {
            'exists': False,
            'accessible': False,
            'field_info': {},
            'optimization_suggestions': [],
            'warnings': []
        }

        if database == 'mongodb' and self.mongodb_schema:
            if collection_or_table in self.mongodb_schema:
                collection_info = self.mongodb_schema[collection_or_table]
                if 'fields' in collection_info and field_name in collection_info['fields']:
                    validation['exists'] = True
                    validation['accessible'] = True
                    validation['field_info'] = collection_info['fields'][field_name]

                    # Add MongoDB-specific suggestions
                    if field_name in ['_id', 'employee_id', 'menu_id']:
                        validation['optimization_suggestions'].append("This field likely has an index - good for filtering")

        elif database == 'cassandra' and self.cassandra_schema:
            if collection_or_table in self.cassandra_schema:
                table_info = self.cassandra_schema[collection_or_table]
                if 'columns' in table_info and field_name in table_info['columns']:
                    validation['exists'] = True
                    column_info = table_info['columns'][field_name]
                    validation['field_info'] = column_info

                    # Check accessibility based on Cassandra rules
                    if column_info.get('is_partition_key'):
                        validation['accessible'] = True
                        validation['optimization_suggestions'].append("Partition key - excellent for filtering (no ALLOW FILTERING needed)")
                    elif column_info.get('is_clustering_key'):
                        validation['accessible'] = True
                        validation['optimization_suggestions'].append("Clustering key - good for filtering within partitions")
                    else:
                        validation['accessible'] = True
                        validation['warnings'].append("Regular column - may require ALLOW FILTERING (slower)")

        return validation


# Example usage and testing
if __name__ == "__main__":
    from .database_manager import DatabaseManager

    # Test schema inspection
    db_manager = DatabaseManager()

    if db_manager.connect_all():
        inspector = SchemaInspector(db_manager)

        print("🔍 Starting schema inspection...")
        schema_info = inspector.inspect_all_schemas()

        print("\n📊 MongoDB Schema:")
        for collection, info in schema_info['mongodb'].items():
            if isinstance(info, dict) and 'fields' in info:
                print(f"   {collection}: {info['total_unique_fields']} fields")
                # Show first few fields
                for field_name in list(info['fields'].keys())[:5]:
                    field_info = info['fields'][field_name]
                    print(f"     - {field_name}: {field_info.get('dominant_type', 'unknown')}")

        print("\n🏛️ Cassandra Schema:")
        for table, info in schema_info['cassandra'].items():
            if isinstance(info, dict) and 'columns' in info:
                print(f"   {table}: {info['total_columns']} columns")
                if info.get('partition_keys'):
                    print(f"     Partition keys: {', '.join(info['partition_keys'])}")

        print(f"\n🔗 Cross-database fields: {len(schema_info['cross_database_fields'])}")
        for field in schema_info['cross_database_fields'][:3]:
            print(f"   - {field['field_name']} (join potential: {field['join_potential']})")

    db_manager.close_all_connections()
