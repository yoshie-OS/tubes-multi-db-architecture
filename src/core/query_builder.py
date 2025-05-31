# src/core/query_builder.py
from typing import Dict, List, Any, Optional, Union, Tuple
import logging
from datetime import datetime, date
from decimal import Decimal
import re
from .database_manager import DatabaseManager
from .schema_inspector import SchemaInspector

logger = logging.getLogger(__name__)

class QueryFilter:
    """Represents a single filter condition"""
    def __init__(self, field: str, operator: str, value: Any, field_type: str = 'str'):
        self.field = field
        self.operator = operator
        self.value = value
        self.field_type = field_type

    def __repr__(self):
        return f"QueryFilter({self.field} {self.operator} {self.value})"

class QueryBuilder:
    """
    Dynamic query builder that constructs MongoDB and Cassandra queries
    based on user field selections and optimization preferences.
    Enhanced with flexible field selection for handling curveball questions.
    """

    def __init__(self, db_manager: DatabaseManager, schema_inspector: SchemaInspector):
        self.db_manager = db_manager
        self.schema_inspector = schema_inspector

        # Operator mappings for different databases
        self.mongodb_operators = {
            '=': lambda field, value: {field: value},
            '!=': lambda field, value: {field: {'$ne': value}},
            '>': lambda field, value: {field: {'$gt': value}},
            '<': lambda field, value: {field: {'$lt': value}},
            '>=': lambda field, value: {field: {'$gte': value}},
            '<=': lambda field, value: {field: {'$lte': value}},
            'contains': lambda field, value: {field: {'$regex': str(value), '$options': 'i'}},
            'starts_with': lambda field, value: {field: {'$regex': f'^{str(value)}', '$options': 'i'}},
            'ends_with': lambda field, value: {field: {'$regex': f'{str(value)}$', '$options': 'i'}},
            'in': lambda field, value: {field: {'$in': value if isinstance(value, list) else [value]}},
            'range': lambda field, value: {field: {'$gte': value[0], '$lte': value[1]}},
            'exists': lambda field, value: {field: {'$exists': bool(value)}},
            'regex': lambda field, value: {field: {'$regex': str(value), '$options': 'i'}}
        }

        self.cassandra_operators = {
            '=': lambda field, value: f"{field} = %s",
            '!=': lambda field, value: f"{field} != %s",
            '>': lambda field, value: f"{field} > %s",
            '<': lambda field, value: f"{field} < %s",
            '>=': lambda field, value: f"{field} >= %s",
            '<=': lambda field, value: f"{field} <= %s",
            'in': lambda field, value: f"{field} IN ({', '.join(['%s' for _ in (value if isinstance(value, list) else [value])])})"
        }

    def find_best_table_for_field(self, field_name: str, database: str) -> Optional[str]:
        """Smart table selection - find the best table/collection for a given field"""

        if database == 'mongodb':
            mongodb_schema = self.schema_inspector.mongodb_schema
            for collection, info in mongodb_schema.items():
                if 'fields' in info and field_name in info['fields']:
                    return collection

        elif database == 'cassandra':
            cassandra_schema = self.schema_inspector.cassandra_schema

            # Priority order: partition key tables first, then main table
            table_priority = []
            main_table = None

            for table, info in cassandra_schema.items():
                if 'columns' in info and field_name in info['columns']:
                    column_info = info['columns'][field_name]

                    if column_info.get('is_partition_key'):
                        # Partition key - highest priority
                        table_priority.insert(0, table)
                    elif table == 'transactions':
                        # Main table - lowest priority
                        main_table = table
                    else:
                        # Regular optimized table - medium priority
                        table_priority.append(table)

            # Add main table at the end if field exists there
            if main_table:
                table_priority.append(main_table)

            return table_priority[0] if table_priority else None

        return None

    def get_all_available_fields(self, database: str) -> Dict[str, List[str]]:
        """Get all available fields organized by table/collection"""

        available_fields = {}

        if database == 'mongodb':
            mongodb_schema = self.schema_inspector.mongodb_schema
            for collection, info in mongodb_schema.items():
                if 'fields' in info:
                    available_fields[collection] = list(info['fields'].keys())

        elif database == 'cassandra':
            cassandra_schema = self.schema_inspector.cassandra_schema
            for table, info in cassandra_schema.items():
                if 'columns' in info:
                    available_fields[table] = list(info['columns'].keys())

        return available_fields

    def build_smart_query(self, database: str, user_fields: List[str],
                         filters: List[QueryFilter], use_optimization: bool = True) -> Dict[str, Any]:
        """Smart query builder that automatically selects best table/collection"""

        if database == 'mongodb':
            # Find best collection for the main field
            main_field = user_fields[0] if user_fields else (filters[0].field if filters else 'employee_id')
            collection = self.find_best_table_for_field(main_field, 'mongodb')

            if not collection:
                # Fallback to first available collection
                collections = list(self.schema_inspector.mongodb_schema.keys())
                collection = collections[0] if collections else 'employees'

            return self.build_mongodb_query(
                collection=collection,
                filters=filters,
                use_optimization=use_optimization
            )

        elif database == 'cassandra':
            # Find best table for the main field
            main_field = user_fields[0] if user_fields else (filters[0].field if filters else 'employee_id')
            table = self.find_best_table_for_field(main_field, 'cassandra')

            if not table:
                # Fallback to main transactions table
                table = 'transactions'

            return self.build_cassandra_query(
                table=table,
                filters=filters,
                use_optimization=use_optimization
            )

    def build_mongodb_query(self, collection: str, filters: List[QueryFilter],
                           sort_field: str = None, sort_order: int = 1,
                           limit: int = None, use_optimization: bool = True) -> Dict[str, Any]:
        """Build MongoDB query with filters, sorting, and optimization control"""

        query_config = {
            'database': 'mongodb',
            'collection': collection,
            'filters': {},
            'sort': None,
            'limit': limit or 5,  # Default to top 5 results
            'use_optimization': use_optimization,
            'query_explanation': []
        }

        # Build filter conditions
        for filter_obj in filters:
            if filter_obj.operator in self.mongodb_operators:
                filter_condition = self.mongodb_operators[filter_obj.operator](
                    filter_obj.field, filter_obj.value
                )
                query_config['filters'].update(filter_condition)
                query_config['query_explanation'].append(
                    f"Filter: {filter_obj.field} {filter_obj.operator} {filter_obj.value}"
                )
            else:
                logger.warning(f"Unsupported MongoDB operator: {filter_obj.operator}")

        # Add sorting
        if sort_field:
            query_config['sort'] = {'field': sort_field, 'order': sort_order}
            query_config['query_explanation'].append(
                f"Sort: {sort_field} {'ascending' if sort_order == 1 else 'descending'}"
            )

        # Add optimization explanation
        if use_optimization:
            query_config['query_explanation'].append("Optimization: Using available indexes")
        else:
            query_config['query_explanation'].append("Optimization: Forcing collection scan (hint: $natural)")

        return query_config

    def build_cassandra_query(self, table: str, filters: List[QueryFilter],
                             use_optimization: bool = True) -> Dict[str, Any]:
        """Build Cassandra query with automatic table optimization"""

        # Get table schema for optimization decisions
        table_schema = self.schema_inspector.cassandra_schema.get(table, {})
        partition_keys = table_schema.get('partition_keys', [])
        clustering_keys = table_schema.get('clustering_keys', [])

        query_config = {
            'database': 'cassandra',
            'table': table,
            'filters': {},
            'use_optimization': use_optimization,
            'query_explanation': [],
            'optimization_strategy': 'unknown'
        }

        # Analyze filters for optimization potential
        partition_key_filters = []
        clustering_key_filters = []
        regular_filters = []

        for filter_obj in filters:
            if filter_obj.field in partition_keys:
                partition_key_filters.append(filter_obj)
            elif filter_obj.field in clustering_keys:
                clustering_key_filters.append(filter_obj)
            else:
                regular_filters.append(filter_obj)

        # Smart optimization strategy
        if use_optimization and partition_key_filters:
            query_config['optimization_strategy'] = 'partition_key_optimized'
            query_config['query_explanation'].append(
                f"Optimization: Using partition key(s) {[f.field for f in partition_key_filters]} for direct node access"
            )
        elif partition_key_filters and not use_optimization:
            # Force non-optimized by using main table
            if table != 'transactions':
                query_config['table'] = 'transactions'
                query_config['optimization_strategy'] = 'forced_main_table'
                query_config['query_explanation'].append(
                    "Optimization: Forced to use main transactions table with ALLOW FILTERING"
                )
        else:
            query_config['optimization_strategy'] = 'allow_filtering'
            query_config['query_explanation'].append(
                "Optimization: Using ALLOW FILTERING (flexible but slower)"
            )

        # Build WHERE clause
        where_conditions = []
        params = {}

        all_filters = partition_key_filters + clustering_key_filters + regular_filters
        for filter_obj in all_filters:
            if filter_obj.operator in self.cassandra_operators:
                condition = self.cassandra_operators[filter_obj.operator](
                    filter_obj.field, filter_obj.value
                )
                where_conditions.append(condition)

                # Handle parameter values properly
                if filter_obj.operator == 'in':
                    values = filter_obj.value if isinstance(filter_obj.value, list) else [filter_obj.value]
                    params[f'{filter_obj.field}_values'] = values
                else:
                    params[filter_obj.field] = self._convert_value_for_cassandra(filter_obj.value)

                query_config['query_explanation'].append(
                    f"Filter: {filter_obj.field} {filter_obj.operator} {filter_obj.value}"
                )

        query_config['where_clause'] = ' AND '.join(where_conditions) if where_conditions else None
        query_config['params'] = params
        query_config['filters'] = {f.field: f.value for f in filters}

        return query_config

    def build_cross_database_query(self, mongodb_config: Dict[str, Any],
                                  cassandra_config: Dict[str, Any],
                                  join_field: str,
                                  result_limit: int = None) -> Dict[str, Any]:
        """Build cross-database query that joins MongoDB and Cassandra data"""

        cross_query_config = {
            'database': 'combined',
            'mongodb': mongodb_config,
            'cassandra': cassandra_config,
            'join_field': join_field,
            'limit': result_limit or 5,  # Default to top 5 results
            'use_optimization': True,
            'query_explanation': []
        }

        # Add cross-database explanation
        mongodb_collection = mongodb_config.get('collection', 'unknown')
        cassandra_table = cassandra_config.get('table', 'unknown')

        cross_query_config['query_explanation'].extend([
            f"Cross-database query joining on field: {join_field}",
            f"Step 1: Query MongoDB {mongodb_collection} collection",
            f"Step 2: Use results to query Cassandra {cassandra_table} table",
            f"Step 3: Combine results with aggregation"
        ])

        # Inherit optimization settings
        cross_query_config['mongodb']['use_optimization'] = mongodb_config.get('use_optimization', True)
        cross_query_config['cassandra']['use_optimization'] = cassandra_config.get('use_optimization', True)

        return cross_query_config

    def build_flexible_query(self, query_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Ultimate flexible query builder for handling any curveball questions"""

        database = query_spec.get('database', 'mongodb')
        user_fields = query_spec.get('fields', [])
        use_optimization = query_spec.get('use_optimization', True)

        # Parse filters
        filters = []
        for filter_spec in query_spec.get('filters', []):
            filter_obj = QueryFilter(
                field=filter_spec['field'],
                operator=filter_spec.get('operator', '='),
                value=filter_spec['value'],
                field_type=filter_spec.get('field_type', 'str')
            )
            filters.append(filter_obj)

        # Use smart query building
        return self.build_smart_query(
            database=database,
            user_fields=user_fields,
            filters=filters,
            use_optimization=use_optimization
        )

    def build_query_from_user_input(self, query_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Build query from user specification with intelligent defaults"""

        query_type = query_spec.get('type', 'single')  # single, cross
        use_optimization = query_spec.get('use_optimization', True)

        if query_type == 'single':
            database = query_spec.get('database')  # mongodb or cassandra
            target = query_spec.get('target')  # collection name or table name

            # Parse filters
            filters = []
            for filter_spec in query_spec.get('filters', []):
                filter_obj = QueryFilter(
                    field=filter_spec['field'],
                    operator=filter_spec['operator'],
                    value=filter_spec['value'],
                    field_type=filter_spec.get('field_type', 'str')
                )
                filters.append(filter_obj)

            if database == 'mongodb':
                # Use smart table selection if target not specified
                if not target:
                    user_fields = [f['field'] for f in query_spec.get('filters', [])]
                    return self.build_smart_query('mongodb', user_fields, filters, use_optimization)

                return self.build_mongodb_query(
                    collection=target,
                    filters=filters,
                    sort_field=query_spec.get('sort_field'),
                    sort_order=query_spec.get('sort_order', 1),
                    limit=query_spec.get('limit', 5),
                    use_optimization=use_optimization
                )
            elif database == 'cassandra':
                # Use smart table selection if target not specified
                if not target:
                    user_fields = [f['field'] for f in query_spec.get('filters', [])]
                    return self.build_smart_query('cassandra', user_fields, filters, use_optimization)

                return self.build_cassandra_query(
                    table=target,
                    filters=filters,
                    use_optimization=use_optimization
                )

        elif query_type == 'cross':
            # Build MongoDB part
            mongodb_spec = query_spec.get('mongodb', {})
            mongodb_filters = []
            for filter_spec in mongodb_spec.get('filters', []):
                filter_obj = QueryFilter(
                    field=filter_spec['field'],
                    operator=filter_spec['operator'],
                    value=filter_spec['value']
                )
                mongodb_filters.append(filter_obj)

            # Smart collection selection for MongoDB
            mongodb_collection = mongodb_spec.get('collection')
            if not mongodb_collection:
                user_fields = [f['field'] for f in mongodb_spec.get('filters', [])]
                mongodb_config = self.build_smart_query('mongodb', user_fields, mongodb_filters, use_optimization)
            else:
                mongodb_config = self.build_mongodb_query(
                    collection=mongodb_collection,
                    filters=mongodb_filters,
                    use_optimization=use_optimization
                )

            # Build Cassandra part
            cassandra_spec = query_spec.get('cassandra', {})
            cassandra_filters = []
            for filter_spec in cassandra_spec.get('filters', []):
                filter_obj = QueryFilter(
                    field=filter_spec['field'],
                    operator=filter_spec['operator'],
                    value=filter_spec['value']
                )
                cassandra_filters.append(filter_obj)

            # Smart table selection for Cassandra
            cassandra_table = cassandra_spec.get('table')
            if not cassandra_table:
                user_fields = [f['field'] for f in cassandra_spec.get('filters', [])]
                cassandra_config = self.build_smart_query('cassandra', user_fields, cassandra_filters, use_optimization)
            else:
                cassandra_config = self.build_cassandra_query(
                    table=cassandra_table,
                    filters=cassandra_filters,
                    use_optimization=use_optimization
                )

            return self.build_cross_database_query(
                mongodb_config=mongodb_config,
                cassandra_config=cassandra_config,
                join_field=query_spec.get('join_field', 'employee_id'),
                result_limit=query_spec.get('limit', 5)
            )

        raise ValueError(f"Unsupported query type: {query_type}")

    def _convert_value_for_cassandra(self, value: Any) -> Any:
        """Convert Python values to Cassandra-compatible types"""
        if isinstance(value, str):
            return value
        elif isinstance(value, (int, float)):
            return value
        elif isinstance(value, bool):
            return value
        elif isinstance(value, datetime):
            return value
        elif isinstance(value, date):
            return value
        elif isinstance(value, list):
            return value
        else:
            return str(value)

    def get_query_optimization_suggestions(self, query_config: Dict[str, Any]) -> List[str]:
        """Analyze query and provide optimization suggestions"""
        suggestions = []

        if query_config['database'] == 'mongodb':
            collection = query_config['collection']
            filters = query_config.get('filters', {})

            # Check for indexed fields
            if 'employee_id' in filters:
                suggestions.append("✅ employee_id is likely indexed - good performance expected")
            if '_id' in filters:
                suggestions.append("✅ _id has primary index - excellent performance")
            if len(filters) > 3:
                suggestions.append("⚠️ Many filters may slow query - consider adding compound index")

            # Check sort field
            sort_info = query_config.get('sort', {})
            if sort_info and sort_info['field'] not in filters:
                suggestions.append("⚠️ Sorting on unfiltered field may require full collection scan")

        elif query_config['database'] == 'cassandra':
            table = query_config['table']
            table_schema = self.schema_inspector.cassandra_schema.get(table, {})
            partition_keys = table_schema.get('partition_keys', [])
            filters = query_config.get('filters', {})

            # Check partition key usage
            partition_key_used = any(pk in filters for pk in partition_keys)
            if partition_key_used:
                suggestions.append("✅ Using partition key - excellent performance (no cluster scan)")
            else:
                suggestions.append("⚠️ No partition key filter - will scan all nodes (slower)")

            # Check for ALLOW FILTERING
            if query_config.get('optimization_strategy') == 'allow_filtering':
                suggestions.append("⚠️ Using ALLOW FILTERING - acceptable for small datasets but avoid in production")

            # Check table choice
            if table.startswith('transactions_by_'):
                suggestions.append("✅ Using optimized denormalized table - good choice")

        elif query_config['database'] == 'combined':
            suggestions.extend([
                "🔗 Cross-database query detected",
                "💡 Performance depends on result set sizes from both databases",
                "💡 Ensure join field has good selectivity"
            ])

            # Analyze sub-queries
            mongo_suggestions = self.get_query_optimization_suggestions(query_config['mongodb'])
            cassandra_suggestions = self.get_query_optimization_suggestions(query_config['cassandra'])

            suggestions.extend([f"MongoDB: {s}" for s in mongo_suggestions])
            suggestions.extend([f"Cassandra: {s}" for s in cassandra_suggestions])

        return suggestions

    def validate_query_config(self, query_config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate query configuration and suggest fixes"""
        validation = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'suggestions': []
        }

        try:
            if query_config['database'] == 'mongodb':
                collection = query_config['collection']
                if collection not in self.schema_inspector.mongodb_schema:
                    validation['errors'].append(f"Collection '{collection}' not found")
                    validation['is_valid'] = False
                else:
                    # Validate fields exist
                    collection_fields = self.schema_inspector.mongodb_schema[collection].get('fields', {})
                    for field in query_config.get('filters', {}):
                        if field not in collection_fields:
                            validation['warnings'].append(f"Field '{field}' not found in schema - may not exist")

            elif query_config['database'] == 'cassandra':
                table = query_config['table']
                if table not in self.schema_inspector.cassandra_schema:
                    validation['errors'].append(f"Table '{table}' not found")
                    validation['is_valid'] = False
                else:
                    # Validate columns exist
                    table_columns = self.schema_inspector.cassandra_schema[table].get('columns', {})
                    for field in query_config.get('filters', {}):
                        if field not in table_columns:
                            validation['warnings'].append(f"Column '{field}' not found in schema")

            elif query_config['database'] == 'combined':
                # Validate both sub-queries
                mongo_validation = self.validate_query_config(query_config['mongodb'])
                cassandra_validation = self.validate_query_config(query_config['cassandra'])

                if not mongo_validation['is_valid'] or not cassandra_validation['is_valid']:
                    validation['is_valid'] = False
                    validation['errors'].extend(mongo_validation.get('errors', []))
                    validation['errors'].extend(cassandra_validation.get('errors', []))

            # Add optimization suggestions
            validation['suggestions'] = self.get_query_optimization_suggestions(query_config)

        except Exception as e:
            validation['is_valid'] = False
            validation['errors'].append(f"Query validation error: {str(e)}")

        return validation


# Helper function for creating filters easily
def create_filter(field: str, operator: str, value: Any, field_type: str = 'str') -> QueryFilter:
    """Convenience function for creating QueryFilter objects"""
    return QueryFilter(field, operator, value, field_type)


# Example usage and testing
if __name__ == "__main__":
    from .database_manager import DatabaseManager
    from .schema_inspector import SchemaInspector

    # Test query builder
    db_manager = DatabaseManager()

    if db_manager.connect_all():
        inspector = SchemaInspector(db_manager)
        inspector.inspect_all_schemas()

        builder = QueryBuilder(db_manager, inspector)

        print("🔧 Testing Dynamic Query Builder...")

        # Test MongoDB query
        print("\n🍃 MongoDB Query Test:")
        mongo_filters = [
            create_filter('position', '=', 'Barista'),
            create_filter('performance_rating', '>', 4.0, 'float')
        ]
        mongo_query = builder.build_mongodb_query(
            collection='employees',
            filters=mongo_filters,
            sort_field='performance_rating',
            sort_order=-1,
            limit=5
        )
        print(f"   Query: {mongo_query}")

        # Test Cassandra query
        print("\n🏛️ Cassandra Query Test:")
        cassandra_filters = [
            create_filter('employee_id', '=', 1, 'int'),
            create_filter('total_amount', '>', 50000, 'float')
        ]
        cassandra_query = builder.build_cassandra_query(
            table='transactions_by_employee',
            filters=cassandra_filters
        )
        print(f"   Query: {cassandra_query}")

        # Test cross-database query
        print("\n🔗 Cross-Database Query Test:")
        cross_query = builder.build_cross_database_query(
            mongodb_config=mongo_query,
            cassandra_config=cassandra_query,
            join_field='employee_id'
        )
        print(f"   Query explanation: {cross_query['query_explanation']}")

        # Test optimization suggestions
        print("\n💡 Optimization Suggestions:")
        suggestions = builder.get_query_optimization_suggestions(mongo_query)
        for suggestion in suggestions:
            print(f"   {suggestion}")

    db_manager.close_all_connections()
