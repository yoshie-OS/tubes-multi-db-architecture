# src/core/performance_analyzer.py
import time
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, date
from decimal import Decimal
from dataclasses import dataclass
from .database_manager import DatabaseManager
from .schema_inspector import SchemaInspector

logger = logging.getLogger(__name__)

@dataclass
class QueryResult:
    """Container for query execution results"""
    success: bool
    execution_time_ms: float
    result_count: int
    results: List[Dict[str, Any]]
    error_message: Optional[str] = None
    query_explanation: List[str] = None
    optimization_used: bool = True
    database_specific_info: Dict[str, Any] = None

@dataclass
class PerformanceComparison:
    """Container for performance comparison results"""
    optimized_result: QueryResult
    unoptimized_result: QueryResult
    performance_improvement: Dict[str, float]
    winner: str
    analysis: List[str]
    recommendations: List[str]

class PerformanceAnalyzer:
    """
    Executes queries with performance measurement and provides detailed
    optimization analysis for educational demonstrations.
    """

    def __init__(self, db_manager: DatabaseManager, schema_inspector: SchemaInspector):
        self.db_manager = db_manager
        self.schema_inspector = schema_inspector

    def execute_query_with_timing(self, query_config: Dict[str, Any]) -> QueryResult:
        """Execute a single query with precise timing measurement"""

        start_time = time.perf_counter()

        try:
            if query_config['database'] == 'mongodb':
                result = self._execute_mongodb_query(query_config)
            elif query_config['database'] == 'cassandra':
                result = self._execute_cassandra_query(query_config)
            elif query_config['database'] == 'combined':
                result = self._execute_combined_query(query_config)
            else:
                raise ValueError(f"Unsupported database: {query_config['database']}")

            end_time = time.perf_counter()
            execution_time = (end_time - start_time) * 1000  # Convert to milliseconds

            return QueryResult(
                success=True,
                execution_time_ms=round(execution_time, 3),
                result_count=len(result['results']),
                results=result['results'],
                query_explanation=query_config.get('query_explanation', []),
                optimization_used=query_config.get('use_optimization', True),
                database_specific_info=result.get('database_info', {})
            )

        except Exception as e:
            end_time = time.perf_counter()
            execution_time = (end_time - start_time) * 1000

            logger.error(f"Query execution failed: {e}")
            return QueryResult(
                success=False,
                execution_time_ms=round(execution_time, 3),
                result_count=0,
                results=[],
                error_message=str(e),
                query_explanation=query_config.get('query_explanation', []),
                optimization_used=query_config.get('use_optimization', True)
            )

    def compare_optimization_scenarios(self, base_query_config: Dict[str, Any]) -> PerformanceComparison:
        """Compare the same query with and without optimization"""

        logger.info("🔄 Starting optimization performance comparison...")

        # Create optimized version
        optimized_config = base_query_config.copy()
        optimized_config['use_optimization'] = True

        # Create non-optimized version
        unoptimized_config = base_query_config.copy()
        unoptimized_config['use_optimization'] = False

        # Execute both versions
        logger.info("   Executing optimized query...")
        optimized_result = self.execute_query_with_timing(optimized_config)

        logger.info("   Executing non-optimized query...")
        unoptimized_result = self.execute_query_with_timing(unoptimized_config)

        # Calculate performance metrics
        performance_improvement = self._calculate_performance_metrics(
            optimized_result, unoptimized_result
        )

        # Determine winner and generate analysis
        winner = self._determine_performance_winner(optimized_result, unoptimized_result)
        analysis = self._generate_performance_analysis(
            optimized_result, unoptimized_result, base_query_config
        )
        recommendations = self._generate_optimization_recommendations(
            optimized_result, unoptimized_result, base_query_config
        )

        logger.info("✅ Performance comparison complete")

        return PerformanceComparison(
            optimized_result=optimized_result,
            unoptimized_result=unoptimized_result,
            performance_improvement=performance_improvement,
            winner=winner,
            analysis=analysis,
            recommendations=recommendations
        )

    def _execute_mongodb_query(self, query_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute MongoDB query with optimization control"""

        collection_name = query_config['collection']
        filters = query_config.get('filters', {})
        use_optimization = query_config.get('use_optimization', True)

        collection = getattr(self.db_manager.mongo_db, collection_name)

        # Build query
        query = collection.find(filters)

        # Apply optimization choice
        if not use_optimization:
            # Force collection scan by hinting natural order
            query = query.hint({'$natural': 1})

        # Apply sorting if specified
        sort_info = query_config.get('sort')
        if sort_info:
            sort_field = sort_info['field']
            sort_order = sort_info['order']
            query = query.sort(sort_field, sort_order)

        # Apply limit
        limit = query_config.get('limit')
        if limit:
            query = query.limit(limit)

        # Execute and convert results
        results = list(query)

        # Convert ObjectId and other non-serializable types
        serializable_results = []
        for result in results:
            serializable_result = self._make_serializable(result)
            serializable_results.append(serializable_result)

        return {
            'results': serializable_results,
            'database_info': {
                'collection': collection_name,
                'filter_count': len(filters),
                'optimization_hint': 'index_scan' if use_optimization else 'collection_scan'
            }
        }

    def _execute_cassandra_query(self, query_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute Cassandra query with proper ALLOW FILTERING logic"""

        table_name = query_config['table']
        use_optimization = query_config.get('use_optimization', True)
        where_clause = query_config.get('where_clause')
        params = query_config.get('params', {})

        # Build the actual CQL query
        base_query = f"SELECT * FROM {table_name}"

        if where_clause:
            full_query = f"{base_query} WHERE {where_clause}"

            # FIXED LOGIC: Add ALLOW FILTERING based on table structure and optimization setting
            table_schema = self.schema_inspector.cassandra_schema.get(table_name, {})
            partition_keys = table_schema.get('partition_keys', [])

            # Check if we're filtering by partition key
            filters = query_config.get('filters', {})
            has_partition_key_filter = any(pk in filters for pk in partition_keys)

            # Add ALLOW FILTERING if:
            # 1. Not using optimization, OR
            # 2. Using main transactions table, OR
            # 3. Not filtering by partition key
            needs_allow_filtering = (
                not use_optimization or
                table_name == 'transactions' or
                not has_partition_key_filter
            )

            if needs_allow_filtering:
                full_query += " ALLOW FILTERING"

        else:
            full_query = base_query
            if not use_optimization:
                full_query += " ALLOW FILTERING"

        # Debug: Log the actual query being executed
        logger.info(f"   Executing CQL: {full_query}")
        logger.info(f"   With params: {params}")
        logger.info(f"   Table: {table_name}, Optimization: {use_optimization}")
        logger.info(f"   Partition keys: {table_schema.get('partition_keys', [])}")
        logger.info(f"   Has partition key filter: {has_partition_key_filter if 'has_partition_key_filter' in locals() else 'unknown'}")

        try:
            # Execute query with proper parameter handling for Cassandra driver
            if isinstance(params, dict):
                # Convert dict params to positional params for CQL
                if params:
                    param_values = list(params.values())
                    result = self.db_manager.cassandra_session.execute(full_query, param_values)
                else:
                    result = self.db_manager.cassandra_session.execute(full_query)
            elif isinstance(params, list):
                result = self.db_manager.cassandra_session.execute(full_query, params)
            else:
                result = self.db_manager.cassandra_session.execute(full_query)

            rows = list(result)

            # Convert to serializable format
            serializable_results = []
            for row in rows:
                row_dict = {}
                for column, value in zip(row._fields, row):
                    row_dict[column] = self._convert_cassandra_value(value)
                serializable_results.append(row_dict)

            return {
                'results': serializable_results,
                'database_info': {
                    'table': table_name,
                    'query': full_query,
                    'optimization_strategy': query_config.get('optimization_strategy', 'unknown'),
                    'used_allow_filtering': 'ALLOW FILTERING' in full_query,
                    'partition_key_filter': has_partition_key_filter if 'has_partition_key_filter' in locals() else False
                }
            }
        except Exception as e:
            logger.error(f"   CQL execution failed: {e}")
            logger.error(f"   Query was: {full_query}")
            logger.error(f"   Params were: {params}")
            raise

    def _execute_combined_query(self, query_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute cross-database query with join logic"""

        # Step 1: Execute MongoDB query
        mongodb_config = query_config['mongodb']
        mongodb_result = self._execute_mongodb_query(mongodb_config)

        if not mongodb_result['results']:
            return {'results': [], 'database_info': {'join_field': query_config.get('join_field'), 'mongodb_results': 0}}

        # Extract join values
        join_field = query_config.get('join_field', 'employee_id')
        join_values = []
        for mongo_doc in mongodb_result['results']:
            if join_field in mongo_doc:
                join_values.append(mongo_doc[join_field])

        if not join_values:
            return {'results': [], 'database_info': {'join_field': join_field, 'no_join_values': True}}

        # Step 2: Execute Cassandra query with join filter
        cassandra_config = query_config['cassandra'].copy()

        # For cross-database join, use the optimized table for employee queries
        if join_values and len(join_values) > 0:
            # Use the first employee for demonstration
            first_employee_id = join_values[0]

            # Update config to use the optimized table
            cassandra_config['table'] = 'transactions_by_employee'
            cassandra_config['filters'] = {join_field: first_employee_id}
            cassandra_config['where_clause'] = f"{join_field} = %s"
            cassandra_config['params'] = [first_employee_id]

            logger.info(f"   Cross-database query using transactions_by_employee for employee_id: {first_employee_id}")

        cassandra_result = self._execute_cassandra_query(cassandra_config)

        # Step 3: Join results
        combined_results = self._join_results(
            mongodb_result['results'],
            cassandra_result['results'],
            join_field
        )

        return {
            'results': combined_results,
            'database_info': {
                'join_field': join_field,
                'mongodb_results': len(mongodb_result['results']),
                'cassandra_results': len(cassandra_result['results']),
                'combined_results': len(combined_results)
            }
        }

    def _join_results(self, mongodb_results: List[Dict], cassandra_results: List[Dict],
                     join_field: str) -> List[Dict]:
        """Join MongoDB and Cassandra results on specified field"""

        # Create lookup for Cassandra results
        cassandra_lookup = {}
        for cass_record in cassandra_results:
            key = cass_record.get(join_field)
            if key not in cassandra_lookup:
                cassandra_lookup[key] = []
            cassandra_lookup[key].append(cass_record)

        # Combine results
        combined_results = []
        for mongo_record in mongodb_results:
            join_key = mongo_record.get(join_field)
            if join_key in cassandra_lookup:
                # Aggregate Cassandra data for this MongoDB record
                cassandra_records = cassandra_lookup[join_key]

                transaction_count = len(cassandra_records)
                total_amount = sum(record.get('total_amount', 0) for record in cassandra_records)
                avg_amount = total_amount / transaction_count if transaction_count > 0 else 0

                # Create combined record
                combined_record = mongo_record.copy()
                combined_record.update({
                    'transaction_count': transaction_count,
                    'total_transaction_amount': total_amount,
                    'avg_transaction_amount': avg_amount,
                    'latest_transactions': cassandra_records[:5]  # Include sample transactions
                })

                combined_results.append(combined_record)

        return combined_results

    def _calculate_performance_metrics(self, optimized: QueryResult,
                                     unoptimized: QueryResult) -> Dict[str, float]:
        """Calculate detailed performance improvement metrics"""

        metrics = {}

        if unoptimized.execution_time_ms > 0 and optimized.execution_time_ms > 0:
            # Speed improvement
            speedup_factor = unoptimized.execution_time_ms / optimized.execution_time_ms
            improvement_percent = ((unoptimized.execution_time_ms - optimized.execution_time_ms)
                                 / unoptimized.execution_time_ms) * 100
            time_saved = unoptimized.execution_time_ms - optimized.execution_time_ms

            metrics.update({
                'speedup_factor': round(speedup_factor, 2),
                'improvement_percent': round(improvement_percent, 2),
                'time_saved_ms': round(time_saved, 3),
                'optimized_time_ms': round(optimized.execution_time_ms, 3),
                'unoptimized_time_ms': round(unoptimized.execution_time_ms, 3)
            })
        else:
            metrics.update({
                'speedup_factor': 1.0,
                'improvement_percent': 0.0,
                'time_saved_ms': 0.0,
                'optimized_time_ms': optimized.execution_time_ms,
                'unoptimized_time_ms': unoptimized.execution_time_ms
            })

        # Result consistency check
        metrics['result_count_match'] = optimized.result_count == unoptimized.result_count
        metrics['both_successful'] = optimized.success and unoptimized.success

        return metrics

    def _determine_performance_winner(self, optimized: QueryResult,
                                    unoptimized: QueryResult) -> str:
        """Determine which approach performed better"""

        if not optimized.success and not unoptimized.success:
            return "both_failed"
        elif not optimized.success:
            return "unoptimized"
        elif not unoptimized.success:
            return "optimized"
        elif optimized.execution_time_ms < unoptimized.execution_time_ms:
            return "optimized"
        elif unoptimized.execution_time_ms < optimized.execution_time_ms:
            return "unoptimized"
        else:
            return "tie"

    def _generate_performance_analysis(self, optimized: QueryResult, unoptimized: QueryResult,
                                     query_config: Dict[str, Any]) -> List[str]:
        """Generate detailed performance analysis for educational purposes"""

        analysis = []
        database = query_config['database']

        # Overall performance summary
        if optimized.success and unoptimized.success:
            time_diff = unoptimized.execution_time_ms - optimized.execution_time_ms
            if time_diff > 1:  # More than 1ms difference
                analysis.append(f"⚡ Optimization provided {time_diff:.1f}ms improvement")
            else:
                analysis.append("📊 Both approaches performed similarly (small dataset or simple query)")

        # Database-specific analysis
        if database == 'mongodb':
            analysis.extend([
                "🍃 MongoDB Performance Analysis:",
                f"   Optimized: {'Used indexes' if optimized.optimization_used else 'Collection scan'}",
                f"   Unoptimized: {'Used indexes' if unoptimized.optimization_used else 'Collection scan (forced)'}"
            ])

            if optimized.optimization_used and not unoptimized.optimization_used:
                analysis.append("   💡 Index usage vs collection scan demonstrates MongoDB optimization")

        elif database == 'cassandra':
            opt_info = optimized.database_specific_info or {}
            unopt_info = unoptimized.database_specific_info or {}

            analysis.extend([
                "🏛️ Cassandra Performance Analysis:",
                f"   Optimized strategy: {opt_info.get('optimization_strategy', 'unknown')}",
                f"   Unoptimized strategy: {unopt_info.get('optimization_strategy', 'unknown')}"
            ])

            if opt_info.get('optimization_strategy') == 'partition_key_optimized':
                analysis.append("   💡 Partition key usage enables single-node access")
            if unopt_info.get('used_allow_filtering'):
                analysis.append("   ⚠️ ALLOW FILTERING scans all nodes in cluster")

        elif database == 'combined':
            analysis.extend([
                "🔗 Cross-Database Performance Analysis:",
                "   Query involves network overhead between databases",
                "   Performance depends on result set sizes and join selectivity"
            ])

        # Result validation
        if optimized.result_count != unoptimized.result_count:
            analysis.append(f"⚠️ Result count mismatch: optimized={optimized.result_count}, unoptimized={unoptimized.result_count}")
        else:
            analysis.append(f"✅ Both approaches returned {optimized.result_count} results")

        return analysis

    def _generate_optimization_recommendations(self, optimized: QueryResult, unoptimized: QueryResult,
                                             query_config: Dict[str, Any]) -> List[str]:
        """Generate actionable optimization recommendations"""

        recommendations = []
        database = query_config['database']

        # General recommendations based on performance
        if optimized.success and unoptimized.success:
            improvement = ((unoptimized.execution_time_ms - optimized.execution_time_ms)
                          / unoptimized.execution_time_ms * 100)

            if improvement > 50:
                recommendations.append("🎯 High optimization impact - always use optimized approach in production")
            elif improvement > 10:
                recommendations.append("✅ Moderate optimization benefit - recommended for frequent queries")
            else:
                recommendations.append("📝 Small optimization benefit - either approach acceptable")

        # Database-specific recommendations
        if database == 'mongodb':
            recommendations.extend([
                "💡 MongoDB Recommendations:",
                "   • Create compound indexes for multi-field queries",
                "   • Use $explain to analyze query plans",
                "   • Consider field order in compound indexes"
            ])

        elif database == 'cassandra':
            recommendations.extend([
                "💡 Cassandra Recommendations:",
                "   • Always filter by partition key when possible",
                "   • Design denormalized tables for query patterns",
                "   • Avoid ALLOW FILTERING in production workloads"
            ])

            # Specific recommendations based on query
            cassandra_config = query_config if query_config['database'] == 'cassandra' else query_config.get('cassandra', {})
            table_name = cassandra_config.get('table', '')
            if table_name.startswith('transactions_by_'):
                recommendations.append(f"   ✅ Good choice using {table_name} for this query pattern")

        elif database == 'combined':
            recommendations.extend([
                "💡 Cross-Database Recommendations:",
                "   • Minimize network round trips with efficient filtering",
                "   • Consider data locality and join field cardinality",
                "   • Cache frequently joined data if possible"
            ])

        return recommendations

    def _make_serializable(self, obj: Any) -> Any:
        """Convert MongoDB objects to JSON-serializable format"""
        if hasattr(obj, '__dict__'):
            return {key: self._make_serializable(value) for key, value in obj.__dict__.items()}
        elif isinstance(obj, dict):
            return {key: self._make_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(item) for item in obj]
        elif hasattr(obj, '__str__') and not isinstance(obj, (str, int, float, bool)):
            return str(obj)
        else:
            return obj

    def _convert_cassandra_value(self, value: Any) -> Any:
        """Convert Cassandra values to JSON-serializable format"""
        if isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, date):
            return value.isoformat()
        else:
            return value

    def get_performance_summary(self, comparison: PerformanceComparison) -> Dict[str, Any]:
        """Get a comprehensive performance summary for display"""

        return {
            'execution_times': {
                'optimized_ms': comparison.optimized_result.execution_time_ms,
                'unoptimized_ms': comparison.unoptimized_result.execution_time_ms
            },
            'performance_metrics': comparison.performance_improvement,
            'result_counts': {
                'optimized': comparison.optimized_result.result_count,
                'unoptimized': comparison.unoptimized_result.result_count
            },
            'success_status': {
                'optimized': comparison.optimized_result.success,
                'unoptimized': comparison.unoptimized_result.success
            },
            'winner': comparison.winner,
            'analysis': comparison.analysis,
            'recommendations': comparison.recommendations,
            'sample_results': comparison.optimized_result.results[:3] if comparison.optimized_result.results else []
        }


# Example usage and testing
if __name__ == "__main__":
    from .database_manager import DatabaseManager
    from .schema_inspector import SchemaInspector
    from .query_builder import QueryBuilder, create_filter

    # Test performance analyzer
    db_manager = DatabaseManager()

    if db_manager.connect_all():
        inspector = SchemaInspector(db_manager)
        inspector.inspect_all_schemas()

        builder = QueryBuilder(db_manager, inspector)
        analyzer = PerformanceAnalyzer(db_manager, inspector)

        print("⚡ Testing Performance Analyzer...")

        # Create a test query
        filters = [create_filter('position', '=', 'Barista')]
        query_config = builder.build_mongodb_query(
            collection='employees',
            filters=filters,
            use_optimization=True
        )

        print(f"\n🔧 Test Query: {query_config}")

        # Run performance comparison
        comparison = analyzer.compare_optimization_scenarios(query_config)

        print(f"\n📊 Performance Results:")
        print(f"   Optimized: {comparison.optimized_result.execution_time_ms}ms")
        print(f"   Unoptimized: {comparison.unoptimized_result.execution_time_ms}ms")
        print(f"   Winner: {comparison.winner}")
        print(f"   Speedup: {comparison.performance_improvement.get('speedup_factor', 1)}x")

        print(f"\n💡 Analysis:")
        for analysis_point in comparison.analysis:
            print(f"   {analysis_point}")

    db_manager.close_all_connections()
