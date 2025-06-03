# src/interfaces/cli_interface.py
import os
import sys
import json
import time
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database_manager import DatabaseManager
from core.schema_inspector import SchemaInspector
from core.query_builder import QueryBuilder, QueryFilter, create_filter
from core.performance_analyzer import PerformanceAnalyzer, PerformanceComparison

logger = logging.getLogger(__name__)

class CLIInterface:
    """
    Interactive CLI interface for dynamic database queries.
    Designed for live professor demonstrations with maximum flexibility.
    """

    def __init__(self):
        self.db_manager = DatabaseManager()
        self.schema_inspector = None
        self.query_builder = None
        self.performance_analyzer = None
        self.statistical_analyzer = None  # NEW
        self.is_initialized = False
        self.available_schemas = {}

        # UI styling
        self.colors = {
            'header': '\033[95m',
            'blue': '\033[94m',
            'green': '\033[92m',
            'yellow': '\033[93m',
            'red': '\033[91m',
            'bold': '\033[1m',
            'underline': '\033[4m',
            'end': '\033[0m'
        }

    def colored_text(self, text: str, color: str) -> str:
        """Add color to text for better terminal display"""
        return f"{self.colors.get(color, '')}{text}{self.colors['end']}"

    def clear_screen(self):
        """Clear the terminal screen"""
        os.system('cls' if os.name == 'nt' else 'clear')

    def print_header(self):
        """Print the main system header"""
        header = """
╔══════════════════════════════════════════════════════════════════╗
║          🌟 KEDAI KOPI NUSANTARA - DYNAMIC QUERY SYSTEM 🌟        ║
║                Multi-Database Performance Laboratory              ║
║                     Academic Demonstration Tool                  ║
╚══════════════════════════════════════════════════════════════════╝
        """
        print(self.colored_text(header, 'header'))

    def print_section_header(self, title: str):
        """Print a section header"""
        print(f"\n{self.colored_text('='*60, 'blue')}")
        print(f"{self.colored_text(f'📋 {title}', 'bold')}")
        print(f"{self.colored_text('='*60, 'blue')}")

    def print_menu(self, title: str, options: Dict[str, str]):
        """Print a formatted menu"""
        print(f"\n{self.colored_text(title, 'yellow')}")
        print(self.colored_text("-" * len(title), 'yellow'))
        for key, value in options.items():
            print(f"  {self.colored_text(f'[{key}]', 'green')} {value}")
        print()

    def get_user_choice(self, prompt: str, valid_choices: List[str]) -> str:
        """Get and validate user input"""
        while True:
            choice = input(f"{prompt} ").strip().lower()
            if choice in [c.lower() for c in valid_choices]:
                return choice
            print(f"{self.colored_text('❌ Invalid choice.', 'red')} Please select from: {', '.join(valid_choices)}")

    def initialize_system(self) -> bool:
        """Initialize all system components including statistical analyzer"""
        self.print_section_header("SYSTEM INITIALIZATION")

        print("🚀 Starting multi-database system...")
        print("   Connecting to MongoDB, Cassandra, and Redis...")

        # Connect to databases
        if not self.db_manager.connect_all():
            print(f"{self.colored_text('❌ Database connection failed!', 'red')}")
            print("   Please ensure Docker containers are running:")
            print("   sudo docker compose up -d")
            return False

        print(f"{self.colored_text('✅ All databases connected successfully!', 'green')}")

        # Initialize components
        print("🔍 Initializing schema inspector...")
        self.schema_inspector = SchemaInspector(self.db_manager)
        self.available_schemas = self.schema_inspector.inspect_all_schemas()

        print("🔧 Initializing query builder...")
        self.query_builder = QueryBuilder(self.db_manager, self.schema_inspector)

        print("⚡ Initializing performance analyzer...")
        self.performance_analyzer = PerformanceAnalyzer(self.db_manager, self.schema_inspector)

        print("📊 Initializing statistical analyzer...")
        from core.statistical_performance_analyzer import StatisticalPerformanceAnalyzer  # NEW
        self.statistical_analyzer = StatisticalPerformanceAnalyzer(self.performance_analyzer)

        # Display system status
        self.display_system_status()

        self.is_initialized = True
        print(f"{self.colored_text('🎉 System ready for queries!', 'green')}")
        return True

    def display_system_status(self):
        """Display current system and data status"""
        print(f"\n{self.colored_text('📊 SYSTEM STATUS', 'bold')}")

        # Connection status
        status = self.db_manager.get_connection_status()
        for db, connected in status['databases'].items():
            status_icon = "✅" if connected else "❌"
            print(f"   {status_icon} {db.capitalize()}: {'Connected' if connected else 'Disconnected'}")

        # Data counts
        print(f"\n{self.colored_text('📈 DATA OVERVIEW', 'bold')}")
        counts = self.db_manager.get_data_counts()

        # MongoDB
        if 'mongodb' in counts and isinstance(counts['mongodb'], dict):
            print(f"   🍃 MongoDB:")
            for collection, count in counts['mongodb'].items():
                print(f"     • {collection}: {count:,} documents")

        # Cassandra
        if 'cassandra' in counts and isinstance(counts['cassandra'], dict):
            print(f"   🏛️ Cassandra:")
            for table, count in counts['cassandra'].items():
                if isinstance(count, int):
                    print(f"     • {table}: {count:,} records")
                else:
                    print(f"     • {table}: {count}")

    def dynamic_query_wizard(self):
        """Interactive wizard for building any query combination"""
        self.print_section_header("DYNAMIC QUERY WIZARD")
        print("🧙‍♂️ Build any query combination for live demonstration!")
        print("   This wizard can handle ANY professor question...")

        # Step 1: Choose query type
        query_types = {
            '1': '🍃 MongoDB Only (Document Store)',
            '2': '🏛️ Cassandra Only (Column Store)',
            '3': '🔗 Cross-Database (Join both stores)',
            '4': '🎯 Smart Query (Auto-select best tables/fields)'
        }

        self.print_menu("Step 1: Select Query Type", query_types)
        choice = self.get_user_choice("Enter choice:", ['1', '2', '3', '4'])

        if choice == '1':
            return self._build_mongodb_query_interactive()
        elif choice == '2':
            return self._build_cassandra_query_interactive()
        elif choice == '3':
            return self._build_cross_database_query_interactive()
        else:
            return self._build_smart_query_interactive()

    def _build_mongodb_query_interactive(self) -> Dict[str, Any]:
        """Interactive MongoDB query builder"""
        print(f"\n{self.colored_text('🍃 BUILDING MONGODB QUERY', 'bold')}")

        # Choose collection
        mongodb_schema = self.available_schemas.get('mongodb', {})
        if not mongodb_schema:
            print(f"{self.colored_text('❌ No MongoDB collections found!', 'red')}")
            return None

        collections = list(mongodb_schema.keys())
        if 'error' in collections:
            collections.remove('error')

        print(f"Available collections: {', '.join(collections)}")
        collection_choice = input("Enter collection name: ").strip()

        if collection_choice not in collections:
            print(f"{self.colored_text('❌ Collection not found!', 'red')}")
            return None

        # Show available fields
        collection_info = mongodb_schema[collection_choice]
        if 'fields' not in collection_info:
            print(f"{self.colored_text('❌ No field information available!', 'red')}")
            return None

        fields = collection_info['fields']
        print(f"\n📋 Available fields in '{collection_choice}':")
        for field_name, field_info in list(fields.items())[:10]:  # Show first 10 fields
            field_type = field_info.get('dominant_type', 'unknown')
            print(f"   • {field_name} ({field_type})")

        if len(fields) > 10:
            print(f"   ... and {len(fields) - 10} more fields")

        # Build filters interactively
        filters = self._build_filters_interactive(fields, 'mongodb')

        # Optional sorting
        sort_field = None
        sort_order = 1
        if input("\nAdd sorting? (y/n): ").lower().startswith('y'):
            sort_field = input("Sort field: ").strip()
            if input("Descending order? (y/n): ").lower().startswith('y'):
                sort_order = -1

        # Optional limit
        limit = None
        if input("Add result limit? (y/n): ").lower().startswith('y'):
            try:
                limit = int(input("Limit (number): "))
            except ValueError:
                limit = 10

        # Build and execute query
        query_config = self.query_builder.build_mongodb_query(
            collection=collection_choice,
            filters=filters,
            sort_field=sort_field,
            sort_order=sort_order,
            limit=limit,
            use_optimization=True
        )

        return self._execute_and_display_query(query_config)

    def _build_cassandra_query_interactive(self) -> Dict[str, Any]:
        """Interactive Cassandra query builder"""
        print(f"\n{self.colored_text('🏛️ BUILDING CASSANDRA QUERY', 'bold')}")

        # Choose table
        cassandra_schema = self.available_schemas.get('cassandra', {})
        if not cassandra_schema:
            print(f"{self.colored_text('❌ No Cassandra tables found!', 'red')}")
            return None

        tables = list(cassandra_schema.keys())
        if 'error' in tables:
            tables.remove('error')

        print(f"Available tables:")
        for i, table in enumerate(tables, 1):
            table_info = cassandra_schema.get(table, {})
            partition_keys = table_info.get('partition_keys', [])
            opt_hint = f" (Optimized for: {', '.join(partition_keys)})" if partition_keys else ""
            print(f"   {i}. {table}{opt_hint}")

        table_choice = input("Enter table name: ").strip()

        if table_choice not in tables:
            print(f"{self.colored_text('❌ Table not found!', 'red')}")
            return None

        # Show available columns
        table_info = cassandra_schema[table_choice]
        if 'columns' not in table_info:
            print(f"{self.colored_text('❌ No column information available!', 'red')}")
            return None

        columns = table_info['columns']
        partition_keys = table_info.get('partition_keys', [])

        print(f"\n📋 Available columns in '{table_choice}':")
        for col_name, col_info in columns.items():
            col_type = col_info.get('python_type', 'unknown')
            special_tags = []
            if col_info.get('is_partition_key'):
                special_tags.append('PARTITION KEY')
            if col_info.get('is_clustering_key'):
                special_tags.append('CLUSTERING')

            tag_str = f" [{', '.join(special_tags)}]" if special_tags else ""
            print(f"   • {col_name} ({col_type}){tag_str}")

        if partition_keys:
            print(f"\n💡 {self.colored_text('Optimization tip:', 'yellow')} Filter by partition key(s) {partition_keys} for best performance!")

        # Build filters interactively
        filters = self._build_filters_interactive(columns, 'cassandra')

        # Build and execute query
        query_config = self.query_builder.build_cassandra_query(
            table=table_choice,
            filters=filters,
            use_optimization=True
        )

        return self._execute_and_display_query(query_config)

    def _build_smart_query_interactive(self):
        """Smart query builder that auto-selects best tables and handles any field"""
        print(f"\n{self.colored_text('🎯 SMART QUERY BUILDER', 'bold')}")
        print("This mode automatically finds the best table/collection for your fields!")

        # Choose database
        print(f"\nWhich database do you want to query?")
        print("  [1] MongoDB (Documents)")
        print("  [2] Cassandra (Columns)")

        db_choice = self.get_user_choice("Database:", ['1', '2'])
        database = 'mongodb' if db_choice == '1' else 'cassandra'

        # Show ALL available fields across all tables/collections
        print(f"\n{self.colored_text(f'📋 ALL AVAILABLE FIELDS IN {database.upper()}:', 'bold')}")

        all_fields = self.query_builder.get_all_available_fields(database)
        field_list = []

        for table_name, fields in all_fields.items():
            print(f"\n🏷️ {table_name}:")
            for field in fields[:10]:  # Show first 10 fields per table
                field_list.append(field)
                print(f"   • {field}")
            if len(fields) > 10:
                print(f"   ... and {len(fields) - 10} more")

        # Let user type ANY field name
        print(f"\n{self.colored_text('✨ FLEXIBLE FIELD SELECTION', 'yellow')}")
        print("Type any field name from the lists above (or try any field you think might exist!):")

        filters = []
        for i in range(5):  # Allow up to 5 filters
            print(f"\nFilter {i+1} (press Enter to skip):")

            field_name = input("  Field name: ").strip()
            if not field_name:
                break

            # Smart field validation - find which table has this field
            best_table = self.query_builder.find_best_table_for_field(field_name, database)
            if best_table:
                print(f"  ✅ Found '{field_name}' in {best_table}")

                # Get field info for type hints
                if database == 'mongodb':
                    field_info = self.available_schemas.get('mongodb', {}).get(best_table, {}).get('fields', {}).get(field_name, {})
                    field_type = field_info.get('dominant_type', 'str')
                    operators = field_info.get('available_operators', ['=', '!=', '>', '<'])
                else:
                    field_info = self.available_schemas.get('cassandra', {}).get(best_table, {}).get('columns', {}).get(field_name, {})
                    field_type = field_info.get('python_type', 'str')
                    operators = field_info.get('available_operators', ['=', '!=', '>', '<'])

                print(f"  💡 Type: {field_type}, Available operators: {', '.join(operators[:5])}")
            else:
                print(f"  ⚠️ Field '{field_name}' not found in schema, but we'll try anyway!")
                field_type = 'str'
                operators = ['=', '!=', '>', '<']

            # Get operator
            operator = input(f"  Operator (default =): ").strip() or '='

            # Get value with type hints
            value_input = input(f"  Value ({field_type}): ").strip()
            if not value_input:
                continue

            # Convert value to appropriate type
            try:
                if field_type in ['int', 'bigint']:
                    value = int(value_input)
                elif field_type in ['float', 'double', 'decimal']:
                    value = float(value_input)
                elif field_type == 'bool':
                    value = value_input.lower() in ['true', 'yes', '1']
                else:
                    value = value_input

                filter_obj = create_filter(field_name, operator, value, field_type)
                filters.append(filter_obj)
                print(f"  ✅ Added filter: {filter_obj}")

            except ValueError:
                print(f"  {self.colored_text('❌ Invalid value for type', 'red')} {field_type}")

        if not filters:
            print(f"{self.colored_text('❌ No filters added - adding default filter', 'yellow')}")
            # Add a default filter based on database
            if database == 'mongodb':
                filters = [create_filter('position', '=', 'Barista')]
            else:
                filters = [create_filter('employee_id', '=', 1, 'int')]

        # Build smart query
        print(f"\n{self.colored_text('🔧 Building smart query...', 'bold')}")

        user_fields = [f.field for f in filters]
        query_config = self.query_builder.build_smart_query(
            database=database,
            user_fields=user_fields,
            filters=filters,
            use_optimization=True
        )

        print(f"📋 Selected table/collection: {query_config.get('table', query_config.get('collection'))}")
        print(f"🎯 Optimization strategy: {query_config.get('optimization_strategy', 'automatic')}")

        return self._execute_and_display_query(query_config)

    def _build_cross_database_query_interactive(self) -> Dict[str, Any]:
        """Interactive cross-database query builder with proper error handling"""
        print(f"\n{self.colored_text('🔗 BUILDING CROSS-DATABASE QUERY', 'bold')}")
        print("This will join data from MongoDB and Cassandra!")

        # Show common fields for joining
        cross_fields = self.available_schemas.get('cross_database_fields', [])
        if cross_fields:
            print(f"\n💡 Available join fields: {', '.join([f['field_name'] for f in cross_fields])}")

        # Step 1: Build MongoDB part
        print(f"\n{self.colored_text('Step 1: MongoDB Query', 'blue')}")
        mongodb_config = self._build_mongodb_query_interactive()
        if not mongodb_config:
            return None

        # Step 2: Build Cassandra part
        print(f"\n{self.colored_text('Step 2: Cassandra Query', 'blue')}")
        cassandra_config = self._build_cassandra_query_interactive()
        if not cassandra_config:
            return None

        # Step 3: Choose join field
        print(f"\n{self.colored_text('Step 3: Join Configuration', 'blue')}")
        join_field = input("Join field (default: employee_id): ").strip() or 'employee_id'

        # Build cross-database query with proper config structure
        cross_query_config = self.query_builder.build_cross_database_query(
            mongodb_config=mongodb_config.get('query_config', mongodb_config),
            cassandra_config=cassandra_config.get('query_config', cassandra_config),
            join_field=join_field
        )

        return self._execute_and_display_query(cross_query_config)

    def _build_filters_interactive(self, fields_info: Dict[str, Any], database_type: str) -> List[QueryFilter]:
        """Interactive filter builder"""
        filters = []

        print(f"\n🔍 Building filters (press Enter to skip any field):")

        # Get up to 5 filters
        for i in range(5):
            print(f"\nFilter {i+1}:")
            field_name = input("  Field name: ").strip()
            if not field_name:
                break

            if field_name not in fields_info:
                print(f"  {self.colored_text('⚠️ Field not found in schema', 'yellow')}")
                continue

            field_info = fields_info[field_name]

            # Show available operators
            if database_type == 'mongodb':
                operators = field_info.get('available_operators', ['=', '!=', '>', '<'])
            else:
                operators = field_info.get('available_operators', ['=', '!=', '>', '<'])

            print(f"  Available operators: {', '.join(operators)}")
            operator = input("  Operator: ").strip() or '='

            # Get value with type hint
            field_type = field_info.get('dominant_type' if database_type == 'mongodb' else 'python_type', 'str')
            value_input = input(f"  Value ({field_type}): ").strip()

            if not value_input:
                continue

            # Convert value to appropriate type
            try:
                if field_type in ['int', 'bigint']:
                    value = int(value_input)
                elif field_type in ['float', 'double', 'decimal']:
                    value = float(value_input)
                elif field_type == 'bool':
                    value = value_input.lower() in ['true', 'yes', '1']
                else:
                    value = value_input

                filter_obj = create_filter(field_name, operator, value, field_type)
                filters.append(filter_obj)
                print(f"  ✅ Added filter: {filter_obj}")

            except ValueError:
                print(f"  {self.colored_text('❌ Invalid value for type', 'red')} {field_type}")

        return filters

    def _execute_and_display_query(self, query_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute query with choice of single or statistical analysis"""
        if not query_config:
            return None

        print(f"\n{self.colored_text('⚡ PERFORMANCE ANALYSIS OPTIONS', 'bold')}")

        analysis_options = {
            '1': '🔬 Single Run (Quick)',
            '2': '📊 Statistical Analysis (10 runs)',
            '3': '🎯 Comprehensive Benchmark (20 runs)',
            '4': '🔧 Custom Run Count'
        }

        self.print_menu("Choose Analysis Type", analysis_options)
        choice = self.get_user_choice("Enter choice:", ['1', '2', '3', '4'])

        if choice == '1':
            # Original single-run analysis
            print("Running single performance comparison...")
            comparison = self.performance_analyzer.compare_optimization_scenarios(query_config)
            # ADD THESE DEBUG LINES:
            print(f"\n🔍 DEBUG INFO:")
            print(f"   Optimized Success: {comparison.optimized_result.success}")
            print(f"   Optimized Count: {comparison.optimized_result.result_count}")
            print(f"   Optimized Error: {comparison.optimized_result.error_message}")
            print(f"   Unoptimized Success: {comparison.unoptimized_result.success}")
            print(f"   Unoptimized Count: {comparison.unoptimized_result.result_count}")
            print(f"   Unoptimized Error: {comparison.unoptimized_result.error_message}")

            if comparison.optimized_result.results:
                print(f"   Sample Result: {comparison.optimized_result.results[0]}")
            else:
                print(f"   No results returned")
            self._display_performance_comparison_with_results(comparison)

            return {
                'query_config': query_config,
                'performance_comparison': comparison
            }

        else:
            # Statistical analysis
            if choice == '2':
                run_count = 10
            elif choice == '3':
                run_count = 20
            else:  # choice == '4'
                try:
                    run_count = int(input("Enter number of runs (5-50): "))
                    run_count = max(5, min(50, run_count))  # Clamp between 5-50
                except ValueError:
                    run_count = 10
                    print("Invalid input, using 10 runs")

            # Run statistical analysis
            results = self.statistical_analyzer.run_statistical_analysis(query_config, run_count)

            # Offer export options
            self._offer_export_options(results)

            return {
                'query_config': query_config,
                'statistical_results': results
            }

    def _offer_export_options(self, results: Dict[str, Any]):
        """Offer to export statistical results and generate charts"""

        print(f"\n{self.colored_text('📊 EXPORT & VISUALIZATION OPTIONS', 'bold')}")

        export_choice = input("Export results and/or create charts? (y/N): ").strip().lower()
        if export_choice == 'y':
            export_options = {
                '1': '📋 CSV Export (for Excel/analysis)',
                '2': '📄 JSON Export (raw data)',
                '3': '📈 Generate Charts (PNG images)',
                '4': '🎯 Complete Package (CSV + JSON + Charts)',
                '5': '📊 Professional Dashboard (single comprehensive chart)'
            }

            self.print_menu("Export Options", export_options)
            format_choice = self.get_user_choice("Enter choice:", ['1', '2', '3', '4', '5'])

            try:
                files_created = []

                # Data exports
                if format_choice in ['1', '4']:
                    csv_file = self.statistical_analyzer.export_results(results, 'csv')
                    files_created.append(f"📋 {csv_file}")

                if format_choice in ['2', '4']:
                    json_file = self.statistical_analyzer.export_results(results, 'json')
                    files_created.append(f"📄 {json_file}")

                # Chart generation
                if format_choice in ['3', '4', '5']:
                    try:
                        import sys
                        import os
                        sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
                        from src.utils.chart_generator import PerformanceChartGenerator
                        chart_gen = PerformanceChartGenerator()

                        if format_choice == '5':
                            # Professional dashboard
                            dashboard_file = chart_gen.generate_comparison_chart(results, 'dashboard')
                            files_created.append(f"📊 {dashboard_file}")
                        else:
                            # Multiple chart types
                            line_chart = chart_gen.generate_comparison_chart(results, 'line')
                            box_chart = chart_gen.generate_comparison_chart(results, 'box')
                            bar_chart = chart_gen.generate_comparison_chart(results, 'bar')

                            files_created.extend([
                                f"📈 {line_chart}",
                                f"📦 {box_chart}",
                                f"📊 {bar_chart}"
                            ])

                    except ImportError:
                        print(f"⚠️  Chart generation requires matplotlib: pip install matplotlib")
                    except Exception as chart_error:
                        print(f"⚠️  Chart generation failed: {chart_error}")

                # Display created files
                if files_created:
                    print(f"\n{self.colored_text('✅ FILES CREATED:', 'green')}")
                    for file_desc in files_created:
                        print(f"   {file_desc}")

                    print(f"\n💡 {self.colored_text('Usage Tips:', 'yellow')}")
                    print("   📋 Import CSV into Excel/Google Sheets for custom analysis")
                    print("   📈 Use PNG charts in presentations or reports")
                    print("   📄 JSON contains raw data for programmatic analysis")
                else:
                    print("❌ No files were created")

            except Exception as e:
                print(f"❌ Export failed: {e}")

    def _display_performance_comparison_with_results(self, comparison):
        """Display comprehensive performance comparison with top 5 actual results"""

        print(f"\n{self.colored_text('📊 PERFORMANCE RESULTS', 'bold')}")
        print("="*50)

        # Execution times
        opt_time = comparison.optimized_result.execution_time_ms
        unopt_time = comparison.unoptimized_result.execution_time_ms

        print(f"🚀 {self.colored_text('OPTIMIZED:', 'green')}    {opt_time:.2f}ms")
        print(f"🐌 {self.colored_text('UNOPTIMIZED:', 'red')}  {unopt_time:.2f}ms")

        # Performance improvement
        improvement = comparison.performance_improvement
        if improvement.get('speedup_factor', 1) > 1:
            speedup = improvement['speedup_factor']
            percent = improvement['improvement_percent']
            time_saved = improvement['time_saved_ms']

            print(f"\n🏆 {self.colored_text('IMPROVEMENT:', 'yellow')}")
            print(f"   ⚡ Speedup Factor: {speedup:.1f}x faster")
            print(f"   📈 Performance Gain: {percent:.1f}%")
            print(f"   ⏱️  Time Saved: {time_saved:.2f}ms")
        else:
            print(f"\n📊 Both approaches performed similarly")

        # Result counts
        opt_count = comparison.optimized_result.result_count
        unopt_count = comparison.unoptimized_result.result_count
        print(f"   📋 Results: {opt_count} records")

        if opt_count != unopt_count:
            print(f"   {self.colored_text('⚠️ Result count mismatch!', 'red')} Unoptimized: {unopt_count}")

        # Show errors if any
        if not comparison.optimized_result.success:
            print(f"   {self.colored_text('❌ Optimized query failed:', 'red')} {comparison.optimized_result.error_message}")
        if not comparison.unoptimized_result.success:
            print(f"   {self.colored_text('❌ Unoptimized query failed:', 'red')} {comparison.unoptimized_result.error_message}")

        # Display TOP 5 ACTUAL RESULTS
        if comparison.optimized_result.success and comparison.optimized_result.results:
            print(f"\n{self.colored_text('🏆 TOP 5 RESULTS:', 'bold')}")
            self._display_top_results(comparison.optimized_result.results[:5])
        elif comparison.unoptimized_result.success and comparison.unoptimized_result.results:
            print(f"\n{self.colored_text('🏆 TOP 5 RESULTS (from unoptimized):', 'bold')}")
            self._display_top_results(comparison.unoptimized_result.results[:5])
        else:
            print(f"\n{self.colored_text('📋 No results to display', 'yellow')}")

        # Analysis and recommendations
        print(f"\n{self.colored_text('💡 PERFORMANCE ANALYSIS', 'bold')}")
        for analysis_point in comparison.analysis:
            print(f"   {analysis_point}")

        print(f"\n{self.colored_text('🎯 RECOMMENDATIONS', 'bold')}")
        for recommendation in comparison.recommendations:
            print(f"   {recommendation}")

    def _display_top_results(self, results: List[Dict[str, Any]]):
        """Display top results in a clean, readable format"""

        for i, result in enumerate(results, 1):
            print(f"\n📌 {self.colored_text(f'Result #{i}:', 'green')}")

            # Display key fields first
            priority_fields = ['employee_id', 'name', 'position', 'total_amount', 'payment_method', 'timestamp']
            displayed_fields = set()

            # Show priority fields first
            for field in priority_fields:
                if field in result:
                    value = result[field]
                    if isinstance(value, str) and len(value) > 50:
                        value = value[:47] + "..."
                    elif isinstance(value, list) and len(value) > 3:
                        value = value[:3] + ["..."]

                    print(f"   {self.colored_text(field + ':', 'blue')} {value}")
                    displayed_fields.add(field)

            # Show remaining fields (up to 8 total to keep readable)
            remaining_fields = [k for k in result.keys() if k not in displayed_fields and not k.startswith('_')][:5]
            for field in remaining_fields:
                value = result[field]
                if isinstance(value, str) and len(value) > 50:
                    value = value[:47] + "..."
                elif isinstance(value, list) and len(value) > 3:
                    value = value[:3] + ["..."]

                print(f"   {field}: {value}")

            if len(result) > len(displayed_fields) + len(remaining_fields):
                print(f"   {self.colored_text('... and more fields', 'yellow')}")

    def _display_performance_comparison(self, comparison: PerformanceComparison):
        """Display comprehensive performance comparison results"""

        print(f"\n{self.colored_text('📊 PERFORMANCE RESULTS', 'bold')}")
        print("="*50)

        # Execution times
        opt_time = comparison.optimized_result.execution_time_ms
        unopt_time = comparison.unoptimized_result.execution_time_ms

        print(f"🚀 {self.colored_text('OPTIMIZED:', 'green')}    {opt_time:.2f}ms")
        print(f"🐌 {self.colored_text('UNOPTIMIZED:', 'red')}  {unopt_time:.2f}ms")

        # Performance improvement
        improvement = comparison.performance_improvement
        if improvement.get('speedup_factor', 1) > 1:
            speedup = improvement['speedup_factor']
            percent = improvement['improvement_percent']
            time_saved = improvement['time_saved_ms']

            print(f"\n🏆 {self.colored_text('IMPROVEMENT:', 'yellow')}")
            print(f"   ⚡ Speedup Factor: {speedup:.1f}x faster")
            print(f"   📈 Performance Gain: {percent:.1f}%")
            print(f"   ⏱️  Time Saved: {time_saved:.2f}ms")
        else:
            print(f"\n📊 Both approaches performed similarly")

        # Result counts
        opt_count = comparison.optimized_result.result_count
        unopt_count = comparison.unoptimized_result.result_count
        print(f"   📋 Results: {opt_count} records")

        if opt_count != unopt_count:
            print(f"   {self.colored_text('⚠️ Result count mismatch!', 'red')} Unoptimized: {unopt_count}")

    def force_reload_data(self):
        """Force reload transaction data from CSV"""
        self.print_section_header("FORCE RELOAD TRANSACTION DATA")

        print("🔄 This will reload all transaction data from the CSV file...")
        print("⚠️  This may take several minutes for large datasets")

        confirm = input("Continue? (y/N): ").strip().lower()
        if confirm != 'y':
            print("❌ Data reload cancelled")
            return

        try:
            # Import the fixed DataLoader
            import sys
            import os
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

            from data_loaders.data_loader import DataLoader

            loader = DataLoader(self.db_manager)

            # Look for CSV file
            csv_paths = [
                'data/transactions.csv',
                'transactions.csv',
                '../data/transactions.csv'
            ]

            csv_file = None
            for path in csv_paths:
                if os.path.exists(path):
                    csv_file = path
                    break

            if not csv_file:
                print(f"{self.colored_text('❌ transactions.csv not found!', 'red')}")
                print("   Checked locations:")
                for path in csv_paths:
                    print(f"   - {path}")
                return

            print(f"📁 Found CSV file: {csv_file}")

            # Check file size
            file_size = os.path.getsize(csv_file)
            print(f"📄 File size: {file_size:,} bytes")

            # Count lines
            with open(csv_file, 'r') as f:
                line_count = sum(1 for line in f) - 1  # Subtract header
            print(f"📊 Estimated records: {line_count:,}")

            print("\n🚀 Starting data loading...")
            result = loader.load_cassandra_transactions(csv_file)

            if result['status'] == 'success':
                print(f"\n{self.colored_text('✅ Data loading successful!', 'green')}")
                print(f"📊 CSV rows: {result.get('total_rows_in_csv', 'unknown'):,}")
                print(f"📊 Parsed: {result.get('total_parsed', 'unknown'):,}")
                print(f"📊 Inserted: {result.get('total_inserted', 0):,}")
                print(f"📋 Tables: {', '.join(result.get('tables_populated', []))}")

                if 'performance_note' in result:
                    print(f"\n💡 {result['performance_note']}")

                if result.get('failed_inserts', 0) > 0:
                    print(f"⚠️  Failed inserts: {result['failed_inserts']}")

                # Show transaction items data if loaded
                if 'transaction_items_inserted' in result:
                    print(f"📊 Transaction items: {result['transaction_items_inserted']:,}")

                # Show current data status after loading
                print(f"\n{self.colored_text('📊 Updated System Status:', 'bold')}")
                self.display_system_status()

            else:
                print(f"\n{self.colored_text('❌ Data loading failed!', 'red')}")
                print(f"Error: {result.get('message', 'Unknown error')}")

        except Exception as e:
            print(f"\n{self.colored_text(f'❌ Error during data loading: {e}', 'red')}")
            logger.exception("Data loading error")

    def force_reload_mongodb_data(self):
        """Force reload MongoDB data specifically"""
        self.print_section_header("FORCE RELOAD MONGODB DATA")

        print("🔄 This will reload employees and menu data from JSON files...")
        print("⚠️  This will clear existing MongoDB data")

        confirm = input("Continue? (y/N): ").strip().lower()
        if confirm != 'y':
            print("❌ MongoDB reload cancelled")
            return

        try:
            from data_loaders.data_loader import DataLoader
            loader = DataLoader(self.db_manager)

            print("\n🚀 Starting MongoDB data loading...")
            result = loader.load_mongodb_data()

            if result.get('employees', {}).get('status') == 'success':
                emp_count = result['employees'].get('inserted_count', 0)
                print(f"\n{self.colored_text('✅ Employee loading successful!', 'green')}")
                print(f"📊 Employees loaded: {emp_count}")
            else:
                print(f"\n{self.colored_text('❌ Employee loading failed!', 'red')}")
                print(f"Error: {result.get('employees', {}).get('message', 'Unknown error')}")

            if result.get('menu_items', {}).get('status') == 'success':
                menu_count = result['menu_items'].get('inserted_count', 0)
                print(f"✅ Menu loading successful!")
                print(f"📊 Menu items loaded: {menu_count}")
            else:
                print(f"❌ Menu loading failed!")
                print(f"Error: {result.get('menu_items', {}).get('message', 'Unknown error')}")

            # Show updated status
            print(f"\n{self.colored_text('📊 Updated MongoDB Status:', 'bold')}")
            counts = self.db_manager.get_data_counts()
            mongodb_counts = counts.get('mongodb', {})
            for collection, count in mongodb_counts.items():
                print(f"   🍃 {collection}: {count} documents")

        except Exception as e:
            print(f"\n{self.colored_text(f'❌ Error during MongoDB loading: {e}', 'red')}")
            logger.exception("MongoDB loading error")

    def _display_sample_results(self, results: List[Dict[str, Any]]):
        """Display sample query results in a readable format"""

        for i, result in enumerate(results, 1):
            print(f"\n📌 Record {i}:")
            for key, value in result.items():
                if key != '_id':  # Skip MongoDB ObjectId
                    # Truncate long values
                    if isinstance(value, str) and len(value) > 50:
                        value = value[:47] + "..."
                    elif isinstance(value, list) and len(value) > 3:
                        value = value[:3] + ["..."]

                    print(f"   {key}: {value}")

        if len(results) > 3:
            print(f"\n   ... and {len(results) - 3} more records")

    def run_predefined_demos(self):
        """Run the 3 required demo scenarios with proper optimization demonstrations"""
        self.print_section_header("PREDEFINED DEMO SCENARIOS")
        print("🎯 Running the 3 required query types for assignment demonstration...")
        print("💡 These demos use SIMPLE queries to show clear optimization differences")

        # Demo 1: Cassandra only - Simple partition key vs ALLOW FILTERING
        print(f"\n{self.colored_text('Demo 1: Cassandra Query (Payment Method Optimization)', 'bold')}")
        print("Shows: Partition key access vs full table scan")

        # Create SIMPLE Cassandra query - just partition key filter
        cassandra_filters_simple = [create_filter('payment_method', '=', 'Credit Card')]

        # Optimized: Use transactions_by_payment (partition key = payment_method)
        cassandra_config_opt = self.query_builder.build_cassandra_query(
            table='transactions_by_payment',
            filters=cassandra_filters_simple,
            use_optimization=True
        )

        # Non-optimized: Force main table with ALLOW FILTERING
        cassandra_config_unopt = self.query_builder.build_cassandra_query(
            table='transactions',  # Main table, no partition key optimization
            filters=cassandra_filters_simple,
            use_optimization=False
        )

        # Manual comparison for better control
        print("🚀 Optimized: Using transactions_by_payment (partition key access)")
        opt_result = self.performance_analyzer.execute_query_with_timing(cassandra_config_opt)

        print("🐌 Non-optimized: Using transactions table with ALLOW FILTERING")
        unopt_result = self.performance_analyzer.execute_query_with_timing(cassandra_config_unopt)

        self._display_manual_comparison(opt_result, unopt_result, "Cassandra Partition Key vs Full Scan")

        input("\n⏸️ Press Enter to continue to next demo...")

        # Demo 2: MongoDB only - Simple employee query
        print(f"\n{self.colored_text('Demo 2: MongoDB Query (Employee Position Index)', 'bold')}")
        print("Shows: Index usage vs collection scan")

        mongodb_filters = [create_filter('position', '=', 'Barista')]

        # Optimized: Use indexes
        mongodb_config_opt = self.query_builder.build_mongodb_query(
            collection='employees',
            filters=mongodb_filters,
            sort_field='performance_rating',
            sort_order=-1,
            limit=10,
            use_optimization=True
        )

        # Non-optimized: Force collection scan
        mongodb_config_unopt = self.query_builder.build_mongodb_query(
            collection='employees',
            filters=mongodb_filters,
            sort_field='performance_rating',
            sort_order=-1,
            limit=10,
            use_optimization=False
        )

        print("🚀 Optimized: Using position index")
        opt_result = self.performance_analyzer.execute_query_with_timing(mongodb_config_opt)

        print("🐌 Non-optimized: Collection scan with $natural hint")
        unopt_result = self.performance_analyzer.execute_query_with_timing(mongodb_config_unopt)

        self._display_manual_comparison(opt_result, unopt_result, "MongoDB Index vs Collection Scan")

        input("\n⏸️ Press Enter to continue to next demo...")

        # Demo 3: Cross-database - Simple join
        print(f"\n{self.colored_text('Demo 3: Cross-Database Query (Simple Employee-Transaction Join)', 'bold')}")
        print("Shows: Optimized vs non-optimized cross-database operations")

        # Get barista employees
        barista_query = self.query_builder.build_mongodb_query(
            collection='employees',
            filters=[create_filter('position', '=', 'Barista')],
            limit=3,  # Limit to 3 employees for demo
            use_optimization=True
        )

        print("🔄 Step 1: Finding Barista employees...")
        mongo_result = self.performance_analyzer.execute_query_with_timing(barista_query)

        if mongo_result.success and mongo_result.results:
            employee_ids = [emp['employee_id'] for emp in mongo_result.results[:2]]  # Just 2 employees
            print(f"   Found employees: {employee_ids}")

            print("🔄 Step 2: Getting their transactions...")

            # Optimized: Use employee partition table
            print("🚀 Optimized: Using transactions_by_employee partition table")
            start_time = time.time()
            opt_transactions = []
            for emp_id in employee_ids:
                emp_query = f"SELECT * FROM transactions_by_employee WHERE employee_id = %s LIMIT 100"
                result = self.db_manager.cassandra_session.execute(emp_query, [emp_id])
                opt_transactions.extend(list(result))
            opt_time = (time.time() - start_time) * 1000

            # Non-optimized: Use main table with ALLOW FILTERING
            print("🐌 Non-optimized: Using main transactions table with ALLOW FILTERING")
            start_time = time.time()
            unopt_query = f"SELECT * FROM transactions WHERE employee_id IN ({','.join(['%s']*len(employee_ids))}) ALLOW FILTERING"
            result = self.db_manager.cassandra_session.execute(unopt_query, employee_ids)
            unopt_transactions = list(result)
            unopt_time = (time.time() - start_time) * 1000

            print(f"\n📊 Cross-Database Performance:")
            print(f"🚀 Optimized (partition tables): {opt_time:.2f}ms")
            print(f"🐌 Non-optimized (ALLOW FILTERING): {unopt_time:.2f}ms")

            if unopt_time > opt_time:
                speedup = unopt_time / opt_time
                print(f"⚡ Speedup: {speedup:.1f}x faster")
                print(f"📊 Results: {len(opt_transactions)} transactions")
            else:
                print("📊 Both approaches completed successfully")

            print(f"\n💡 Why This Works:")
            print(f"   ✅ Optimized: Direct partition key access per employee")
            print(f"   ❌ Non-optimized: Full cluster scan with filtering")

        print(f"\n{self.colored_text('🎉 All demo scenarios completed!', 'green')}")
        print("💡 With 500K records, optimization differences become very clear!")

        # BONUS Demo: Make menu data actually useful
        print(f"\n{self.colored_text('🍽️ BONUS Demo: Menu-Smart Business Intelligence', 'bold')}")
        print("Shows: Cross-database analytics combining menu and transaction data")

        try:
            self._run_menu_business_demo()
        except Exception as e:
            print(f"⚠️ Menu demo skipped: {e}")

    def _run_menu_business_demo(self):
        """Demonstrate meaningful menu data analysis with transaction items"""
        print("🔄 Analyzing menu performance with real transaction data...")

        # Get menu data
        menu_query = self.query_builder.build_mongodb_query(
            collection='menu_items',
            filters=[],
            use_optimization=True
        )
        menu_result = self.performance_analyzer.execute_query_with_timing(menu_query)

        if not menu_result.success or not menu_result.results:
            print("❌ Could not load menu data")
            return

        menu_items = menu_result.results

        # Create menu lookup
        menu_lookup = {item['menu_id']: item for item in menu_items}

        # Categorize menu items
        categories = {}
        for item in menu_items:
            category = item['category']
            if category not in categories:
                categories[category] = []
            categories[category].append(item)

        print(f"\n📊 Menu Catalog Analysis:")
        for category, items in categories.items():
            avg_price = sum(item['price'] for item in items) / len(items)
            print(f"   🏷️ {category}: {len(items)} items (avg: {avg_price:,.0f} IDR)")

        # Analyze top-selling menu items using transaction items
        print(f"\n🔄 Analyzing menu item sales performance...")

        try:
            # Get top selling items (using partition key optimization)
            top_items_query = """
                SELECT menu_item_id, COUNT(*) as order_count
                FROM items_by_menu
                WHERE menu_item_id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
                GROUP BY menu_item_id
            """

            start_time = time.time()
            result = self.db_manager.cassandra_session.execute(top_items_query)
            query_time = (time.time() - start_time) * 1000

            item_sales = list(result)

            print(f"📊 Top Menu Items Performance:")
            print(f"   ⏱️ Query time: {query_time:.2f}ms")

            # Sort and display top items
            sorted_items = sorted(item_sales, key=lambda x: x.order_count, reverse=True)

            print(f"\n🏆 Top 10 Best-Selling Items:")
            for i, item_data in enumerate(sorted_items[:10], 1):
                menu_item = menu_lookup.get(item_data.menu_item_id)
                if menu_item:
                    revenue_est = item_data.order_count * menu_item['price']
                    print(f"   {i}. {menu_item['name']} ({menu_item['category']})")
                    print(f"      📈 {item_data.order_count:,} orders, ~{revenue_est:,.0f} IDR revenue")

            # Category performance analysis
            print(f"\n📊 Category Performance Analysis:")
            category_performance = {}

            for item_data in item_sales:
                menu_item = menu_lookup.get(item_data.menu_item_id)
                if menu_item:
                    category = menu_item['category']
                    if category not in category_performance:
                        category_performance[category] = {'orders': 0, 'items': 0, 'revenue': 0}

                    category_performance[category]['orders'] += item_data.order_count
                    category_performance[category]['items'] += 1
                    category_performance[category]['revenue'] += item_data.order_count * menu_item['price']

            for category, perf in sorted(category_performance.items(), key=lambda x: x[1]['revenue'], reverse=True):
                print(f"   🏷️ {category}:")
                print(f"      📊 {perf['orders']:,} total orders from {perf['items']} items")
                print(f"      💰 ~{perf['revenue']:,.0f} IDR estimated revenue")
                print(f"      📈 {perf['orders']/perf['items']:.1f} avg orders per item")

            # Employee menu expertise analysis (FIXED QUERY)
            print(f"\n👥 Employee Menu Expertise Analysis:")

            # Fixed query - removed DISTINCT which was causing syntax error
            employee_expertise_query = """
                SELECT employee_id, COUNT(*) as items_sold
                FROM items_by_menu
                WHERE menu_item_id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
                GROUP BY employee_id
                ALLOW FILTERING
            """

            start_time = time.time()
            emp_result = self.db_manager.cassandra_session.execute(employee_expertise_query)
            emp_query_time = (time.time() - start_time) * 1000

            emp_performance = list(emp_result)
            sorted_emp = sorted(emp_performance, key=lambda x: x.items_sold, reverse=True)[:5]

            print(f"🏆 Top 5 Menu Item Sellers:")
            for i, emp_data in enumerate(sorted_emp, 1):
                print(f"   {i}. Employee {emp_data.employee_id}:")
                print(f"      📊 {emp_data.items_sold:,} items sold")

            print(f"\n💡 Business Intelligence Summary:")
            print(f"   📊 Menu Strategy: {len(categories)} categories analyzed")
            print(f"   🎯 Performance Data: 500K+ transaction items processed")
            print(f"   ⚡ Query Performance: {query_time:.2f}ms + {emp_query_time:.2f}ms")
            print(f"   💰 Revenue Insights: Category ranking by actual sales")
            print(f"   👥 Staff Training: Employee menu expertise identified")

            print(f"\n🎉 This demonstrates the REAL power of connecting menu data to transactions!")

        except Exception as e:
            print(f"❌ Advanced menu analysis failed: {e}")
            print("   (This feature requires transaction_items data to be loaded)")

            # Fallback to basic menu analysis
            print(f"\n💡 Basic Menu Analysis (without transaction items):")
            for category, items in categories.items():
                total_value = sum(item['price'] for item in items)
                print(f"   🏷️ {category}: {len(items)} items, {total_value:,.0f} IDR total menu value")

    def _display_manual_comparison(self, opt_result, unopt_result, title):
        """Display manual performance comparison with top 5 results"""
        print(f"\n📊 {title} Results:")
        print(f"🚀 Optimized: {opt_result.execution_time_ms:.2f}ms ({opt_result.result_count} results)")
        print(f"🐌 Non-optimized: {unopt_result.execution_time_ms:.2f}ms ({unopt_result.result_count} results)")

        if opt_result.success and unopt_result.success:
            if unopt_result.execution_time_ms > opt_result.execution_time_ms:
                speedup = unopt_result.execution_time_ms / opt_result.execution_time_ms
                improvement = ((unopt_result.execution_time_ms - opt_result.execution_time_ms) / unopt_result.execution_time_ms) * 100
                print(f"⚡ Performance: {speedup:.1f}x faster ({improvement:.1f}% improvement)")
            elif opt_result.execution_time_ms > unopt_result.execution_time_ms:
                print("⚠️ Optimized query was slower (possible reasons: cache effects, small dataset)")
            else:
                print("📊 Similar performance (both very fast)")

        # Show top 5 results from the successful query
        if opt_result.success and opt_result.results:
            print(f"\n{self.colored_text('🏆 TOP 5 RESULTS:', 'bold')}")
            self._display_top_results(opt_result.results[:5])
        elif unopt_result.success and unopt_result.results:
            print(f"\n{self.colored_text('🏆 TOP 5 RESULTS (from unoptimized):', 'bold')}")
            self._display_top_results(unopt_result.results[:5])

        if not opt_result.success:
            print(f"❌ Optimized query failed: {opt_result.error_message}")
        if not unopt_result.success:
            print(f"❌ Non-optimized query failed: {unopt_result.error_message}")

    def main_menu(self):
        """Enhanced main menu with MongoDB reload option"""
        while True:
            self.clear_screen()
            self.print_header()

            if not self.is_initialized:
                if not self.initialize_system():
                    input(f"\n{self.colored_text('❌ Press Enter to retry...', 'red')}")
                    continue

            menu_options = {
                '1': '🎯 Quick Demo (3 Required Scenarios)',
                '2': '🧙‍♂️ Dynamic Query Wizard (Handle ANY Question)',
                '3': '📊 System Status & Data Overview',
                '4': '📥 Force Reload Transaction Data (Cassandra)',
                '5': '🍃 Force Reload MongoDB Data (Employees & Menu)',  # NEW
                '6': '❌ Exit'
            }

            self.print_menu("MAIN MENU", menu_options)
            choice = self.get_user_choice("Select option:", ['1', '2', '3', '4', '5', '6'])

            try:
                if choice == '1':
                    self.run_predefined_demos()
                elif choice == '2':
                    self.dynamic_query_wizard()
                elif choice == '3':
                    self.display_system_status()
                elif choice == '4':
                    self.force_reload_data()
                elif choice == '5':
                    self.force_reload_mongodb_data()  # NEW
                elif choice == '6':
                    print(f"\n{self.colored_text('👋 Thank you for using Kedai Kopi Dynamic Query System!', 'green')}")
                    print("🎓 Perfect for handling any professor question!")
                    break

                input(f"\n{self.colored_text('⏸️ Press Enter to continue...', 'yellow')}")

            except KeyboardInterrupt:
                print(f"\n\n{self.colored_text('👋 Goodbye!', 'green')}")
                break
            except Exception as e:
                print(f"\n{self.colored_text(f'❌ Error: {e}', 'red')}")
                logger.exception("CLI Interface error")
                input(f"{self.colored_text('⏸️ Press Enter to continue...', 'yellow')}")

    def cleanup(self):
        """Clean up resources"""
        if self.db_manager:
            self.db_manager.close_all_connections()


# Entry point
if __name__ == "__main__":
    cli = CLIInterface()
    try:
        cli.main_menu()
    finally:
        cli.cleanup()
