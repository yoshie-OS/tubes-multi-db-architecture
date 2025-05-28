# aggregator/query_scenarios.py
from flask import Blueprint, jsonify, request
import time
from datetime import datetime, timedelta
import logging
import csv
import io
from werkzeug.utils import secure_filename

logger = logging.getLogger(__name__)

# Create Blueprint for scenarios
scenarios_bp = Blueprint('scenarios', __name__, url_prefix='/api/scenarios')
data_bp = Blueprint('data', __name__, url_prefix='/api')

def measure_query_time(func):
    """Decorator to measure query execution time"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = round((end_time - start_time) * 1000, 2)  # in milliseconds

        if isinstance(result, dict):
            result['execution_time_ms'] = execution_time
        return result
    return wrapper

class DataLoader:
    def __init__(self, mongo_db, cassandra_session):
        self.mongo_db = mongo_db
        self.cassandra_session = cassandra_session

    def load_transactions_csv(self, csv_content):
        """Load transactions CSV to all 5 Cassandra tables"""
        try:
            # Parse CSV content
            csv_reader = csv.DictReader(io.StringIO(csv_content))
            transactions = []

            for row in csv_reader:
                # Parse date from YYYY-MM-DD format
                timestamp = datetime.strptime(row['timestamp'], '%Y-%m-%d')
                date_only = timestamp.date()

                transaction = {
                    'transaction_id': int(row['transaction_id']),
                    'timestamp': timestamp,
                    'date': date_only,
                    'customer_id': int(row['customer_id']),
                    'employee_id': int(row['employee_id']),
                    'total_amount': float(row['total_amount']),
                    'payment_method': row['payment_method']
                }
                transactions.append(transaction)

            # Prepare batch statements for each table
            batch_queries = {
                'transactions': "INSERT INTO transactions (transaction_id, timestamp, customer_id, employee_id, total_amount, payment_method) VALUES (?, ?, ?, ?, ?, ?)",
                'transactions_by_employee': "INSERT INTO transactions_by_employee (employee_id, timestamp, transaction_id, customer_id, total_amount, payment_method) VALUES (?, ?, ?, ?, ?, ?)",
                'transactions_by_payment': "INSERT INTO transactions_by_payment (payment_method, timestamp, transaction_id, customer_id, employee_id, total_amount) VALUES (?, ?, ?, ?, ?, ?)",
                'transactions_by_date': "INSERT INTO transactions_by_date (date, timestamp, transaction_id, customer_id, employee_id, total_amount, payment_method) VALUES (?, ?, ?, ?, ?, ?, ?)",
                'transactions_by_customer': "INSERT INTO transactions_by_customer (customer_id, timestamp, transaction_id, employee_id, total_amount, payment_method) VALUES (?, ?, ?, ?, ?, ?)"
            }

            # Insert to all tables
            total_inserted = 0
            for transaction in transactions:
                # 1. Main transactions table
                self.cassandra_session.execute(batch_queries['transactions'], [
                    transaction['transaction_id'],
                    transaction['timestamp'],
                    transaction['customer_id'],
                    transaction['employee_id'],
                    transaction['total_amount'],
                    transaction['payment_method']
                ])

                # 2. By employee table
                self.cassandra_session.execute(batch_queries['transactions_by_employee'], [
                    transaction['employee_id'],
                    transaction['timestamp'],
                    transaction['transaction_id'],
                    transaction['customer_id'],
                    transaction['total_amount'],
                    transaction['payment_method']
                ])

                # 3. By payment method table
                self.cassandra_session.execute(batch_queries['transactions_by_payment'], [
                    transaction['payment_method'],
                    transaction['timestamp'],
                    transaction['transaction_id'],
                    transaction['customer_id'],
                    transaction['employee_id'],
                    transaction['total_amount']
                ])

                # 4. By date table
                self.cassandra_session.execute(batch_queries['transactions_by_date'], [
                    transaction['date'],
                    transaction['timestamp'],
                    transaction['transaction_id'],
                    transaction['customer_id'],
                    transaction['employee_id'],
                    transaction['total_amount'],
                    transaction['payment_method']
                ])

                # 5. By customer table
                self.cassandra_session.execute(batch_queries['transactions_by_customer'], [
                    transaction['customer_id'],
                    transaction['timestamp'],
                    transaction['transaction_id'],
                    transaction['employee_id'],
                    transaction['total_amount'],
                    transaction['payment_method']
                ])

                total_inserted += 1

            return {
                'status': 'success',
                'message': f'Successfully loaded {total_inserted} transactions to all 5 Cassandra tables',
                'tables_populated': ['transactions', 'transactions_by_employee', 'transactions_by_payment', 'transactions_by_date', 'transactions_by_customer']
            }

        except Exception as e:
            logger.error(f"Error loading transactions CSV: {e}")
            return {'status': 'error', 'message': str(e)}

class QueryScenarios:
    def __init__(self, mongo_db, cassandra_session):
        self.mongo_db = mongo_db
        self.cassandra_session = cassandra_session

    # ================================
    # SCENARIO 1: WITHOUT OPTIMIZATION
    # ================================

    @measure_query_time
    def query_db1_no_optimization(self, payment_method='cash', start_date=None, end_date=None):
        """
        Query ke DB1 (Cassandra) TANPA optimasi
        Query: Total penjualan dengan payment method tertentu dalam periode
        Menggunakan main table dengan ALLOW FILTERING (sangat lambat)
        """
        try:
            if start_date and end_date:
                query = """
                SELECT COUNT(*) as total_transactions, SUM(total_amount) as total_revenue
                FROM transactions
                WHERE payment_method = ? AND timestamp >= ? AND timestamp <= ?
                ALLOW FILTERING
                """
                result = self.cassandra_session.execute(query, [payment_method, start_date, end_date])
            else:
                query = """
                SELECT COUNT(*) as total_transactions, SUM(total_amount) as total_revenue
                FROM transactions
                WHERE payment_method = ?
                ALLOW FILTERING
                """
                result = self.cassandra_session.execute(query, [payment_method])

            row = result.one()
            return {
                'scenario': 'without_optimization',
                'database': 'cassandra',
                'query_type': 'db1_payment_analysis',
                'payment_method': payment_method,
                'total_transactions': int(row.total_transactions) if row.total_transactions else 0,
                'total_revenue': float(row.total_revenue) if row.total_revenue else 0.0,
                'optimization_used': False,
                'table_used': 'transactions (main table)',
                'note': 'Using ALLOW FILTERING on main table - full table scan, very slow'
            }
        except Exception as e:
            logger.error(f"Error in query_db1_no_optimization: {e}")
            return {'error': str(e)}

    @measure_query_time
    def query_db2_no_optimization(self, position='Barista'):
        """
        Query ke DB2 (MongoDB) TANPA optimasi
        Query: Employee dengan performance rating tertinggi di posisi tertentu
        Menggunakan collection scan tanpa index
        """
        try:
            # Force collection scan - ignore indexes
            result = list(self.mongo_db.employees.find(
                {'position': position}
            ).hint({'$natural': 1}).sort('performance_rating', -1).limit(5))

            # Convert ObjectId to string for JSON serialization
            for emp in result:
                emp['_id'] = str(emp['_id'])

            return {
                'scenario': 'without_optimization',
                'database': 'mongodb',
                'query_type': 'db2_employee_performance',
                'position': position,
                'top_employees': result,
                'optimization_used': False,
                'collection_used': 'employees',
                'note': 'Using $natural hint - collection scan, ignoring indexes'
            }
        except Exception as e:
            logger.error(f"Error in query_db2_no_optimization: {e}")
            return {'error': str(e)}

    @measure_query_time
    def query_combined_no_optimization(self, position='Barista', start_date=None, end_date=None):
        """
        Query gabungan TANPA optimasi
        Query: Employee dengan performance rating tertinggi yang juga punya transaksi terbanyak
        Menggunakan N+1 queries dan full table scans
        """
        try:
            # Step 1: Get employees without index (MongoDB)
            employees = list(self.mongo_db.employees.find(
                {'position': position}
            ).hint({'$natural': 1}))  # Force collection scan

            # Step 2: For each employee, count transactions using main table with ALLOW FILTERING
            employee_stats = []
            for emp in employees:
                employee_id = emp['employee_id']

                # Very inefficient - full table scan for each employee
                if start_date and end_date:
                    query = """
                    SELECT COUNT(*) as transaction_count, SUM(total_amount) as total_amount
                    FROM transactions
                    WHERE employee_id = ? AND timestamp >= ? AND timestamp <= ?
                    ALLOW FILTERING
                    """
                    result = self.cassandra_session.execute(query, [employee_id, start_date, end_date])
                else:
                    query = """
                    SELECT COUNT(*) as transaction_count, SUM(total_amount) as total_amount
                    FROM transactions
                    WHERE employee_id = ?
                    ALLOW FILTERING
                    """
                    result = self.cassandra_session.execute(query, [employee_id])

                row = result.one()

                employee_stats.append({
                    'employee_id': employee_id,
                    'name': emp['name'],
                    'position': emp['position'],
                    'performance_rating': emp['performance_rating'],
                    'transaction_count': int(row.transaction_count) if row.transaction_count else 0,
                    'total_amount': float(row.total_amount) if row.total_amount else 0.0
                })

            # Sort by combined score (performance rating + transaction count)
            employee_stats.sort(key=lambda x: (x['performance_rating'], x['transaction_count']), reverse=True)

            return {
                'scenario': 'without_optimization',
                'database': 'both',
                'query_type': 'combined_employee_performance',
                'position': position,
                'employee_performance': employee_stats[:5],
                'optimization_used': False,
                'note': 'N+1 query problem: collection scan + multiple ALLOW FILTERING queries'
            }
        except Exception as e:
            logger.error(f"Error in query_combined_no_optimization: {e}")
            return {'error': str(e)}

    # ================================
    # SCENARIO 2: WITH OPTIMIZATION
    # ================================

    @measure_query_time
    def query_db1_with_optimization(self, payment_method='cash', start_date=None, end_date=None):
        """
        Query ke DB1 (Cassandra) DENGAN optimasi
        Query: Total penjualan dengan payment method tertentu dalam periode
        Menggunakan transactions_by_payment table (partition key optimization)
        """
        try:
            if start_date and end_date:
                query = """
                SELECT COUNT(*) as total_transactions, SUM(total_amount) as total_revenue
                FROM transactions_by_payment
                WHERE payment_method = ? AND timestamp >= ? AND timestamp <= ?
                """
                result = self.cassandra_session.execute(query, [payment_method, start_date, end_date])
            else:
                query = """
                SELECT COUNT(*) as total_transactions, SUM(total_amount) as total_revenue
                FROM transactions_by_payment
                WHERE payment_method = ?
                """
                result = self.cassandra_session.execute(query, [payment_method])

            row = result.one()
            return {
                'scenario': 'with_optimization',
                'database': 'cassandra',
                'query_type': 'db1_payment_analysis',
                'payment_method': payment_method,
                'total_transactions': int(row.total_transactions) if row.total_transactions else 0,
                'total_revenue': float(row.total_revenue) if row.total_revenue else 0.0,
                'optimization_used': True,
                'table_used': 'transactions_by_payment (partitioned)',
                'note': 'Using partition key (payment_method) for fast lookup'
            }
        except Exception as e:
            logger.error(f"Error in query_db1_with_optimization: {e}")
            return {'error': str(e)}

    @measure_query_time
    def query_db2_with_optimization(self, position='Barista'):
        """
        Query ke DB2 (MongoDB) DENGAN optimasi
        Query: Employee dengan performance rating tertinggi di posisi tertentu
        Menggunakan compound index (position + performance_rating)
        """
        try:
            # Use indexes efficiently
            result = list(self.mongo_db.employees.find(
                {'position': position}
            ).sort('performance_rating', -1).limit(5))

            # Convert ObjectId to string for JSON serialization
            for emp in result:
                emp['_id'] = str(emp['_id'])

            return {
                'scenario': 'with_optimization',
                'database': 'mongodb',
                'query_type': 'db2_employee_performance',
                'position': position,
                'top_employees': result,
                'optimization_used': True,
                'collection_used': 'employees',
                'note': 'Using compound index on position + performance_rating for efficient query'
            }
        except Exception as e:
            logger.error(f"Error in query_db2_with_optimization: {e}")
            return {'error': str(e)}

    @measure_query_time
    def query_combined_with_optimization(self, position='Barista', start_date=None, end_date=None):
        """
        Query gabungan DENGAN optimasi
        Query: Employee dengan performance rating tertinggi yang juga punya transaksi terbanyak
        Menggunakan optimized tables dan batch processing
        """
        try:
            # Step 1: Get employees using index (MongoDB)
            employees = list(self.mongo_db.employees.find(
                {'position': position}
            ).sort('performance_rating', -1))

            # Step 2: Get transaction stats using transactions_by_employee table (optimized)
            employee_stats = []
            for emp in employees:
                employee_id = emp['employee_id']

                # Use optimized table with employee_id as partition key
                if start_date and end_date:
                    query = """
                    SELECT COUNT(*) as transaction_count, SUM(total_amount) as total_amount
                    FROM transactions_by_employee
                    WHERE employee_id = ? AND timestamp >= ? AND timestamp <= ?
                    """
                    result = self.cassandra_session.execute(query, [employee_id, start_date, end_date])
                else:
                    query = """
                    SELECT COUNT(*) as transaction_count, SUM(total_amount) as total_amount
                    FROM transactions_by_employee
                    WHERE employee_id = ?
                    """
                    result = self.cassandra_session.execute(query, [employee_id])

                row = result.one()

                employee_stats.append({
                    'employee_id': employee_id,
                    'name': emp['name'],
                    'position': emp['position'],
                    'performance_rating': emp['performance_rating'],
                    'transaction_count': int(row.transaction_count) if row.transaction_count else 0,
                    'total_amount': float(row.total_amount) if row.total_amount else 0.0
                })

            # Sort by combined weighted score
            employee_stats.sort(
                key=lambda x: (x['performance_rating'] * 0.6 + (x['transaction_count'] / 100) * 0.4),
                reverse=True
            )

            return {
                'scenario': 'with_optimization',
                'database': 'both',
                'query_type': 'combined_employee_performance',
                'position': position,
                'employee_performance': employee_stats[:5],
                'optimization_used': True,
                'note': 'Using MongoDB indexes + Cassandra partition keys (employee_id) for fast lookup'
            }
        except Exception as e:
            logger.error(f"Error in query_combined_with_optimization: {e}")
            return {'error': str(e)}

# Global variables to store instances
query_scenarios = None
data_loader = None

def init_scenarios(mongo_db, cassandra_session):
    """Initialize scenarios and data loader with database connections"""
    global query_scenarios, data_loader
    query_scenarios = QueryScenarios(mongo_db, cassandra_session)
    data_loader = DataLoader(mongo_db, cassandra_session)

# ================================
# DATA LOADING ENDPOINTS
# ================================

@data_bp.route('/load-data', methods=['POST'])
def load_transactions_data():
    """Load transactions CSV data to Cassandra tables"""
    if not data_loader:
        return jsonify({'error': 'Database connections not initialized'}), 500

    try:
        # Check if file was uploaded
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400

        if not file.filename.endswith('.csv'):
            return jsonify({'error': 'File must be a CSV'}), 400

        # Read CSV content
        csv_content = file.read().decode('utf-8')

        # Load data to Cassandra
        result = data_loader.load_transactions_csv(csv_content)

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error in load_transactions_data: {e}")
        return jsonify({'error': str(e)}), 500

@data_bp.route('/data-status')
def data_status():
    """Check data status in both databases"""
    if not query_scenarios:
        return jsonify({'error': 'Database connections not initialized'}), 500

    try:
        status = {}

        # MongoDB data status
        status['mongodb'] = {
            'employees_count': query_scenarios.mongo_db.employees.count_documents({}),
            'menu_items_count': query_scenarios.mongo_db.menu_items.count_documents({})
        }

        # Cassandra data status
        cassandra_tables = ['transactions', 'transactions_by_employee', 'transactions_by_payment', 'transactions_by_date', 'transactions_by_customer']
        status['cassandra'] = {}

        for table in cassandra_tables:
            try:
                result = query_scenarios.cassandra_session.execute(f"SELECT COUNT(*) FROM {table}")
                status['cassandra'][table] = int(result.one().count)
            except Exception as e:
                status['cassandra'][table] = f"Error: {str(e)}"

        return jsonify(status)

    except Exception as e:
        logger.error(f"Error in data_status: {e}")
        return jsonify({'error': str(e)}), 500

# ================================
# SCENARIO TESTING ENDPOINTS
# ================================

@scenarios_bp.route('/compare-all')
def compare_all_scenarios():
    """Compare all query types with and without optimization"""
    if not query_scenarios:
        return jsonify({'error': 'Database connections not initialized'}), 500

    # Set default date range (last 30 days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    results = {
        'comparison_date': datetime.now().isoformat(),
        'date_range': {'start': start_date.isoformat(), 'end': end_date.isoformat()},
        'scenarios': {
            'without_optimization': {},
            'with_optimization': {}
        }
    }

    # Without optimization
    results['scenarios']['without_optimization']['db1_query'] = query_scenarios.query_db1_no_optimization('cash', start_date, end_date)
    results['scenarios']['without_optimization']['db2_query'] = query_scenarios.query_db2_no_optimization('Barista')
    results['scenarios']['without_optimization']['combined_query'] = query_scenarios.query_combined_no_optimization('Barista', start_date, end_date)

    # With optimization
    results['scenarios']['with_optimization']['db1_query'] = query_scenarios.query_db1_with_optimization('cash', start_date, end_date)
    results['scenarios']['with_optimization']['db2_query'] = query_scenarios.query_db2_with_optimization('Barista')
    results['scenarios']['with_optimization']['combined_query'] = query_scenarios.query_combined_with_optimization('Barista', start_date, end_date)

    # Calculate performance improvements
    improvements = {}
    for query_type in ['db1_query', 'db2_query', 'combined_query']:
        without_time = results['scenarios']['without_optimization'][query_type].get('execution_time_ms', 0)
        with_time = results['scenarios']['with_optimization'][query_type].get('execution_time_ms', 0)

        if without_time > 0:
            improvement_percent = round(((without_time - with_time) / without_time) * 100, 2)
            improvements[query_type] = {
                'without_optimization_ms': without_time,
                'with_optimization_ms': with_time,
                'improvement_percent': improvement_percent,
                'speedup_factor': round(without_time / with_time, 2) if with_time > 0 else 'infinite'
            }

    results['performance_improvements'] = improvements

    return jsonify(results)

@scenarios_bp.route('/db1/<scenario>')
def test_db1_scenario(scenario):
    """Test DB1 query with specific scenario"""
    if not query_scenarios:
        return jsonify({'error': 'Database connections not initialized'}), 500

    payment_method = request.args.get('payment_method', 'cash')

    if scenario == 'no-optimization':
        result = query_scenarios.query_db1_no_optimization(payment_method)
    elif scenario == 'with-optimization':
        result = query_scenarios.query_db1_with_optimization(payment_method)
    else:
        return jsonify({'error': 'Invalid scenario. Use: no-optimization or with-optimization'}), 400

    return jsonify(result)

@scenarios_bp.route('/db2/<scenario>')
def test_db2_scenario(scenario):
    """Test DB2 query with specific scenario"""
    if not query_scenarios:
        return jsonify({'error': 'Database connections not initialized'}), 500

    position = request.args.get('position', 'Barista')

    if scenario == 'no-optimization':
        result = query_scenarios.query_db2_no_optimization(position)
    elif scenario == 'with-optimization':
        result = query_scenarios.query_db2_with_optimization(position)
    else:
        return jsonify({'error': 'Invalid scenario. Use: no-optimization or with-optimization'}), 400

    return jsonify(result)

@scenarios_bp.route('/combined/<scenario>')
def test_combined_scenario(scenario):
    """Test combined query with specific scenario"""
    if not query_scenarios:
        return jsonify({'error': 'Database connections not initialized'}), 500

    position = request.args.get('position', 'Barista')

    if scenario == 'no-optimization':
        result = query_scenarios.query_combined_no_optimization(position)
    elif scenario == 'with-optimization':
        result = query_scenarios.query_combined_with_optimization(position)
    else:
        return jsonify({'error': 'Invalid scenario. Use: no-optimization or with-optimization'}), 400

    return jsonify(result)
