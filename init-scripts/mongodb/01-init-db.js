# src/data_loaders/data_loader.py
import os
import json
import csv
import io
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path

logger = logging.getLogger(__name__)

class DataLoader:
    """
    Data loading utilities for MongoDB and Cassandra databases.
    Handles sample data initialization for demo purposes.
    """

    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.project_root = Path(__file__).parent.parent.parent

    def load_mongodb_data(self) -> Dict[str, Any]:
        """Load employees and menu data into MongoDB"""
        logger.info("📥 Loading MongoDB data...")

        results = {
            'employees': {'status': 'not_attempted'},
            'menu_items': {'status': 'not_attempted'}
        }

        try:
            # Load employees
            employees_file = self.project_root / 'data' / 'employees.json'
            if employees_file.exists():
                with open(employees_file, 'r') as f:
                    employees_data = json.load(f)

                # Clear existing data
                self.db_manager.mongo_db.employees.delete_many({})

                # Insert new data
                if employees_data:
                    result = self.db_manager.mongo_db.employees.insert_many(employees_data)
                    results['employees'] = {
                        'status': 'success',
                        'inserted_count': len(result.inserted_ids)
                    }
                    logger.info(f"   ✅ Loaded {len(result.inserted_ids)} employees")
                else:
                    results['employees'] = {'status': 'error', 'message': 'No employee data found'}
            else:
                results['employees'] = {'status': 'error', 'message': 'employees.json not found'}

            # Load menu items
            menu_file = self.project_root / 'data' / 'menu.json'
            if menu_file.exists():
                with open(menu_file, 'r') as f:
                    menu_data = json.load(f)

                # Clear existing data
                self.db_manager.mongo_db.menu_items.delete_many({})

                # Insert new data
                if menu_data:
                    result = self.db_manager.mongo_db.menu_items.insert_many(menu_data)
                    results['menu_items'] = {
                        'status': 'success',
                        'inserted_count': len(result.inserted_ids)
                    }
                    logger.info(f"   ✅ Loaded {len(result.inserted_ids)} menu items")
                else:
                    results['menu_items'] = {'status': 'error', 'message': 'No menu data found'}
            else:
                results['menu_items'] = {'status': 'error', 'message': 'menu.json not found'}

            return results

        except Exception as e:
            logger.error(f"❌ MongoDB data loading failed: {e}")
            return {'status': 'error', 'message': str(e)}

    def create_cassandra_schema(self) -> bool:
        """Create Cassandra keyspace and tables if they don't exist"""
        logger.info("🏛️ Setting up Cassandra schema...")

        try:
            # Create keyspace
            keyspace_query = """
                CREATE KEYSPACE IF NOT EXISTS cafe_analytics
                WITH REPLICATION = {
                    'class': 'SimpleStrategy',
                    'replication_factor': 1
                }
            """
            self.db_manager.cassandra_session.execute(keyspace_query)

            # Use keyspace
            self.db_manager.cassandra_session.set_keyspace('cafe_analytics')

            # Create tables
            table_queries = [
                # Main transactions table
                """
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id int PRIMARY KEY,
                    timestamp timestamp,
                    customer_id int,
                    employee_id int,
                    total_amount decimal,
                    payment_method text
                )
                """,

                # Optimized table for employee queries
                """
                CREATE TABLE IF NOT EXISTS transactions_by_employee (
                    employee_id int,
                    timestamp timestamp,
                    transaction_id int,
                    customer_id int,
                    total_amount decimal,
                    payment_method text,
                    PRIMARY KEY (employee_id, timestamp, transaction_id)
                )
                """,

                # Optimized table for payment method queries
                """
                CREATE TABLE IF NOT EXISTS transactions_by_payment (
                    payment_method text,
                    timestamp timestamp,
                    transaction_id int,
                    customer_id int,
                    employee_id int,
                    total_amount decimal,
                    PRIMARY KEY (payment_method, timestamp, transaction_id)
                )
                """,

                # Optimized table for date queries
                """
                CREATE TABLE IF NOT EXISTS transactions_by_date (
                    date date,
                    timestamp timestamp,
                    transaction_id int,
                    customer_id int,
                    employee_id int,
                    total_amount decimal,
                    payment_method text,
                    PRIMARY KEY (date, timestamp, transaction_id)
                )
                """,

                # Optimized table for customer queries
                """
                CREATE TABLE IF NOT EXISTS transactions_by_customer (
                    customer_id int,
                    timestamp timestamp,
                    transaction_id int,
                    employee_id int,
                    total_amount decimal,
                    payment_method text,
                    PRIMARY KEY (customer_id, timestamp, transaction_id)
                )
                """
            ]

            for query in table_queries:
                self.db_manager.cassandra_session.execute(query)

            logger.info("   ✅ Cassandra schema created successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Cassandra schema creation failed: {e}")
            return False

    def load_cassandra_transactions(self, csv_file_path: str) -> Dict[str, Any]:
        """Load transaction data into all Cassandra tables"""
        logger.info(f"📥 Loading Cassandra transactions from {csv_file_path}...")

        try:
            # Ensure schema exists
            if not self.create_cassandra_schema():
                return {'status': 'error', 'message': 'Failed to create Cassandra schema'}

            # Read and parse CSV
            if not os.path.exists(csv_file_path):
                return {'status': 'error', 'message': f'CSV file not found: {csv_file_path}'}

            with open(csv_file_path, 'r') as file:
                csv_content = file.read()

            csv_reader = csv.DictReader(io.StringIO(csv_content))
            transactions = []

            for i, row in enumerate(csv_reader):
                try:
                    timestamp = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S')
                    date_only = timestamp.date()

                    transaction = {
                        'transaction_id': int(row['transaction_id']),
                        'timestamp': timestamp,
                        'date': date_only,
                        'customer_id': int(row['customer_id']),
                        'employee_id': int(row['employee_id']),
                        'total_amount': Decimal(str(row['total_amount'])),
                        'payment_method': row['payment_method'].strip()
                    }
                    transactions.append(transaction)

                    if (i + 1) % 1000 == 0:
                        logger.info(f"   Parsed {i + 1} transactions...")

                except Exception as e:
                    logger.warning(f"   Skipping row {i}: {e}")
                    continue

            logger.info(f"   📊 Parsed {len(transactions):,} transactions")

            # Prepare INSERT statements for all tables
            insert_queries = {
                'transactions': """
                    INSERT INTO transactions (transaction_id, timestamp, customer_id, employee_id, total_amount, payment_method)
                    VALUES (%(transaction_id)s, %(timestamp)s, %(customer_id)s, %(employee_id)s, %(total_amount)s, %(payment_method)s)
                """,

                'transactions_by_employee': """
                    INSERT INTO transactions_by_employee (employee_id, timestamp, transaction_id, customer_id, total_amount, payment_method)
                    VALUES (%(employee_id)s, %(timestamp)s, %(transaction_id)s, %(customer_id)s, %(total_amount)s, %(payment_method)s)
                """,

                'transactions_by_payment': """
                    INSERT INTO transactions_by_payment (payment_method, timestamp, transaction_id, customer_id, employee_id, total_amount)
                    VALUES (%(payment_method)s, %(timestamp)s, %(transaction_id)s, %(customer_id)s, %(employee_id)s, %(total_amount)s)
                """,

                'transactions_by_date': """
                    INSERT INTO transactions_by_date (date, timestamp, transaction_id, customer_id, employee_id, total_amount, payment_method)
                    VALUES (%(date)s, %(timestamp)s, %(transaction_id)s, %(customer_id)s, %(employee_id)s, %(total_amount)s, %(payment_method)s)
                """,

                'transactions_by_customer': """
                    INSERT INTO transactions_by_customer (customer_id, timestamp, transaction_id, employee_id, total_amount, payment_method)
                    VALUES (%(customer_id)s, %(timestamp)s, %(transaction_id)s, %(employee_id)s, %(total_amount)s, %(payment_method)s)
                """
            }

            # Clear existing data
            for table_name in insert_queries.keys():
                try:
                    self.db_manager.cassandra_session.execute(f"TRUNCATE {table_name}")
                except Exception as e:
                    logger.warning(f"   Could not truncate {table_name}: {e}")

            # Insert data to all tables
            total_inserted = 0
            failed_inserts = 0

            logger.info("   💾 Inserting data to all tables...")

            for transaction in transactions:
                try:
                    # Insert to all tables simultaneously
                    for table_name, query in insert_queries.items():
                        self.db_manager.cassandra_session.execute(query, transaction)

                    total_inserted += 1

                    if total_inserted % 1000 == 0:
                        logger.info(f"   💾 Inserted {total_inserted:,} transactions...")

                except Exception as e:
                    failed_inserts += 1
                    if failed_inserts <= 5:  # Only log first 5 errors
                        logger.warning(f"   Failed insert for transaction {transaction['transaction_id']}: {e}")

            result = {
                'status': 'success',
                'total_inserted': total_inserted,
                'failed_inserts': failed_inserts,
                'tables_populated': list(insert_queries.keys())
            }

            logger.info(f"   ✅ Successfully loaded {total_inserted:,} transactions to {len(insert_queries)} tables")
            if failed_inserts > 0:
                logger.warning(f"   ⚠️ {failed_inserts} failed inserts")

            return result

        except Exception as e:
            logger.error(f"❌ Cassandra data loading failed: {e}")
            return {'status': 'error', 'message': str(e)}

    def load_all_sample_data(self) -> Dict[str, Any]:
        """Load all sample data into both databases"""
        logger.info("📥 Loading all sample data...")

        results = {
            'mongodb': self.load_mongodb_data(),
            'cassandra': {'status': 'not_attempted'}
        }

        # Look for transactions CSV
        csv_paths = [
            self.project_root / 'data' / 'transactions.csv',
            Path('transactions.csv'),
            Path('../data/transactions.csv')
        ]

        csv_file = None
        for path in csv_paths:
            if path.exists():
                csv_file = str(path)
                break

        if csv_file:
            results['cassandra'] = self.load_cassandra_transactions(csv_file)
        else:
            results['cassandra'] = {
                'status': 'error',
                'message': 'transactions.csv not found in expected locations'
            }

        return results

    def get_data_summary(self) -> Dict[str, Any]:
        """Get summary of loaded data"""
        try:
            counts = self.db_manager.get_data_counts()

            summary = {
                'mongodb': {
                    'employees': counts.get('mongodb', {}).get('employees', 0),
                    'menu_items': counts.get('mongodb', {}).get('menu_items', 0)
                },
                'cassandra': {}
            }

            # Get Cassandra counts
            cassandra_counts = counts.get('cassandra', {})
            for table_name, count in cassandra_counts.items():
                if isinstance(count, int):
                    summary['cassandra'][table_name] = count

            return summary

        except Exception as e:
            logger.error(f"Error getting data summary: {e}")
            return {'error': str(e)}


# Example usage
if __name__ == "__main__":
    from core.database_manager import DatabaseManager

    # Test data loader
    db_manager = DatabaseManager()

    if db_manager.connect_all():
        loader = DataLoader(db_manager)

        print("📥 Testing data loader...")
        results = loader.load_all_sample_data()

        print("\nResults:")
        for db, result in results.items():
            print(f"  {db}: {result}")

        print("\nData summary:")
        summary = loader.get_data_summary()
        for db, data in summary.items():
            print(f"  {db}: {data}")

    db_manager.close_all_connections()
