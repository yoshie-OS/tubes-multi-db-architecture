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
                # Drop and recreate collections to remove any validation schemas
                try:
                    self.db_manager.mongo_db.employees.drop()
                    self.db_manager.mongo_db.menu_items.drop()
                    logger.info("   🗑️ Dropped existing collections with validation schemas")
                except Exception as e:
                    logger.info(f"   Collections didn't exist or couldn't be dropped: {e}")

                # Create fresh collections without validation
                self.db_manager.mongo_db.create_collection('employees')
                self.db_manager.mongo_db.create_collection('menu_items')
                logger.info("   ✨ Created fresh collections without validation")

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
                """,

                # NEW: Transaction items table for detailed menu analysis
                """
                CREATE TABLE IF NOT EXISTS transaction_items (
                    transaction_id int,
                    menu_item_id int,
                    timestamp timestamp,
                    employee_id int,
                    customer_id int,
                    PRIMARY KEY (transaction_id, menu_item_id)
                )
                """,

                # NEW: Optimized for menu item performance analysis
                """
                CREATE TABLE IF NOT EXISTS items_by_menu (
                    menu_item_id int,
                    timestamp timestamp,
                    transaction_id int,
                    employee_id int,
                    customer_id int,
                    PRIMARY KEY (menu_item_id, timestamp, transaction_id)
                ) WITH CLUSTERING ORDER BY (timestamp DESC, transaction_id ASC)
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

        # Initialize result dictionary early
        result = {
            'status': 'error',
            'message': 'Unknown error',
            'total_rows_in_csv': 0,
            'total_parsed': 0,
            'total_inserted': 0,
            'failed_inserts': 0,
            'tables_populated': [],
            'performance_note': ''
        }

        try:
            # Ensure schema exists
            if not self.create_cassandra_schema():
                result['message'] = 'Failed to create Cassandra schema'
                return result

            # Read and parse CSV
            if not os.path.exists(csv_file_path):
                result['message'] = f'CSV file not found: {csv_file_path}'
                return result

            # Check file size first
            file_size = os.path.getsize(csv_file_path)
            logger.info(f"   📄 CSV file size: {file_size:,} bytes")

            with open(csv_file_path, 'r') as file:
                csv_content = file.read()

            csv_reader = csv.DictReader(io.StringIO(csv_content))
            transactions = []

            # Show progress for large files
            row_count = 0
            for i, row in enumerate(csv_reader):
                row_count += 1
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
                        'payment_method': row['payment_method'].strip(),
                        'menu_item_ids': row.get('menu_item_ids', '')  # Keep raw menu_item_ids
                    }
                    transactions.append(transaction)

                    if (i + 1) % 5000 == 0:
                        logger.info(f"   ✅ Parsed {i + 1:,} transactions...")

                except Exception as e:
                    logger.warning(f"   Skipping row {i}: {e}")
                    continue

            result['total_rows_in_csv'] = row_count
            result['total_parsed'] = len(transactions)

            logger.info(f"   📊 Total rows in CSV: {row_count:,}")
            logger.info(f"   📊 Successfully parsed: {len(transactions):,} transactions")

            if len(transactions) == 0:
                result['message'] = 'No valid transactions found in CSV'
                return result

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
                """,

                # NEW: Transaction items table for menu analysis
                'transaction_items': """
                    INSERT INTO transaction_items (transaction_id, menu_item_id, timestamp, employee_id, customer_id)
                    VALUES (%(transaction_id)s, %(menu_item_id)s, %(timestamp)s, %(employee_id)s, %(customer_id)s)
                """,

                # NEW: Optimized for menu item analysis
                'items_by_menu': """
                    INSERT INTO items_by_menu (menu_item_id, timestamp, transaction_id, employee_id, customer_id)
                    VALUES (%(menu_item_id)s, %(timestamp)s, %(transaction_id)s, %(employee_id)s, %(customer_id)s)
                """
            }

            # Clear existing data
            logger.info("   🗑️  Clearing existing data...")
            for table_name in insert_queries.keys():
                try:
                    self.db_manager.cassandra_session.execute(f"TRUNCATE {table_name}")
                    logger.info(f"   ✅ Cleared {table_name}")
                except Exception as e:
                    logger.warning(f"   Could not truncate {table_name}: {e}")

            # Process transaction items from menu_item_ids column (FIXED LOGIC)
            transaction_items = []
            for transaction in transactions:
                menu_items_str = transaction.get('menu_item_ids', '').strip()
                if menu_items_str and menu_items_str not in ['', 'nan', 'null']:
                    try:
                        # Handle both quoted and unquoted comma-separated values
                        # Remove any quotes and split by comma
                        menu_items_str = menu_items_str.replace('"', '').replace("'", '')
                        menu_item_ids = [int(x.strip()) for x in menu_items_str.split(',') if x.strip()]

                        for menu_item_id in menu_item_ids:
                            transaction_items.append({
                                'transaction_id': transaction['transaction_id'],
                                'menu_item_id': menu_item_id,
                                'timestamp': transaction['timestamp'],
                                'employee_id': transaction['employee_id'],
                                'customer_id': transaction['customer_id']
                            })
                    except (ValueError, AttributeError) as e:
                        logger.warning(f"   Could not parse menu_item_ids '{menu_items_str}' for transaction {transaction['transaction_id']}: {e}")

            logger.info(f"   📊 Extracted {len(transaction_items):,} menu item records from transactions")

            # Insert data to transaction tables with batch processing
            total_inserted = 0
            failed_inserts = 0
            batch_size = 1000

            logger.info(f"   💾 Inserting {len(transactions):,} transactions to transaction tables...")

            for i in range(0, len(transactions), batch_size):
                batch = transactions[i:i + batch_size]
                batch_failed = 0

                for transaction in batch:
                    try:
                        # Insert to transaction tables (excluding item tables for now)
                        for table_name, query in insert_queries.items():
                            if table_name not in ['transaction_items', 'items_by_menu']:
                                # Remove menu_item_ids field for transaction tables
                                clean_transaction = {k: v for k, v in transaction.items() if k != 'menu_item_ids'}
                                self.db_manager.cassandra_session.execute(query, clean_transaction)

                        total_inserted += 1

                    except Exception as e:
                        batch_failed += 1
                        failed_inserts += 1
                        if failed_inserts <= 10:
                            logger.warning(f"   Failed insert for transaction {transaction['transaction_id']}: {e}")

                # Progress update
                progress = min(i + batch_size, len(transactions))
                logger.info(f"   💾 Progress: {progress:,}/{len(transactions):,} ({progress/len(transactions)*100:.1f}%)")

            # Insert transaction items if we extracted any
            items_inserted = 0
            items_failed = 0

            if transaction_items:
                logger.info(f"   💾 Inserting {len(transaction_items):,} transaction items...")

                for item in transaction_items:
                    try:
                        # Insert to transaction_items table
                        self.db_manager.cassandra_session.execute(insert_queries['transaction_items'], item)

                        # Insert to items_by_menu table
                        self.db_manager.cassandra_session.execute(insert_queries['items_by_menu'], item)

                        items_inserted += 1

                        if items_inserted % 25000 == 0:
                            logger.info(f"   💾 Items progress: {items_inserted:,}/{len(transaction_items):,}")

                    except Exception as e:
                        items_failed += 1
                        if items_failed <= 10:
                            logger.warning(f"   Failed item insert: {e}")

                logger.info(f"   ✅ Inserted {items_inserted:,} transaction items from menu_item_ids column")

            # Check for separate transaction_items.csv file
            transaction_items_file = csv_file_path.replace('transactions.csv', 'transaction_items.csv')
            if os.path.exists(transaction_items_file):
                logger.info(f"   📊 Found transaction_items.csv - loading additional menu item data...")

                with open(transaction_items_file, 'r') as file:
                    items_content = file.read()

                items_reader = csv.DictReader(io.StringIO(items_content))
                additional_items = []

                for i, row in enumerate(items_reader):
                    try:
                        timestamp = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S')

                        item = {
                            'transaction_id': int(row['transaction_id']),
                            'menu_item_id': int(row['menu_item_id']),
                            'timestamp': timestamp,
                            'employee_id': 0,  # Will be filled from transaction data
                            'customer_id': 0   # Will be filled from transaction data
                        }
                        additional_items.append(item)

                        if (i + 1) % 50000 == 0:
                            logger.info(f"   📊 Parsed {i + 1:,} additional transaction items...")

                    except Exception as e:
                        logger.warning(f"   Skipping item row {i}: {e}")
                        continue

                logger.info(f"   📊 Parsed {len(additional_items):,} additional transaction items")

                # Fill in employee_id and customer_id from transaction data
                transaction_lookup = {t['transaction_id']: t for t in transactions}

                for item in additional_items:
                    trans = transaction_lookup.get(item['transaction_id'])
                    if trans:
                        item['employee_id'] = trans['employee_id']
                        item['customer_id'] = trans['customer_id']

                # Insert additional transaction items
                additional_inserted = 0
                additional_failed = 0

                logger.info(f"   💾 Inserting {len(additional_items):,} additional transaction items...")

                for item in additional_items:
                    try:
                        # Insert to transaction_items table
                        self.db_manager.cassandra_session.execute(insert_queries['transaction_items'], item)

                        # Insert to items_by_menu table
                        self.db_manager.cassandra_session.execute(insert_queries['items_by_menu'], item)

                        additional_inserted += 1

                        if additional_inserted % 25000 == 0:
                            logger.info(f"   💾 Additional items progress: {additional_inserted:,}/{len(additional_items):,}")

                    except Exception as e:
                        additional_failed += 1
                        if additional_failed <= 10:
                            logger.warning(f"   Failed additional item insert: {e}")

                logger.info(f"   ✅ Inserted {additional_inserted:,} additional transaction items")
                items_inserted += additional_inserted
                items_failed += additional_failed

            # Update result
            result.update({
                'status': 'success',
                'total_inserted': total_inserted,
                'failed_inserts': failed_inserts,
                'transaction_items_inserted': items_inserted,
                'transaction_items_failed': items_failed,
                'tables_populated': list(insert_queries.keys()),
                'performance_note': f'With {total_inserted:,} transactions and {items_inserted:,} menu items, you should see significant optimization differences!'
            })

            logger.info(f"   🎉 Successfully loaded {total_inserted:,} transactions and {items_inserted:,} menu items to {len(insert_queries)} tables")
            if failed_inserts > 0:
                logger.warning(f"   ⚠️ {failed_inserts} transaction insert failures")
            if items_failed > 0:
                logger.warning(f"   ⚠️ {items_failed} item insert failures")

            return result

        except Exception as e:
            logger.error(f"❌ Cassandra data loading failed: {e}")
            result['message'] = str(e)
            return result

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
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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
