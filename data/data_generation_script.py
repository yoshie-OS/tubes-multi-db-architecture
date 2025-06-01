import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Setting up constants for larger dataset
START_DATE = datetime(2020, 1, 1)  # Extended period: 5+ years
END_DATE = datetime(2025, 5, 19)
RAMADAN_PERIODS = [
    (datetime(2020, 4, 24), datetime(2020, 5, 23)),
    (datetime(2021, 4, 13), datetime(2021, 5, 12)),
    (datetime(2022, 4, 2), datetime(2022, 5, 1)),
    (datetime(2023, 3, 23), datetime(2023, 4, 21)),
    (datetime(2024, 3, 11), datetime(2024, 4, 9)),
    (datetime(2025, 3, 1), datetime(2025, 3, 31))
]

def is_ramadan(date):
    return any(start <= date <= end for start, end in RAMADAN_PERIODS)

def format_date(date):
    return date.strftime('%Y-%m-%d %H:%M:%S')

# Expanded constants for larger dataset
CUSTOMER_COUNT = 2000  # More customers
EMPLOYEE_COUNT = 75    # Much more employees for 5+ years of operations
payment_methods = ['Cash', 'Debit Card', 'Credit Card', 'E-Wallet', 'QRIS', 'Bank Transfer']

# Generate comprehensive employee data
employee_names = [
    # Indonesian names for authenticity
    'Budi Santoso', 'Siti Rahma', 'Agus Prasetyo', 'Dewi Lestari', 'Eko Susanto',
    'Fitri Handayani', 'Ginanjar Prakoso', 'Hana Pertiwi', 'Irfan Hakim', 'Joko Widodo',
    'Kartini Putri', 'Luhut Binsar', 'Maya Indah', 'Ningsih Rahayu', 'Oki Setiana',
    'Putri Sari', 'Qomar Hadi', 'Rina Susanti', 'Samsul Bahri', 'Tuti Wulandari',
    'Umar Faruq', 'Vina Melati', 'Wahyu Utomo', 'Xenia Putri', 'Yudi Pranata',
    'Zahra Amalia', 'Andi Wijaya', 'Bunga Citra', 'Candra Kirana', 'Dika Pratama',
    'Elia Rahayu', 'Farel Prayoga', 'Gita Savitri', 'Hendra Gunawan', 'Indira Sari',
    'Jihan Aulia', 'Kevin Anggara', 'Laras Sekar', 'Mila Karmila', 'Nanda Pratiwi',
    'Omar Abdullah', 'Prita Mulyani', 'Quinsha Maharani', 'Rizki Ramadan', 'Sari Dewi',
    'Teguh Santoso', 'Ulfa Khoiriah', 'Victor Hugo', 'Wulan Sari', 'Ximena Putri',
    'Yoga Pratama', 'Zara Amira', 'Arief Rachman', 'Bella Safira', 'Ciko Hakim',
    'Diana Permata', 'Efan Pradana', 'Farah Diba', 'Gilang Ramadhan', 'Hesti Utami',
    'Ivan Setiawan', 'Jasmine Azzahra', 'Kenzo Pratama', 'Luna Maya', 'Malik Ibrahim',
    'Nadine Chandrawati', 'Oscar Lawalata', 'Poppy Bunga', 'Qiara Maharani', 'Raihan Malik',
    'Shinta Dewi', 'Tegar Septian', 'Ulum Bahri', 'Viola Cantika', 'Wisnu Wardana'
]

positions = ['Barista', 'Server', 'Kitchen Staff', 'Cashier', 'Supervisor', 'Manager', 'Trainee']
position_weights = [0.35, 0.2, 0.18, 0.12, 0.08, 0.05, 0.02]

def generate_employee_data():
    employees = []

    for i in range(1, EMPLOYEE_COUNT + 1):
        # Assign position based on weights
        position = random.choices(positions, weights=position_weights)[0]

        # Generate hire date (some employees started early, some joined later)
        # Early employees (2019-2021), recent hires (2022-2025)
        if i <= 25:  # Original staff
            hire_start = datetime(2019, 1, 1)
            hire_end = datetime(2021, 12, 31)
        elif i <= 50:  # Mid-period hires
            hire_start = datetime(2022, 1, 1)
            hire_end = datetime(2023, 12, 31)
        else:  # Recent hires
            hire_start = datetime(2024, 1, 1)
            hire_end = datetime(2025, 3, 1)

        hire_date = hire_start + timedelta(
            days=random.randint(0, (hire_end - hire_start).days)
        )

        # Determine employment status
        # Some employees left (resigned, terminated, etc.)
        is_active = True
        termination_date = None
        termination_reason = None

        # 15% chance of being terminated/resigned (realistic turnover)
        if random.random() < 0.15 and hire_date < datetime(2024, 12, 1):
            is_active = False
            # Termination date after hire date but before end of data
            term_start = hire_date + timedelta(days=90)  # At least 3 months employment
            term_end = min(datetime(2025, 5, 19), hire_date + timedelta(days=365*2))  # Max 2 years

            if term_start < term_end:
                termination_date = term_start + timedelta(
                    days=random.randint(0, (term_end - term_start).days)
                )

                termination_reasons = ['Resigned', 'Performance Issues', 'Relocated', 'Career Change', 'Contract End']
                termination_reason = random.choice(termination_reasons)

        # Performance rating (active employees generally higher)
        if is_active:
            performance_rating = round(random.uniform(3.2, 5.0), 1)
        else:
            performance_rating = round(random.uniform(2.5, 4.2), 1)  # Lower for terminated

        # Salary based on position and tenure
        salary_ranges = {
            'Trainee': (3500000, 4000000),
            'Barista': (4000000, 6000000),
            'Server': (3800000, 5500000),
            'Kitchen Staff': (4200000, 6200000),
            'Cashier': (4000000, 5800000),
            'Supervisor': (6500000, 9000000),
            'Manager': (9000000, 15000000)
        }

        base_salary = random.randint(*salary_ranges[position])

        # Tenure bonus (5% per year)
        years_worked = (datetime(2025, 5, 19) - hire_date).days / 365.25
        tenure_multiplier = 1 + (years_worked * 0.05)
        current_salary = int(base_salary * tenure_multiplier)

        employees.append({
            'employee_id': i,
            'name': employee_names[i-1] if i <= len(employee_names) else f'Employee {i}',
            'position': position,
            'hire_date': hire_date.strftime('%Y-%m-%d'),
            'is_active': is_active,
            'termination_date': termination_date.strftime('%Y-%m-%d') if termination_date else None,
            'termination_reason': termination_reason,
            'performance_rating': performance_rating,
            'current_salary': current_salary,
            'department': 'Operations' if position in ['Barista', 'Server', 'Kitchen Staff'] else 'Management'
        })

    return employees

employees_data = generate_employee_data()

# Generate more realistic employee distribution
positions = ['Barista', 'Server', 'Kitchen Staff', 'Cashier', 'Supervisor', 'Manager']
position_weights = [0.4, 0.25, 0.2, 0.1, 0.04, 0.01]  # Most are baristas

# Generate realistic menu items
menu_items = []
menu_id = 1

# Coffee items
coffee_items = [
    ("Kopi Tubruk", 25000), ("Kopi Susu", 28000), ("Americano", 32000),
    ("Cappuccino", 38000), ("Latte", 42000), ("Espresso", 30000),
    ("Kopi Gayo", 45000), ("Kopi Toraja", 48000), ("V60 Drip", 40000),
    ("Cold Brew", 35000), ("Affogato", 55000), ("Caramel Macchiato", 50000)
]

for name, base_price in coffee_items:
    menu_items.append({
        'menu_id': menu_id,
        'name': name,
        'category': 'Coffee',
        'price': base_price,
        'is_seasonal': False
    })
    menu_id += 1

# Tea items
tea_items = [
    ("Teh Tarik", 18000), ("Teh Melati", 15000), ("Earl Grey", 22000),
    ("Green Tea", 20000), ("Teh Jahe", 18000), ("Matcha Latte", 38000),
    ("Iced Tea", 15000), ("Thai Tea", 25000), ("Chamomile", 24000)
]

for name, base_price in tea_items:
    menu_items.append({
        'menu_id': menu_id,
        'name': name,
        'category': 'Tea',
        'price': base_price,
        'is_seasonal': False
    })
    menu_id += 1

# Cold Drinks
cold_items = [
    ("Fresh Orange", 22000), ("Lemon Squash", 20000), ("Iced Chocolate", 28000),
    ("Smoothie Bowl", 45000), ("Milkshake Vanilla", 35000), ("Coconut Water", 18000),
    ("Fruit Punch", 25000)
]

for name, base_price in cold_items:
    menu_items.append({
        'menu_id': menu_id,
        'name': name,
        'category': 'Cold Drinks',
        'price': base_price,
        'is_seasonal': False
    })
    menu_id += 1

# Hot Food
hot_food_items = [
    ("Nasi Goreng", 35000), ("Mie Ayam", 32000), ("Gado-Gado", 28000),
    ("Rendang Rice", 45000), ("Chicken Katsu", 42000), ("Beef Teriyaki", 48000),
    ("Nasi Gudeg", 38000), ("Soto Ayam", 30000), ("Bakso", 25000),
    ("Pasta Carbonara", 55000), ("Fried Rice Special", 40000)
]

for name, base_price in hot_food_items:
    menu_items.append({
        'menu_id': menu_id,
        'name': name,
        'category': 'Hot Food',
        'price': base_price,
        'is_seasonal': False
    })
    menu_id += 1

# Cold Food
cold_food_items = [
    ("Caesar Salad", 35000), ("Club Sandwich", 32000), ("Fruit Salad", 25000),
    ("Sushi Roll", 45000), ("Cold Pasta", 38000), ("Sandwich Tuna", 28000)
]

for name, base_price in cold_food_items:
    menu_items.append({
        'menu_id': menu_id,
        'name': name,
        'category': 'Cold Food',
        'price': base_price,
        'is_seasonal': False
    })
    menu_id += 1

# Desserts
dessert_items = [
    ("Chocolate Cake", 32000), ("Tiramisu", 38000), ("Ice Cream", 22000),
    ("Brownies", 25000), ("Cheesecake", 35000), ("Pudding", 20000),
    ("Donut", 18000), ("Croissant", 24000)
]

for name, base_price in dessert_items:
    menu_items.append({
        'menu_id': menu_id,
        'name': name,
        'category': 'Desserts',
        'price': base_price,
        'is_seasonal': False
    })
    menu_id += 1

# Snacks
snack_items = [
    ("French Fries", 18000), ("Onion Rings", 20000), ("Chicken Wings", 28000),
    ("Spring Rolls", 22000), ("Nachos", 25000), ("Garlic Bread", 16000),
    ("Potato Wedges", 20000)
]

for name, base_price in snack_items:
    menu_items.append({
        'menu_id': menu_id,
        'name': name,
        'category': 'Snacks',
        'price': base_price,
        'is_seasonal': False
    })
    menu_id += 1

# Add Ramadan special items
ramadan_items = [
    ("Kolak Pisang", 22000, "Desserts"), ("Es Timun Serut", 18000, "Cold Drinks"),
    ("Bubur Sumsum", 20000, "Desserts"), ("Es Kelapa Muda", 15000, "Cold Drinks"),
    ("Ketupat Sayur", 32000, "Hot Food"), ("Opor Ayam", 45000, "Hot Food")
]

for name, base_price, category in ramadan_items:
    menu_items.append({
        'menu_id': menu_id,
        'name': name,
        'category': category,
        'price': base_price,
        'is_seasonal': True
    })
    menu_id += 1

print(f"Generated {len(menu_items)} menu items")

# Create menu lookup for transactions
menu_by_category = {}
for item in menu_items:
    if item['category'] not in menu_by_category:
        menu_by_category[item['category']] = []
    menu_by_category[item['category']].append(item)

ramadan_specials = [item for item in menu_items if item['is_seasonal']]
regular_items = [item for item in menu_items if not item['is_seasonal']]

# Generate realistic transaction patterns
def get_hourly_distribution(is_ramadan_day=False, is_weekend=False):
    if is_ramadan_day:
        # Ramadan pattern: low during day, spike at iftar (18:00), busy evening
        return [0.5, 0.3, 0.2, 0.2, 0.3, 0.5,  # 0-5: very low
                2, 4, 6, 4, 3, 2,               # 6-11: morning modest
                3, 4, 2, 1, 1, 2,               # 12-17: afternoon low
                15, 12, 8, 6, 4, 2]             # 18-23: iftar rush
    elif is_weekend:
        # Weekend: later start, steady throughout day
        return [0.2, 0.1, 0.1, 0.2, 0.5, 1,    # 0-5: minimal
                3, 6, 8, 7, 6, 8,               # 6-11: slow morning
                10, 9, 7, 6, 7, 8,              # 12-17: steady afternoon
                9, 8, 6, 4, 3, 1]               # 18-23: evening decline
    else:
        # Weekday: clear morning and lunch peaks
        return [0.2, 0.1, 0.1, 0.2, 0.5, 1,    # 0-5: minimal
                5, 12, 15, 10, 6, 4,            # 6-11: morning rush
                12, 15, 8, 4, 3, 5,             # 12-17: lunch peak
                7, 6, 4, 3, 2, 1]               # 18-23: evening wind down

def generate_transaction_batch(date, target_transactions):
    """Generate transactions for a specific date"""
    is_ramadan_day = is_ramadan(date)
    is_weekend = date.weekday() >= 5

    hour_dist = get_hourly_distribution(is_ramadan_day, is_weekend)
    transactions = []

    for hour in range(24):
        # Calculate transactions for this hour
        hour_transactions = int(target_transactions * hour_dist[hour] / sum(hour_dist))

        for _ in range(hour_transactions):
            # Random minute and second
            transaction_time = date.replace(
                hour=hour,
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            )

            # Generate realistic transaction with actual menu items
            num_items = random.choices([1, 2, 3, 4, 5], weights=[0.4, 0.3, 0.15, 0.1, 0.05])[0]
            total_amount = 0
            ordered_items = []

            for _ in range(num_items):
                # Select menu category with realistic weights
                if is_ramadan_day and random.random() < 0.3:
                    # 30% chance of Ramadan special during Ramadan
                    selected_item = random.choice(ramadan_specials)
                else:
                    # Select from regular menu with category preferences
                    category_weights = {
                        'Coffee': 0.35, 'Tea': 0.15, 'Cold Drinks': 0.12,
                        'Hot Food': 0.2, 'Cold Food': 0.08, 'Desserts': 0.07, 'Snacks': 0.03
                    }

                    # Adjust weights based on time of day
                    if hour < 10:  # Morning - more coffee/tea
                        category_weights['Coffee'] = 0.5
                        category_weights['Tea'] = 0.25
                        category_weights['Hot Food'] = 0.15
                    elif 12 <= hour <= 14:  # Lunch - more food
                        category_weights['Hot Food'] = 0.4
                        category_weights['Cold Food'] = 0.2
                        category_weights['Coffee'] = 0.2
                    elif hour >= 17:  # Evening - more desserts/snacks
                        category_weights['Desserts'] = 0.15
                        category_weights['Snacks'] = 0.1

                    category = random.choices(
                        list(category_weights.keys()),
                        weights=list(category_weights.values())
                    )[0]

                    selected_item = random.choice(menu_by_category[category])

                # Apply some price variation (±10%)
                item_price = int(selected_item['price'] * random.uniform(0.9, 1.1))
                total_amount += item_price
                ordered_items.append(selected_item['menu_id'])

            # Employee selection with realistic constraints
            # Only assign active employees who were hired before this transaction date
            available_employees = [
                emp for emp in employees_data
                if datetime.strptime(emp['hire_date'], '%Y-%m-%d') <= transaction_time
                and (emp['is_active'] or
                     (emp['termination_date'] and
                      datetime.strptime(emp['termination_date'], '%Y-%m-%d') >= transaction_time))
            ]

            if not available_employees:
                # Fallback to any early employee
                available_employees = [emp for emp in employees_data if emp['employee_id'] <= 10]

            # Morning shift vs evening shift preference
            if hour < 14:
                # Morning shift - prefer baristas and kitchen staff
                morning_staff = [emp for emp in available_employees
                               if emp['position'] in ['Barista', 'Kitchen Staff', 'Manager']]
                if morning_staff:
                    available_employees = morning_staff
            else:
                # Evening shift - more diverse staff
                pass

            selected_employee = random.choice(available_employees)
            employee_id = selected_employee['employee_id']

            # Payment method with realistic distribution
            payment_weights = [0.25, 0.15, 0.15, 0.35, 0.08, 0.02]  # E-wallet most popular
            payment_method = random.choices(payment_methods, weights=payment_weights)[0]

            transactions.append({
                'transaction_id': len(transactions) + 1,  # Will be adjusted later
                'timestamp': format_date(transaction_time),
                'customer_id': random.randint(1, CUSTOMER_COUNT),
                'employee_id': employee_id,
                'total_amount': total_amount,
                'payment_method': payment_method,
                'menu_item_ids': ','.join(map(str, ordered_items))  # Store as comma-separated string
            })

    return transactions

# Calculate days and target transactions per day
total_days = (END_DATE - START_DATE).days + 1
target_transactions_per_day = 500000 // total_days

print(f"Generating {500000} transactions over {total_days} days")
print(f"Average ~{target_transactions_per_day} transactions per day")

# Generate all transactions
all_transactions = []
current_date = START_DATE
transaction_counter = 1

while current_date <= END_DATE and len(all_transactions) < 500000:
    # Vary daily transaction count for realism
    is_weekend = current_date.weekday() >= 5
    is_ramadan_day = is_ramadan(current_date)

    if is_weekend:
        daily_target = int(target_transactions_per_day * 1.3)  # 30% more on weekends
    elif is_ramadan_day:
        daily_target = int(target_transactions_per_day * 1.5)  # 50% more during Ramadan
    else:
        daily_target = target_transactions_per_day

    # Add some random variation (+/- 20%)
    daily_target = int(daily_target * random.uniform(0.8, 1.2))

    print(f"Generating {daily_target} transactions for {current_date.strftime('%Y-%m-%d')}")

    daily_transactions = generate_transaction_batch(current_date, daily_target)

    # Assign sequential transaction IDs
    for transaction in daily_transactions:
        transaction['transaction_id'] = transaction_counter
        transaction_counter += 1

    all_transactions.extend(daily_transactions)
    current_date += timedelta(days=1)

    # Stop if we've reached our target
    if len(all_transactions) >= 500000:
        break

# Trim to exactly 500,000 if we went over
all_transactions = all_transactions[:500000]

# Create DataFrame and save
transactions_df = pd.DataFrame(all_transactions)

# Sort by timestamp for realism
transactions_df = transactions_df.sort_values('timestamp').reset_index(drop=True)

# Update transaction_ids to be sequential after sorting
transactions_df['transaction_id'] = range(1, len(transactions_df) + 1)

# Save transactions, menu, and employee data
transactions_df.to_csv('transactions_500k.csv', index=False)

# Save menu data as JSON
import json
with open('menu.json', 'w') as f:
    json.dump(menu_items, f, indent=2)

# Save employee data as JSON
with open('employees.json', 'w') as f:
    json.dump(employees_data, f, indent=2)

# Also save employee data as CSV for easier analysis
employees_df = pd.DataFrame(employees_data)
employees_df.to_csv('employees.csv', index=False)

# Also create a transaction_items detail table for analysis
transaction_items = []
for _, transaction in transactions_df.iterrows():
    item_ids = transaction['menu_item_ids'].split(',')
    for item_id in item_ids:
        transaction_items.append({
            'transaction_id': transaction['transaction_id'],
            'menu_item_id': int(item_id),
            'timestamp': transaction['timestamp']
        })

transaction_items_df = pd.DataFrame(transaction_items)
transaction_items_df.to_csv('transaction_items_500k.csv', index=False)

# Print summary statistics
print(f"\nDataset generation complete!")
print(f"Total transactions: {len(transactions_df)}")
print(f"Total transaction items: {len(transaction_items_df)}")
print(f"Total menu items: {len(menu_items)}")
print(f"Total employees: {len(employees_data)}")
print(f"Active employees: {sum(1 for emp in employees_data if emp['is_active'])}")
print(f"Terminated employees: {sum(1 for emp in employees_data if not emp['is_active'])}")
print(f"Date range: {transactions_df['timestamp'].min()} to {transactions_df['timestamp'].max()}")
print(f"Unique customers: {transactions_df['customer_id'].nunique()}")
print(f"Unique employees in transactions: {transactions_df['employee_id'].nunique()}")
print(f"Average transaction amount: Rp {transactions_df['total_amount'].mean():,.0f}")
print(f"Total revenue: Rp {transactions_df['total_amount'].sum():,.0f}")

# Employee statistics
print(f"\nEmployee statistics:")
position_counts = pd.DataFrame(employees_data)['position'].value_counts()
for position, count in position_counts.items():
    print(f"{position}: {count}")

# Payment method distribution
print(f"\nPayment method distribution:")
print(transactions_df['payment_method'].value_counts())

# Most popular menu items
print(f"\nTop 10 most ordered items:")
item_counts = transaction_items_df['menu_item_id'].value_counts().head(10)
for menu_id, count in item_counts.items():
    item_name = next(item['name'] for item in menu_items if item['menu_id'] == menu_id)
    print(f"{item_name}: {count} orders")

# Monthly transaction counts
transactions_df['month'] = pd.to_datetime(transactions_df['timestamp']).dt.to_period('M')
print(f"\nMonthly transaction counts (last 12 months):")
print(transactions_df['month'].value_counts().sort_index().tail(12))

print(f"\nFiles saved:")
print(f"- transactions_500k.csv (main transaction data)")
print(f"- transaction_items_500k.csv (detailed item orders)")
print(f"- menu.json (menu catalog)")
print(f"- employees.json (employee data)")
print(f"- employees.csv (employee data in CSV format)")
