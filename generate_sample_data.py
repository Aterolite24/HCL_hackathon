# -*- coding: utf-8 -*-
"""
Sample Data Generator for Config-Driven Ingestion
Creates sample CSV, Excel, and JSON files for testing.
"""

import pandas as pd
import random
from datetime import datetime, timedelta
from pathlib import Path

# Set random seed for reproducibility
random.seed(42)

# Create data directory if it doesn't exist
Path("data").mkdir(exist_ok=True)

# ============================================================================
# Generate Inventory Snapshot CSV
# ============================================================================
print("Generating inventory_snapshot.csv...")

snapshot_data = []
for i in range(50):
    snapshot_data.append({
        'SnapshotID': f'SN_{i:04d}',
        'Timestamp': (datetime(2025, 1, 1) + timedelta(hours=i)).strftime('%Y-%m-%d %H:%M:%S'),
        'ItemID': random.randint(1, 10),
        'ProductID': f'P{random.randint(1, 10):03d}',
        'WarehouseID': random.randint(1, 3),
        'CurrentQty': random.randint(10, 100),
        'AvailableQty': random.randint(5, 95),
        'ReservedQty': random.randint(0, 10),
        'DamagedQty': random.randint(0, 5),
        'ExpiredQty': random.randint(0, 3),
        'MaxCapacity': 100
    })

snapshot_df = pd.DataFrame(snapshot_data)
snapshot_df.to_csv('data/inventory_snapshot.csv', index=False)
print(f"  ✓ Created {len(snapshot_df)} records")

# ============================================================================
# Generate Restock Events Excel
# ============================================================================
print("Generating restock_events.xlsx...")

restock_data = []
for i in range(30):
    restock_data.append({
        'EventID': f'RS_{i:04d}',
        'EventTimestamp': datetime(2025, 1, 1) + timedelta(hours=i*2),
        'ItemID': random.randint(1, 10),
        'ProductID': f'P{random.randint(1, 10):03d}',
        'WarehouseID': random.randint(1, 3),
        'EmployeeID': random.randint(101, 120),
        'QuantityAdded': random.randint(5, 20),
        'SourceLocation': random.choice(['dock', 'supplier_batch', 'internal_transfer']),
        'RestockType': random.choice(['manual', 'automated']),
        'DamagedUnits': random.randint(0, 2),
        'MaxQuantity': 15
    })

restock_df = pd.DataFrame(restock_data)
with pd.ExcelWriter('data/restock_events.xlsx', engine='openpyxl') as writer:
    restock_df.to_excel(writer, sheet_name='Restocks', index=False)
print(f"  ✓ Created {len(restock_df)} records")

# ============================================================================
# Generate Incoming Inventory JSON
# ============================================================================
print("Generating incoming_inventory.json...")

incoming_data = []
products = [
    {'name': 'Apple Juice 1L', 'brand': 'Tropicana', 'category': 'Beverage'},
    {'name': 'Banana Chips', 'brand': 'Haldirams', 'category': 'Snacks'},
    {'name': 'Oreo Biscuit', 'brand': 'Oreo', 'category': 'Snacks'},
    {'name': 'Detergent Powder', 'brand': 'Surf Excel', 'category': 'Cleaning'}
]

for i in range(20):
    product = random.choice(products)
    incoming_data.append({
        'IncomingID': f'INC_{i:04d}',
        'ItemID': random.randint(1, 10),
        'ProductID': f'P{random.randint(1, 10):03d}' if random.random() > 0.3 else None,
        'ItemName': product['name'],
        'WarehouseID': random.randint(1, 3),
        'ReceivedQty': random.randint(10, 50),
        'ReceivedTimestamp': (datetime(2025, 1, 1) + timedelta(days=i)).isoformat(),
        'Brand': product['brand'],
        'Category': product['category'],
        'SKU': f'SKU{random.randint(1000, 9999)}',
        'SupplierName': random.choice(['SupplierA', 'SupplierB', 'DistributorX'])
    })

incoming_df = pd.DataFrame(incoming_data)
incoming_df.to_json('data/incoming_inventory.json', orient='records', indent=2)
print(f"  ✓ Created {len(incoming_df)} records")

print("\n✓ All sample data files generated successfully!")
