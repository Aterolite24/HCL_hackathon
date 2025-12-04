# -*- coding: utf-8 -*-
"""
Data Generator Module for Inventory Harmonization Pipeline
Generates synthetic inventory data including snapshots, restock events, and incoming inventory.
"""

import pandas as pd
import random
from datetime import datetime, timedelta
from config import *


def random_timestamp(day_index, base_date=BASE_DATE):
    """
    Generate a random timestamp within a given day.
    
    Args:
        day_index (int): Day offset from base_date
        base_date (datetime): Starting date for generation
        
    Returns:
        datetime: Random timestamp within the specified day
    """
    start = base_date + timedelta(days=day_index)
    hour = random.randint(WORK_HOUR_START, WORK_HOUR_END)
    minute = random.randint(0, 59)
    return start.replace(hour=hour, minute=minute, second=0)


def generate_inventory_data():
    """
    Generate inventory snapshots and restock events.
    
    Returns:
        tuple: (inventory_snapshot_df, restock_events_df)
    """
    snapshot_records = []
    restock_records = []
    
    for item in range(1, NUM_ITEMS + 1):
        for wh in range(1, NUM_WAREHOUSES + 1):
            # Initialize starting quantity
            prev_quantity = random.randint(INITIAL_STOCK_MIN, INITIAL_STOCK_MAX)
            last_restock_ts = None
            
            for day in range(DAYS):
                # Generate random number of restocks for this day
                num_restocks_today = random.randint(0, MAX_RESTOCKS_PER_DAY)
                
                total_restock_added = 0
                restock_timestamps = []
                
                # Generate restock events
                for _ in range(num_restocks_today):
                    restock_ts = random_timestamp(day)
                    restock_qty = random.randint(RESTOCK_QTY_MIN, RESTOCK_QTY_MAX)
                    damaged_units = random.randint(0, DAMAGED_UNITS_MAX)
                    
                    restock_records.append({
                        "restock_event_id": f"RS_{item}_{wh}_{day}_{random.randint(1000, 9999)}",
                        "restock_timestamp": restock_ts,
                        "item_id": item,
                        "product_id": f"P{item:03d}",
                        "warehouse_id": wh,
                        "employee_id": random.randint(EMPLOYEE_ID_MIN, EMPLOYEE_ID_MAX),
                        "quantity_added": restock_qty,
                        "source_location": random.choice(SOURCE_LOCATIONS),
                        "restock_type": random.choice(RESTOCK_TYPES),
                        "damaged_units_reported": damaged_units,
                        "logical_max_quantity": LOGICAL_MAX_QUANTITY
                    })
                    
                    total_restock_added += restock_qty
                    restock_timestamps.append(restock_ts)
                
                # Determine snapshot timestamp
                if restock_timestamps:
                    snapshot_ts = max(restock_timestamps) + timedelta(
                        minutes=random.randint(SNAPSHOT_DELAY_MIN, SNAPSHOT_DELAY_MAX)
                    )
                    last_restock_ts = max(restock_timestamps)
                else:
                    snapshot_ts = random_timestamp(day)
                
                # Calculate quantities
                damaged = random.randint(0, 3)
                expired = random.randint(0, 1)
                
                new_quantity = prev_quantity + total_restock_added - damaged - expired
                new_quantity = max(new_quantity, 0)
                
                # Create snapshot record
                snapshot_records.append({
                    "snapshot_id": f"SN_{item}_{wh}_{day}",
                    "snapshot_timestamp": snapshot_ts,
                    "item_id": item,
                    "product_id": f"P{item:03d}",
                    "warehouse_id": wh,
                    "current_quantity": new_quantity,
                    "available_quantity": new_quantity - random.randint(0, 2),
                    "reserved_quantity": random.randint(0, 3),
                    "damaged_quantity": damaged,
                    "expired_quantity": expired,
                    "max_stock_capacity": MAX_STOCK_CAPACITY,
                    "last_restock_timestamp": last_restock_ts
                })
                
                prev_quantity = new_quantity
    
    # Create DataFrames
    inventory_snapshot_df = pd.DataFrame(snapshot_records).sort_values("snapshot_timestamp")
    restock_events_df = pd.DataFrame(restock_records).sort_values("restock_timestamp")
    
    inventory_snapshot_df.reset_index(drop=True, inplace=True)
    restock_events_df.reset_index(drop=True, inplace=True)
    
    return inventory_snapshot_df, restock_events_df


def generate_incoming_inventory(product_master):
    """
    Generate incoming inventory data with intentional errors for reconciliation testing.
    
    Args:
        product_master (pd.DataFrame): Product master reference data
        
    Returns:
        pd.DataFrame: Incoming inventory with errors
    """
    incoming_records = []
    
    for idx, row in product_master.iterrows():
        true_name = row["product_name"]
        variants = NAME_VARIANTS[true_name]
        
        # Generate 5 entries per product
        for _ in range(5):
            incoming_records.append({
                "incoming_id": f"INC_{idx}_{random.randint(1000, 9999)}",
                "item_id": idx + 1,
                "product_id": random.choice(BAD_PRODUCT_IDS),  # Intentionally wrong/missing
                "item_name": random.choice(variants),  # Typo in name
                "warehouse_id": random.choice(WAREHOUSES),
                "received_quantity": random.randint(5, 30),
                "received_timestamp": BASE_DATE + timedelta(days=random.randint(0, DAYS)),
                "brand": row["brand"],
                "category": row["category"],
                "sku_raw": random.choice([f"P00{idx+1}", f"XX{idx+1}", f"SKU{idx+1}", None]),
                "supplier_name": random.choice(SUPPLIERS)
            })
    
    return pd.DataFrame(incoming_records)


def create_product_master():
    """
    Create the product master DataFrame.
    
    Returns:
        pd.DataFrame: Product master data
    """
    product_master = pd.DataFrame(PRODUCT_MASTER_DATA)
    product_master["sku_pattern"] = SKU_PATTERN
    return product_master
