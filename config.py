# -*- coding: utf-8 -*-
"""
Configuration Module for Inventory Harmonization Pipeline
Contains all constants, column definitions, and configuration parameters.
"""

import numpy as np
import random
from datetime import datetime

# =============================================================================
# RANDOM SEEDS (for reproducibility)
# =============================================================================
RANDOM_SEED = 42
np.random.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

# =============================================================================
# DATA GENERATION PARAMETERS
# =============================================================================
NUM_ITEMS = 10
NUM_WAREHOUSES = 3
DAYS = 5
BASE_DATE = datetime(2025, 1, 1)

# Initial stock range
INITIAL_STOCK_MIN = 20
INITIAL_STOCK_MAX = 40

# Restock parameters
MAX_RESTOCKS_PER_DAY = 3
RESTOCK_QTY_MIN = 1
RESTOCK_QTY_MAX = 10
DAMAGED_UNITS_MAX = 2
LOGICAL_MAX_QUANTITY = 15

# Snapshot parameters
MAX_STOCK_CAPACITY = 100

# =============================================================================
# COLUMN DEFINITIONS
# =============================================================================
INVENTORY_SNAPSHOT_COLS = [
    "snapshot_id",
    "snapshot_timestamp",
    "item_id",
    "product_id",
    "warehouse_id",
    "current_quantity",
    "available_quantity",
    "reserved_quantity",
    "damaged_quantity",
    "expired_quantity",
    "max_stock_capacity",
]

RESTOCK_EVENTS_COLS = [
    "restock_event_id",
    "restock_timestamp",
    "item_id",
    "product_id",
    "warehouse_id",
    "employee_id",
    "quantity_added",
    "source_location",
    "restock_type",
    "damaged_units_reported",
    "logical_max_quantity",
]

# =============================================================================
# PRODUCT MASTER DATA
# =============================================================================
PRODUCT_MASTER_DATA = {
    "product_id": ["P001", "P002", "P003", "P004"],
    "product_name": ["Apple Juice 1L", "Banana Chips", "Oreo Biscuit", "Detergent Powder"],
    "brand": ["Tropicana", "Haldirams", "Oreo", "Surf Excel"],
    "category": ["Beverage", "Snacks", "Snacks", "Cleaning"]
}

# =============================================================================
# NAME VARIANTS (for simulating typos)
# =============================================================================
NAME_VARIANTS = {
    "Apple Juice 1L": ["Apple Jucie 1L", "Aple Juice", "Apple Juce 1000ml"],
    "Banana Chips": ["Bannana Chipz", "Banana Crisps", "Bananna Chps"],
    "Oreo Biscuit": ["Oreo Biskit", "Oriyo Biscuit", "Oreo Bisct"],
    "Detergent Powder": ["Deterjent Powder", "Detergent Pwd", "Dtrgent Powder"]
}

# =============================================================================
# INVALID DATA (for testing reconciliation)
# =============================================================================
BAD_PRODUCT_IDS = [None, "PX12", "0001", "P1", " "]

# =============================================================================
# WAREHOUSE AND SUPPLIER DATA
# =============================================================================
WAREHOUSES = [1, 2, 3]
SUPPLIERS = ["SupplierA", "SupplierB", "DistributorX", "WholesalerY"]
EMPLOYEE_ID_MIN = 101
EMPLOYEE_ID_MAX = 120

# =============================================================================
# SOURCE LOCATIONS AND RESTOCK TYPES
# =============================================================================
SOURCE_LOCATIONS = ["dock", "supplier_batch", "internal_transfer"]
RESTOCK_TYPES = ["manual", "automated"]

# =============================================================================
# RECONCILIATION PARAMETERS
# =============================================================================
SKU_PATTERN = r"^P\d{3}$"  # Pattern: P followed by 3 digits
FUZZY_MATCH_THRESHOLD = 75  # Minimum score for fuzzy matching (0-100)

# =============================================================================
# TIME PARAMETERS
# =============================================================================
WORK_HOUR_START = 6
WORK_HOUR_END = 23
SNAPSHOT_DELAY_MIN = 5  # minutes after restock
SNAPSHOT_DELAY_MAX = 60  # minutes after restock
