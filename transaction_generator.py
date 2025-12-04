# -*- coding: utf-8 -*-
"""
Transaction Generator Module
Generates synthetic transaction data for market basket analysis.
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from typing import Tuple


def generate_products_df(inventory_snapshot_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract unique products from inventory snapshot data.
    
    Args:
        inventory_snapshot_df (pd.DataFrame): Inventory snapshot data
        
    Returns:
        pd.DataFrame: Products DataFrame
    """
    products = inventory_snapshot_df[['product_id', 'item_id']].drop_duplicates()
    
    # Add product details
    product_names = {
        'P001': 'Apple Juice 1L',
        'P002': 'Banana Chips',
        'P003': 'Oreo Biscuit',
        'P004': 'Detergent Powder',
        'P005': 'Milk 1L',
        'P006': 'Bread Loaf',
        'P007': 'Eggs 12pk',
        'P008': 'Coffee 500g',
        'P009': 'Tea Bags 100pk',
        'P010': 'Sugar 1kg'
    }
    
    categories = {
        'P001': 'Beverage', 'P002': 'Snacks', 'P003': 'Snacks', 
        'P004': 'Cleaning', 'P005': 'Dairy', 'P006': 'Bakery',
        'P007': 'Dairy', 'P008': 'Beverage', 'P009': 'Beverage', 
        'P010': 'Grocery'
    }
    
    prices = {
        'P001': 3.99, 'P002': 2.49, 'P003': 1.99, 'P004': 5.99,
        'P005': 2.99, 'P006': 2.49, 'P007': 4.49, 'P008': 8.99,
        'P009': 3.49, 'P010': 2.99
    }
    
    products['product_name'] = products['product_id'].map(product_names)
    products['category'] = products['product_id'].map(categories)
    products['unit_price'] = products['product_id'].map(prices)
    
    # Fill any missing values
    products['product_name'].fillna('Unknown Product', inplace=True)
    products['category'].fillna('General', inplace=True)
    products['unit_price'].fillna(4.99, inplace=True)
    
    return products.reset_index(drop=True)


def generate_stores_df(inventory_snapshot_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract unique stores/warehouses from inventory data.
    
    Args:
        inventory_snapshot_df (pd.DataFrame): Inventory snapshot data
        
    Returns:
        pd.DataFrame: Stores DataFrame
    """
    stores = inventory_snapshot_df[['warehouse_id']].drop_duplicates()
    stores = stores.rename(columns={'warehouse_id': 'store_id'})
    
    # Add store details
    store_names = {1: 'Downtown Store', 2: 'Suburban Store', 3: 'Mall Store'}
    store_cities = {1: 'New York', 2: 'Los Angeles', 3: 'Chicago'}
    
    stores['store_name'] = stores['store_id'].map(store_names)
    stores['city'] = stores['store_id'].map(store_cities)
    
    return stores.reset_index(drop=True)


def generate_transactions(
    num_transactions: int = 200,
    stores_df: pd.DataFrame = None,
    start_date: datetime = None,
    seed: int = 42
) -> pd.DataFrame:
    """
    Generate synthetic transaction headers (store_sales_header).
    
    Args:
        num_transactions (int): Number of transactions to generate
        stores_df (pd.DataFrame): Stores DataFrame
        start_date (datetime): Start date for transactions
        seed (int): Random seed
        
    Returns:
        pd.DataFrame: Transaction headers
    """
    random.seed(seed)
    np.random.seed(seed)
    
    if start_date is None:
        start_date = datetime(2025, 1, 1)
    
    if stores_df is None:
        stores_df = pd.DataFrame({'store_id': [1, 2, 3]})
    
    transactions = []
    
    for i in range(num_transactions):
        transaction_id = f'TXN_{i+1:06d}'
        store_id = random.choice(stores_df['store_id'].tolist())
        
        # Generate timestamp (spread over 30 days)
        days_offset = random.randint(0, 29)
        hours_offset = random.randint(8, 20)  # Store hours 8am-8pm
        minutes_offset = random.randint(0, 59)
        
        transaction_timestamp = start_date + timedelta(
            days=days_offset, 
            hours=hours_offset, 
            minutes=minutes_offset
        )
        
        customer_id = f'CUST_{random.randint(1, 100):04d}'
        
        transactions.append({
            'transaction_id': transaction_id,
            'store_id': store_id,
            'transaction_timestamp': transaction_timestamp,
            'customer_id': customer_id,
            'total_amount': 0.0  # Will be calculated from line items
        })
    
    return pd.DataFrame(transactions)


def generate_line_items(
    transactions_df: pd.DataFrame,
    products_df: pd.DataFrame,
    min_items: int = 2,
    max_items: int = 8,
    seed: int = 42
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Generate synthetic line items for transactions with product affinities.
    
    Args:
        transactions_df (pd.DataFrame): Transaction headers
        products_df (pd.DataFrame): Products DataFrame
        min_items (int): Minimum items per basket
        max_items (int): Maximum items per basket
        seed (int): Random seed
        
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: (line_items_df, updated_transactions_df)
    """
    random.seed(seed)
    np.random.seed(seed)
    
    # Define product affinities (products commonly bought together)
    affinities = {
        'P001': ['P002', 'P003'],  # Juice with snacks
        'P005': ['P006', 'P007'],  # Milk with bread and eggs
        'P008': ['P009', 'P010'],  # Coffee with tea and sugar
        'P002': ['P003'],          # Chips with biscuits
        'P006': ['P007'],          # Bread with eggs
    }
    
    line_items = []
    line_item_counter = 1
    
    for _, txn in transactions_df.iterrows():
        transaction_id = txn['transaction_id']
        num_items = random.randint(min_items, max_items)
        
        # Select first product randomly
        selected_products = []
        first_product = random.choice(products_df['product_id'].tolist())
        selected_products.append(first_product)
        
        # Add related products based on affinities (70% chance)
        if first_product in affinities and random.random() < 0.7:
            related = random.choice(affinities[first_product])
            if related not in selected_products:
                selected_products.append(related)
        
        # Fill remaining items randomly
        while len(selected_products) < num_items:
            product = random.choice(products_df['product_id'].tolist())
            if product not in selected_products:
                selected_products.append(product)
        
        # Create line items
        transaction_total = 0.0
        
        for product_id in selected_products:
            product_info = products_df[products_df['product_id'] == product_id].iloc[0]
            quantity = random.randint(1, 3)
            unit_price = product_info['unit_price']
            line_total = quantity * unit_price
            transaction_total += line_total
            
            line_items.append({
                'line_item_id': f'LI_{line_item_counter:08d}',
                'transaction_id': transaction_id,
                'product_id': product_id,
                'product_name': product_info['product_name'],
                'quantity': quantity,
                'unit_price': unit_price,
                'line_total': round(line_total, 2)
            })
            
            line_item_counter += 1
        
        # Update transaction total
        transactions_df.loc[
            transactions_df['transaction_id'] == transaction_id, 
            'total_amount'
        ] = round(transaction_total, 2)
    
    line_items_df = pd.DataFrame(line_items)
    
    return line_items_df, transactions_df


def generate_complete_transaction_data(
    inventory_snapshot_df: pd.DataFrame,
    num_transactions: int = 200,
    seed: int = 42
) -> dict:
    """
    Generate complete transaction dataset from inventory data.
    
    Args:
        inventory_snapshot_df (pd.DataFrame): Inventory snapshot data
        num_transactions (int): Number of transactions to generate
        seed (int): Random seed
        
    Returns:
        dict: Dictionary containing all generated DataFrames
    """
    print("Generating transaction data...")
    
    # Generate products and stores
    products_df = generate_products_df(inventory_snapshot_df)
    stores_df = generate_stores_df(inventory_snapshot_df)
    
    print(f"  ✓ Generated {len(products_df)} products")
    print(f"  ✓ Generated {len(stores_df)} stores")
    
    # Generate transactions
    transactions_df = generate_transactions(num_transactions, stores_df, seed=seed)
    print(f"  ✓ Generated {len(transactions_df)} transactions")
    
    # Generate line items
    line_items_df, transactions_df = generate_line_items(
        transactions_df, products_df, seed=seed
    )
    print(f"  ✓ Generated {len(line_items_df)} line items")
    
    return {
        'products': products_df,
        'stores': stores_df,
        'store_sales_header': transactions_df,
        'store_sales_line_items': line_items_df
    }
