# -*- coding: utf-8 -*-
"""
Refund Data Generator Module
Generates customer details and refund transactions with intentional fraud patterns.
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from typing import Tuple


def generate_customer_details(num_customers: int = 100, seed: int = 42) -> pd.DataFrame:
    """
    Generate customer details dataset.
    
    Args:
        num_customers (int): Number of customers to generate
        seed (int): Random seed
        
    Returns:
        pd.DataFrame: Customer details
    """
    random.seed(seed)
    np.random.seed(seed)
    
    customers = []
    
    for i in range(1, num_customers + 1):
        customer_id = f'CUST_{i:04d}'
        
        # Generate customer details
        first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Robert', 'Lisa']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']
        
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        customer_name = f'{first_name} {last_name}'
        
        email = f'{first_name.lower()}.{last_name.lower()}{i}@email.com'
        phone = f'+1-{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}'
        
        # Registration date (last 2 years)
        days_ago = random.randint(0, 730)
        registration_date = datetime(2025, 1, 1) - timedelta(days=days_ago)
        
        # Customer tier
        tier_weights = [0.6, 0.3, 0.1]  # Bronze, Silver, Gold
        customer_tier = random.choices(['Bronze', 'Silver', 'Gold'], weights=tier_weights)[0]
        
        # Purchase history
        total_purchases = random.randint(1, 50) * 100  # $100 to $5000
        total_refunds = random.randint(0, int(total_purchases * 0.3))  # Up to 30% refund rate
        
        customers.append({
            'customer_id': customer_id,
            'customer_name': customer_name,
            'email': email,
            'phone': phone,
            'registration_date': registration_date,
            'customer_tier': customer_tier,
            'total_purchases': total_purchases,
            'total_refunds': total_refunds
        })
    
    return pd.DataFrame(customers)


def generate_refund_transactions(
    transactions_df: pd.DataFrame,
    line_items_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    fraud_rate: float = 0.20,
    seed: int = 42
) -> pd.DataFrame:
    """
    Generate refund transactions with intentional fraud patterns.
    
    Args:
        transactions_df (pd.DataFrame): Store sales header
        line_items_df (pd.DataFrame): Store sales line items
        customers_df (pd.DataFrame): Customer details
        fraud_rate (float): Percentage of fraudulent refunds (0-1)
        seed (int): Random seed
        
    Returns:
        pd.DataFrame: Refund transactions
    """
    random.seed(seed)
    np.random.seed(seed)
    
    # Select random transactions for refunds (20% refund rate)
    num_refunds = int(len(transactions_df) * 0.20)
    refund_txns = transactions_df.sample(n=num_refunds, random_state=seed)
    
    refunds = []
    refund_counter = 1
    
    # Determine which refunds will be fraudulent
    num_fraud = int(num_refunds * fraud_rate)
    fraud_indices = set(random.sample(range(num_refunds), num_fraud))
    
    for idx, (_, txn) in enumerate(refund_txns.iterrows()):
        is_fraud = idx in fraud_indices
        
        # Get line items for this transaction
        txn_items = line_items_df[line_items_df['transaction_id'] == txn['transaction_id']]
        
        if len(txn_items) == 0:
            continue
        
        # Select a random item to refund
        refund_item = txn_items.sample(n=1).iloc[0]
        
        # Calculate refund amount
        original_amount = refund_item['line_total']
        
        if is_fraud:
            # Fraudulent patterns
            fraud_type = random.choice(['amount', 'customer', 'date', 'payment', 'repeated'])
            
            if fraud_type == 'amount':
                # Refund > Original amount
                refund_amount = original_amount * random.uniform(1.1, 2.0)
            elif fraud_type == 'customer':
                # Mismatched customer ID
                refund_amount = original_amount
            elif fraud_type == 'date':
                # Refund outside allowed window (>30 days)
                refund_amount = original_amount
            else:
                refund_amount = original_amount
        else:
            # Legitimate refund
            refund_amount = original_amount * random.uniform(0.8, 1.0)
        
        # Refund timestamp (1-60 days after purchase)
        if is_fraud and random.random() < 0.3:
            # Some frauds are outside date window
            days_after = random.randint(31, 90)
        else:
            days_after = random.randint(1, 30)
        
        refund_timestamp = txn['transaction_timestamp'] + timedelta(days=days_after)
        
        # Customer ID
        if is_fraud and random.random() < 0.2:
            # Mismatched customer (fraud)
            customer_id = random.choice(customers_df['customer_id'].tolist())
        else:
            customer_id = txn['customer_id']
        
        # Payment mode
        payment_modes = ['Credit Card', 'Debit Card', 'Cash', 'Digital Wallet']
        original_payment = random.choice(payment_modes)
        
        if is_fraud and random.random() < 0.3:
            # Payment mismatch (fraud)
            refund_payment = random.choice([p for p in payment_modes if p != original_payment])
        else:
            refund_payment = original_payment
        
        # Refund reason
        reasons = ['Defective Product', 'Wrong Item', 'Changed Mind', 'Not as Described', 'Duplicate Order']
        refund_reason = random.choice(reasons)
        
        refunds.append({
            'refund_id': f'REF_{refund_counter:06d}',
            'transaction_id': txn['transaction_id'],
            'customer_id': customer_id,
            'original_customer_id': txn['customer_id'],
            'refund_timestamp': refund_timestamp,
            'purchase_timestamp': txn['transaction_timestamp'],
            'refund_amount': round(refund_amount, 2),
            'original_amount': round(original_amount, 2),
            'refund_reason': refund_reason,
            'payment_mode': refund_payment,
            'original_payment_mode': original_payment,
            'product_id': refund_item['product_id'],
            'approved_by': f'EMP_{random.randint(1, 20):03d}',
            'is_fraudulent': is_fraud  # Ground truth for testing
        })
        
        refund_counter += 1
    
    refunds_df = pd.DataFrame(refunds)
    
    # Add some high-frequency fraud (same customer, multiple refunds)
    high_freq_customers = random.sample(customers_df['customer_id'].tolist(), 5)
    
    for customer in high_freq_customers:
        for _ in range(random.randint(6, 10)):
            # Create additional fraudulent refunds
            base_refund = refunds_df.sample(n=1).iloc[0]
            
            refunds.append({
                'refund_id': f'REF_{refund_counter:06d}',
                'transaction_id': base_refund['transaction_id'],
                'customer_id': customer,
                'original_customer_id': base_refund['original_customer_id'],
                'refund_timestamp': datetime(2025, 1, 1) + timedelta(days=random.randint(0, 30)),
                'purchase_timestamp': base_refund['purchase_timestamp'],
                'refund_amount': round(random.uniform(50, 200), 2),
                'original_amount': round(random.uniform(50, 200), 2),
                'refund_reason': random.choice(reasons),
                'payment_mode': random.choice(payment_modes),
                'original_payment_mode': random.choice(payment_modes),
                'product_id': base_refund['product_id'],
                'approved_by': f'EMP_{random.randint(1, 20):03d}',
                'is_fraudulent': True
            })
            
            refund_counter += 1
    
    return pd.DataFrame(refunds)


def generate_complete_refund_data(
    transactions_df: pd.DataFrame,
    line_items_df: pd.DataFrame,
    num_customers: int = 100,
    fraud_rate: float = 0.20,
    seed: int = 42
) -> dict:
    """
    Generate complete refund dataset.
    
    Args:
        transactions_df (pd.DataFrame): Store sales header
        line_items_df (pd.DataFrame): Store sales line items
        num_customers (int): Number of customers
        fraud_rate (float): Fraud rate (0-1)
        seed (int): Random seed
        
    Returns:
        dict: Dictionary with customers and refunds DataFrames
    """
    print("Generating refund data...")
    
    # Generate customers
    customers_df = generate_customer_details(num_customers, seed)
    print(f"  ✓ Generated {len(customers_df)} customers")
    
    # Generate refunds
    refunds_df = generate_refund_transactions(
        transactions_df, line_items_df, customers_df, fraud_rate, seed
    )
    print(f"  ✓ Generated {len(refunds_df)} refund transactions")
    print(f"  ✓ Fraudulent refunds: {refunds_df['is_fraudulent'].sum()} ({refunds_df['is_fraudulent'].mean()*100:.1f}%)")
    
    return {
        'customer_details': customers_df,
        'refund_transactions': refunds_df
    }
