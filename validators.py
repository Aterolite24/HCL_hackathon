# -*- coding: utf-8 -*-
"""
Validators Module for Inventory Harmonization Pipeline
Contains all data validation and quality check functions.
"""

import pandas as pd


def check_negative_stock(inventory_snapshot_df):
    """
    Identify records with negative stock levels.
    
    Args:
        inventory_snapshot_df (pd.DataFrame): Inventory snapshot data
        
    Returns:
        pd.DataFrame: Records with negative current_quantity
    """
    return inventory_snapshot_df[
        inventory_snapshot_df["current_quantity"] < 0
    ].copy()


def check_product_id_mismatch(restock_events_df, inventory_snapshot_df):
    """
    Find product ID mismatches between restock events and inventory snapshots.
    
    Args:
        restock_events_df (pd.DataFrame): Restock events data
        inventory_snapshot_df (pd.DataFrame): Inventory snapshot data
        
    Returns:
        pd.DataFrame: Records with mismatched product IDs
    """
    merged_pid = pd.merge(
        restock_events_df,
        inventory_snapshot_df,
        on=["item_id", "warehouse_id"],
        suffixes=("_rs", "_sn")
    )
    
    return merged_pid[
        merged_pid["product_id_rs"] != merged_pid["product_id_sn"]
    ].copy()


def check_duplicates(inventory_snapshot_df):
    """
    Detect duplicate records based on item_id, warehouse_id, and snapshot_timestamp.
    
    Args:
        inventory_snapshot_df (pd.DataFrame): Inventory snapshot data
        
    Returns:
        pd.DataFrame: Duplicate records
    """
    return inventory_snapshot_df[
        inventory_snapshot_df.duplicated(
            subset=["item_id", "warehouse_id", "snapshot_timestamp"],
            keep=False
        )
    ].copy()


def check_restock_exceeded(restock_events_df):
    """
    Validate that restock quantities don't exceed logical maximum.
    
    Args:
        restock_events_df (pd.DataFrame): Restock events data
        
    Returns:
        pd.DataFrame: Records where quantity_added exceeds logical_max_quantity
    """
    return restock_events_df[
        restock_events_df["quantity_added"] > restock_events_df["logical_max_quantity"]
    ].copy()


def create_quarantine_inventory(neg_stock, pid_mismatch, duplicates, restock_exceeded):
    """
    Aggregate all validation failures into a quarantine inventory.
    
    Args:
        neg_stock (pd.DataFrame): Negative stock records
        pid_mismatch (pd.DataFrame): Product ID mismatch records
        duplicates (pd.DataFrame): Duplicate records
        restock_exceeded (pd.DataFrame): Exceeded restock records
        
    Returns:
        pd.DataFrame: Combined quarantine inventory with unique records
    """
    return pd.concat([
        neg_stock,
        pid_mismatch,
        duplicates,
        restock_exceeded
    ], ignore_index=True).drop_duplicates()


def get_validation_summary(neg_stock, pid_mismatch, duplicates, restock_exceeded):
    """
    Generate a summary of validation results.
    
    Args:
        neg_stock (pd.DataFrame): Negative stock records
        pid_mismatch (pd.DataFrame): Product ID mismatch records
        duplicates (pd.DataFrame): Duplicate records
        restock_exceeded (pd.DataFrame): Exceeded restock records
        
    Returns:
        dict: Summary statistics for each validation check
    """
    return {
        "negative_stock_count": len(neg_stock),
        "product_id_mismatch_count": len(pid_mismatch),
        "duplicate_count": len(duplicates),
        "restock_exceeded_count": len(restock_exceeded),
        "total_quarantined": len(create_quarantine_inventory(
            neg_stock, pid_mismatch, duplicates, restock_exceeded
        ))
    }
