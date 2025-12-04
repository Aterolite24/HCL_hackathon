# -*- coding: utf-8 -*-
"""
Data Processor Module for Inventory Harmonization Pipeline
Handles data transformation, enrichment, and fact table creation.
"""

import pandas as pd


def merge_snapshots_with_restocks(inventory_snapshot_df, restock_events_df):
    """
    Join inventory snapshots with restock events using merge_asof.
    
    Args:
        inventory_snapshot_df (pd.DataFrame): Inventory snapshot data
        restock_events_df (pd.DataFrame): Restock events data
        
    Returns:
        pd.DataFrame: Merged data with snapshots and their preceding restocks
    """
    return pd.merge_asof(
        inventory_snapshot_df.sort_values("snapshot_timestamp"),
        restock_events_df.sort_values("restock_timestamp"),
        left_on="snapshot_timestamp",
        right_on="restock_timestamp",
        by=["item_id", "warehouse_id"],
        direction="backward"
    )


def calculate_effective_stock(inventory_snapshot_df, restock_events_df):
    """
    Calculate effective stock levels by enriching snapshots with restock totals.
    
    Args:
        inventory_snapshot_df (pd.DataFrame): Inventory snapshot data
        restock_events_df (pd.DataFrame): Restock events data
        
    Returns:
        pd.DataFrame: Enriched snapshot data with effective_stock_level column
    """
    # Aggregate restock quantities by item and warehouse
    rs_grouped = (
        restock_events_df.groupby(["item_id", "warehouse_id"])["quantity_added"]
        .sum()
        .reset_index()
        .rename(columns={"quantity_added": "total_restocked"})
    )
    
    # Merge with snapshots
    snap_enriched = pd.merge(
        inventory_snapshot_df,
        rs_grouped,
        on=["item_id", "warehouse_id"],
        how="left"
    ).fillna({"total_restocked": 0})
    
    # Calculate effective stock level
    snap_enriched["effective_stock_level"] = (
        snap_enriched["current_quantity"]
        + snap_enriched["total_restocked"]
        - (snap_enriched["damaged_quantity"] + snap_enriched["expired_quantity"])
    )
    
    return snap_enriched


def create_inventory_fact_table(inventory_snapshot_df, quarantine_inventory):
    """
    Create a clean inventory fact table by excluding quarantined records.
    
    Args:
        inventory_snapshot_df (pd.DataFrame): Inventory snapshot data
        quarantine_inventory (pd.DataFrame): Quarantined records
        
    Returns:
        pd.DataFrame: Clean inventory fact table
    """
    # Get unique snapshot IDs from quarantine
    quarantined_ids = quarantine_inventory["snapshot_id"].unique() if "snapshot_id" in quarantine_inventory.columns else []
    
    # Filter out quarantined records
    return inventory_snapshot_df[
        ~inventory_snapshot_df["snapshot_id"].isin(quarantined_ids)
    ].copy()


def get_processing_summary(inventory_snapshot_df, inventory_fact_table, snap_enriched):
    """
    Generate a summary of data processing results.
    
    Args:
        inventory_snapshot_df (pd.DataFrame): Original inventory snapshot data
        inventory_fact_table (pd.DataFrame): Clean fact table
        snap_enriched (pd.DataFrame): Enriched snapshot data
        
    Returns:
        dict: Summary statistics for data processing
    """
    return {
        "total_snapshots": len(inventory_snapshot_df),
        "clean_records": len(inventory_fact_table),
        "quarantined_records": len(inventory_snapshot_df) - len(inventory_fact_table),
        "records_with_effective_stock": len(snap_enriched),
        "avg_effective_stock": round(snap_enriched["effective_stock_level"].mean(), 2) if "effective_stock_level" in snap_enriched.columns else 0
    }
