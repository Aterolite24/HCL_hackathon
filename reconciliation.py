# -*- coding: utf-8 -*-
"""
Reconciliation Module for Inventory Harmonization Pipeline
Handles product ID reconciliation using SKU validation and fuzzy matching.
"""

import pandas as pd
import re
from rapidfuzz import fuzz, process
from config import FUZZY_MATCH_THRESHOLD


def reconcile_product_id(row, product_master):
    """
    Reconcile a single product ID using SKU validation and fuzzy name matching.
    
    Args:
        row (pd.Series): Row from incoming inventory
        product_master (pd.DataFrame): Product master reference data
        
    Returns:
        tuple: (suggested_product_id, status, confidence_score)
            - suggested_product_id: Reconciled product ID
            - status: 'valid', 'corrected', or 'unresolved'
            - confidence_score: Confidence level (0.0 to 1.0)
    """
    # ---------------------------
    # 1. SKU PATTERN VALIDATION
    # ---------------------------
    valid_sku = False
    correct_sku_pattern = product_master["sku_pattern"].iloc[0]  # Assume uniform pattern
    
    if pd.notna(row["product_id"]):
        valid_sku = bool(re.match(correct_sku_pattern, row["product_id"]))
    
    # ---------------------------
    # 2. If valid product_id → return as is
    # ---------------------------
    if valid_sku:
        return row["product_id"], "valid", 1.0
    
    # ---------------------------
    # 3. FUZZY MATCH USING NAME
    # ---------------------------
    item_name = str(row["item_name"])
    candidates = product_master["product_name"].tolist()
    
    best_match, score, index = process.extractOne(
        item_name, candidates, scorer=fuzz.token_sort_ratio
    )
    
    suggested_pid = product_master.iloc[index]["product_id"]
    
    # ---------------------------
    # 4. Decision
    # ---------------------------
    status = "corrected" if score > FUZZY_MATCH_THRESHOLD else "unresolved"
    
    return suggested_pid, status, score / 100


def apply_reconciliation(incoming_inventory, product_master):
    """
    Apply reconciliation to entire incoming inventory DataFrame.
    
    Args:
        incoming_inventory (pd.DataFrame): Incoming inventory data
        product_master (pd.DataFrame): Product master reference data
        
    Returns:
        pd.DataFrame: Incoming inventory with reconciliation columns added
    """
    # Initialize new columns
    incoming_inventory["reconciled_product_id"] = None
    incoming_inventory["recon_status"] = None
    incoming_inventory["confidence"] = None
    
    # Apply reconciliation to each row
    for idx, row in incoming_inventory.iterrows():
        pid, status, conf = reconcile_product_id(row, product_master)
        incoming_inventory.at[idx, "reconciled_product_id"] = pid
        incoming_inventory.at[idx, "recon_status"] = status
        incoming_inventory.at[idx, "confidence"] = conf
    
    return incoming_inventory


def get_reconciliation_summary(incoming_inventory):
    """
    Generate a summary of reconciliation results.
    
    Args:
        incoming_inventory (pd.DataFrame): Reconciled incoming inventory
        
    Returns:
        dict: Summary statistics for reconciliation
    """
    if "recon_status" not in incoming_inventory.columns:
        return {"error": "Reconciliation not yet applied"}
    
    status_counts = incoming_inventory["recon_status"].value_counts().to_dict()
    avg_confidence = incoming_inventory["confidence"].mean()
    
    return {
        "total_records": len(incoming_inventory),
        "valid_count": status_counts.get("valid", 0),
        "corrected_count": status_counts.get("corrected", 0),
        "unresolved_count": status_counts.get("unresolved", 0),
        "average_confidence": round(avg_confidence, 3)
    }
