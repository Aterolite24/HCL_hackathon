# -*- coding: utf-8 -*-
"""
Main Orchestration Script for Inventory Harmonization Pipeline
Supports both synthetic data generation and config-driven file ingestion.
"""

import argparse
import sys
import pandas as pd

# Synthetic data generation imports
from data_generator import (
    generate_inventory_data,
    generate_incoming_inventory,
    create_product_master
)

# Validation imports
from validators import (
    check_negative_stock,
    check_product_id_mismatch,
    check_duplicates,
    check_restock_exceeded,
    create_quarantine_inventory,
    get_validation_summary
)

# Reconciliation imports
from reconciliation import (
    apply_reconciliation,
    get_reconciliation_summary
)

# Data processing imports
from data_processor import (
    merge_snapshots_with_restocks,
    calculate_effective_stock,
    create_inventory_fact_table,
    get_processing_summary
)

# Config-driven ingestion imports
from ingestion_engine import IngestionEngine


def run_synthetic_mode():
    """
    Execute the pipeline with synthetic data generation.
    """
    print("=" * 80)
    print("INVENTORY HARMONIZATION PIPELINE - SYNTHETIC MODE")
    print("=" * 80)
    
    # STEP 1: Generate Data
    print("\n[STEP 1] Generating synthetic inventory data...")
    inventory_snapshot_df, restock_events_df = generate_inventory_data()
    print(f"  ✓ Generated {len(inventory_snapshot_df)} inventory snapshots")
    print(f"  ✓ Generated {len(restock_events_df)} restock events")
    
    # STEP 2: Validate Data
    print("\n[STEP 2] Running data validation checks...")
    neg_stock = check_negative_stock(inventory_snapshot_df)
    pid_mismatch = check_product_id_mismatch(restock_events_df, inventory_snapshot_df)
    duplicates = check_duplicates(inventory_snapshot_df)
    restock_exceeded = check_restock_exceeded(restock_events_df)
    
    validation_summary = get_validation_summary(neg_stock, pid_mismatch, duplicates, restock_exceeded)
    print(f"  ✓ Negative stock records: {validation_summary['negative_stock_count']}")
    print(f"  ✓ Product ID mismatches: {validation_summary['product_id_mismatch_count']}")
    print(f"  ✓ Duplicate records: {validation_summary['duplicate_count']}")
    print(f"  ✓ Exceeded restock limits: {validation_summary['restock_exceeded_count']}")
    print(f"  ✓ Total quarantined: {validation_summary['total_quarantined']}")
    
    # STEP 3: Create Quarantine Inventory
    print("\n[STEP 3] Creating quarantine inventory...")
    quarantine_inventory = create_quarantine_inventory(neg_stock, pid_mismatch, duplicates, restock_exceeded)
    print(f"  ✓ Quarantine inventory created with {len(quarantine_inventory)} unique records")
    
    # STEP 4: Process Data
    print("\n[STEP 4] Processing and enriching data...")
    snap_enriched = calculate_effective_stock(inventory_snapshot_df, restock_events_df)
    inventory_fact_table = create_inventory_fact_table(inventory_snapshot_df, quarantine_inventory)
    
    processing_summary = get_processing_summary(inventory_snapshot_df, inventory_fact_table, snap_enriched)
    print(f"  ✓ Total snapshots: {processing_summary['total_snapshots']}")
    print(f"  ✓ Clean records in fact table: {processing_summary['clean_records']}")
    print(f"  ✓ Average effective stock: {processing_summary['avg_effective_stock']}")
    
    # STEP 5: Product Reconciliation
    print("\n[STEP 5] Running product reconciliation...")
    product_master = create_product_master()
    incoming_inventory = generate_incoming_inventory(product_master)
    print(f"  ✓ Generated {len(incoming_inventory)} incoming inventory records")
    
    incoming_inventory = apply_reconciliation(incoming_inventory, product_master)
    recon_summary = get_reconciliation_summary(incoming_inventory)
    print(f"  ✓ Valid records: {recon_summary['valid_count']}")
    print(f"  ✓ Corrected records: {recon_summary['corrected_count']}")
    print(f"  ✓ Unresolved records: {recon_summary['unresolved_count']}")
    print(f"  ✓ Average confidence: {recon_summary['average_confidence']}")
    
    # STEP 6: Display Sample Results
    print("\n[STEP 6] Sample results:")
    print("\n--- Inventory Fact Table (first 5 rows) ---")
    print(inventory_fact_table.head())
    
    print("\n--- Enriched Snapshots with Effective Stock (first 5 rows) ---")
    print(snap_enriched[["snapshot_id", "item_id", "warehouse_id", "current_quantity", 
                         "total_restocked", "effective_stock_level"]].head())
    
    print("\n--- Reconciled Incoming Inventory (first 5 rows) ---")
    print(incoming_inventory[["incoming_id", "item_name", "product_id", 
                              "reconciled_product_id", "recon_status", "confidence"]].head())
    
    print("\n" + "=" * 80)
    print("PIPELINE EXECUTION COMPLETE")
    print("=" * 80)
    
    return {
        "inventory_snapshot_df": inventory_snapshot_df,
        "restock_events_df": restock_events_df,
        "inventory_fact_table": inventory_fact_table,
        "snap_enriched": snap_enriched,
        "quarantine_inventory": quarantine_inventory,
        "incoming_inventory": incoming_inventory,
        "product_master": product_master
    }


def run_ingestion_mode(config_path: str):
    """
    Execute the pipeline with config-driven file ingestion.
    
    Args:
        config_path (str): Path to ingestion configuration file
    """
    print("=" * 80)
    print("INVENTORY HARMONIZATION PIPELINE - INGESTION MODE")
    print("=" * 80)
    
    # STEP 1: Ingest Files
    print(f"\n[STEP 1] Ingesting files from configuration: {config_path}")
    engine = IngestionEngine(config_path)
    ingested_data = engine.ingest_all()
    
    # STEP 2: Display Ingestion Summary
    print("\n[STEP 2] Ingestion summary:")
    summary = engine.get_ingestion_summary()
    print(f"  ✓ Total files ingested: {summary['total_files']}")
    
    for name, info in summary['datasets'].items():
        print(f"\n  Dataset: {name}")
        print(f"    - Records: {info['records']}")
        print(f"    - Columns: {info['columns']}")
        print(f"    - Column names: {', '.join(info['column_names'][:5])}...")
    
    # STEP 3: Process Ingested Data (if we have the required datasets)
    if 'inventory_snapshot' in ingested_data and 'restock_events' in ingested_data:
        print("\n[STEP 3] Processing ingested data...")
        
        inventory_snapshot_df = ingested_data['inventory_snapshot']
        restock_events_df = ingested_data['restock_events']
        
        # Run validation
        neg_stock = check_negative_stock(inventory_snapshot_df)
        pid_mismatch = check_product_id_mismatch(restock_events_df, inventory_snapshot_df)
        duplicates = check_duplicates(inventory_snapshot_df)
        restock_exceeded = check_restock_exceeded(restock_events_df)
        
        validation_summary = get_validation_summary(neg_stock, pid_mismatch, duplicates, restock_exceeded)
        print(f"  ✓ Validation complete - Total quarantined: {validation_summary['total_quarantined']}")
        
        # Create quarantine and fact table
        quarantine_inventory = create_quarantine_inventory(neg_stock, pid_mismatch, duplicates, restock_exceeded)
        inventory_fact_table = create_inventory_fact_table(inventory_snapshot_df, quarantine_inventory)
        snap_enriched = calculate_effective_stock(inventory_snapshot_df, restock_events_df)
        
        print(f"  ✓ Clean records in fact table: {len(inventory_fact_table)}")
        print(f"  ✓ Average effective stock: {snap_enriched['effective_stock_level'].mean():.2f}")
        
        ingested_data['inventory_fact_table'] = inventory_fact_table
        ingested_data['snap_enriched'] = snap_enriched
        ingested_data['quarantine_inventory'] = quarantine_inventory
    
    # STEP 4: Reconciliation (if we have incoming inventory)
    if 'incoming_inventory' in ingested_data:
        print("\n[STEP 4] Running product reconciliation...")
        
        product_master = create_product_master()
        incoming_inventory = ingested_data['incoming_inventory']
        
        incoming_inventory = apply_reconciliation(incoming_inventory, product_master)
        recon_summary = get_reconciliation_summary(incoming_inventory)
        
        print(f"  ✓ Valid records: {recon_summary['valid_count']}")
        print(f"  ✓ Corrected records: {recon_summary['corrected_count']}")
        print(f"  ✓ Unresolved records: {recon_summary['unresolved_count']}")
        print(f"  ✓ Average confidence: {recon_summary['average_confidence']}")
        
        ingested_data['incoming_inventory'] = incoming_inventory
    
    # STEP 5: Display Sample Results
    print("\n[STEP 5] Sample results:")
    for name, df in ingested_data.items():
        if isinstance(df, pd.DataFrame) and len(df) > 0:
            print(f"\n--- {name} (first 3 rows) ---")
            print(df.head(3))
    
    print("\n" + "=" * 80)
    print("PIPELINE EXECUTION COMPLETE")
    print("=" * 80)
    
    return ingested_data


def main():
    """
    Main entry point with argument parsing.
    """
    parser = argparse.ArgumentParser(
        description='Inventory Harmonization Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with synthetic data (default)
  python main.py
  
  # Run with config-driven ingestion
  python main.py --mode ingest --config configs/ingestion_config.yaml
        """
    )
    
    parser.add_argument(
        '--mode',
        choices=['synthetic', 'ingest'],
        default='synthetic',
        help='Pipeline mode: synthetic data generation or config-driven ingestion'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='configs/ingestion_config.yaml',
        help='Path to ingestion configuration file (for ingest mode)'
    )
    
    args = parser.parse_args()
    
    try:
        if args.mode == 'synthetic':
            results = run_synthetic_mode()
        else:
            results = run_ingestion_mode(args.config)
        
        return results
    
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    results = main()
