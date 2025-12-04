# -*- coding: utf-8 -*-
"""
Affinity Analyzer Main Script
Main orchestration for shopping basket affinity analysis.
"""

import pandas as pd
import sys
from pathlib import Path

# Import existing modules
from data_generator import generate_inventory_data

# Import new affinity analysis modules
from transaction_generator import generate_complete_transaction_data
from market_basket_analyzer import MarketBasketAnalyzer, perform_market_basket_analysis
from incremental_affinity_updater import IncrementalAffinityUpdater
from affinity_reporter import AffinityReporter, generate_complete_report


def main():
    """
    Execute the complete shopping basket affinity analysis pipeline.
    """
    print("=" * 80)
    print("SHOPPING BASKET AFFINITY ANALYZER")
    print("=" * 80)
    
    # =========================================================================
    # STEP 1: Generate Base Inventory Data
    # =========================================================================
    print("\n[STEP 1] Generating base inventory data...")
    inventory_snapshot_df, restock_events_df = generate_inventory_data()
    print(f"  ✓ Generated {len(inventory_snapshot_df)} inventory snapshots")
    
    # =========================================================================
    # STEP 2: Generate Transaction Data
    # =========================================================================
    print("\n[STEP 2] Generating transaction data...")
    transaction_data = generate_complete_transaction_data(
        inventory_snapshot_df,
        num_transactions=200,
        seed=42
    )
    
    products_df = transaction_data['products']
    stores_df = transaction_data['stores']
    transactions_df = transaction_data['store_sales_header']
    line_items_df = transaction_data['store_sales_line_items']
    
    print(f"\n  Transaction Data Summary:")
    print(f"    - Products: {len(products_df)}")
    print(f"    - Stores: {len(stores_df)}")
    print(f"    - Transactions: {len(transactions_df)}")
    print(f"    - Line Items: {len(line_items_df)}")
    print(f"    - Avg items per basket: {len(line_items_df) / len(transactions_df):.2f}")
    
    # =========================================================================
    # STEP 3: Perform Market Basket Analysis
    # =========================================================================
    print("\n[STEP 3] Performing market basket analysis...")
    
    analyzer = MarketBasketAnalyzer(min_support=0.01, min_confidence=0.1)
    results_df = analyzer.analyze(line_items_df, products_df)
    
    print(f"  ✓ Found {len(results_df)} association rules")
    print(f"  ✓ Average lift: {results_df['lift'].mean():.4f}")
    
    # =========================================================================
    # STEP 4: Identify Top 10 Affinities
    # =========================================================================
    print("\n[STEP 4] Identifying top 10 product affinities...")
    
    top_10 = analyzer.find_top_affinities(results_df, top_n=10, metric='lift')
    
    print("\n  Top 10 Product Affinities (by Lift):")
    print("  " + "-" * 76)
    
    for idx, row in top_10.iterrows():
        print(f"  {idx + 1}. {row['product_a_name']} → {row['product_b_name']}")
        print(f"     Lift: {row['lift']:.4f} | Confidence: {row['confidence']:.4f} | Support: {row['support']:.4f}")
    
    # =========================================================================
    # STEP 5: Generate Reports
    # =========================================================================
    print("\n[STEP 5] Generating reports and visualizations...")
    
    # Create output directory
    output_dir = Path('affinity_analysis_output')
    output_dir.mkdir(exist_ok=True)
    
    # Generate complete report
    generate_complete_report(results_df, products_df, str(output_dir), top_n=10)
    
    # =========================================================================
    # STEP 6: Demonstrate Real-Time Incremental Updates
    # =========================================================================
    print("\n[STEP 6] Demonstrating real-time incremental updates...")
    
    # Initialize incremental updater with first 150 transactions
    updater = IncrementalAffinityUpdater(min_support=0.01, min_confidence=0.1)
    
    initial_line_items = line_items_df[
        line_items_df['transaction_id'].isin(transactions_df['transaction_id'][:150])
    ]
    updater.initialize_from_line_items(initial_line_items)
    
    print(f"  ✓ Initialized with {150} transactions")
    
    # Get initial affinities
    initial_affinities = updater.get_top_affinities(top_n=5)
    print(f"  ✓ Initial top 5 affinities computed")
    
    # Simulate new transactions arriving
    new_line_items = line_items_df[
        line_items_df['transaction_id'].isin(transactions_df['transaction_id'][150:200])
    ]
    
    print(f"\n  Processing {50} new transactions...")
    updater.process_new_batch(new_line_items)
    
    # Get updated affinities
    updated_affinities = updater.get_top_affinities(top_n=5)
    print(f"  ✓ Updated affinities computed incrementally")
    
    print("\n  Top 5 Affinities After Incremental Update:")
    print("  " + "-" * 76)
    
    for idx, row in updated_affinities.iterrows():
        print(f"  {idx + 1}. {row['product_a']} → {row['product_b']}")
        print(f"     Lift: {row['lift']:.4f} | Confidence: {row['confidence']:.4f}")
    
    # Get cache statistics
    stats = updater.cache.get_statistics()
    print(f"\n  Cache Statistics:")
    print(f"    - Total transactions: {stats['total_transactions']}")
    print(f"    - Unique products: {stats['unique_products']}")
    print(f"    - Unique pairs tracked: {stats['unique_pairs']}")
    
    # =========================================================================
    # STEP 7: Product Recommendations Example
    # =========================================================================
    print("\n[STEP 7] Product recommendation examples...")
    
    # Get recommendations for a specific product
    sample_product = products_df.iloc[0]['product_id']
    sample_product_name = products_df.iloc[0]['product_name']
    
    recommendations = analyzer.get_recommendations(results_df, sample_product, top_n=3)
    
    print(f"\n  Customers who buy '{sample_product_name}' also buy:")
    print("  " + "-" * 76)
    
    for idx, row in recommendations.iterrows():
        print(f"  {idx + 1}. {row['product_b_name']}")
        print(f"     Lift: {row['lift']:.4f} | Confidence: {row['confidence']:.4f}")
    
    # =========================================================================
    # STEP 8: Summary
    # =========================================================================
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    
    print(f"\nResults saved to: {output_dir}/")
    print("  - affinity_report.txt")
    print("  - recommendations.csv")
    print("  - top_affinities.png")
    print("  - affinity_heatmap.png")
    
    print("\n" + "=" * 80)
    
    # Return all data for further analysis
    return {
        'products': products_df,
        'stores': stores_df,
        'transactions': transactions_df,
        'line_items': line_items_df,
        'affinity_results': results_df,
        'top_10_affinities': top_10,
        'analyzer': analyzer,
        'incremental_updater': updater
    }


if __name__ == "__main__":
    try:
        results = main()
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
