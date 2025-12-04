# -*- coding: utf-8 -*-
"""
Fraud Detection Main Script
Main orchestration for refund and fraud detection engine.
"""

import pandas as pd
import sys
from pathlib import Path

# Import transaction generator
from transaction_generator import generate_complete_transaction_data
from data_generator import generate_inventory_data

# Import fraud detection modules
from refund_data_generator import generate_complete_refund_data
from refund_validator import RefundValidator
from fraud_detector import FraudDetector
from anomaly_detector import AnomalyDetector
from fraud_flagger import FraudFlagger


def main():
    """
    Execute the complete refund and fraud detection pipeline.
    """
    print("=" * 80)
    print("REFUND & FRAUD DETECTION ENGINE")
    print("=" * 80)
    
    # =========================================================================
    # STEP 1: Generate Base Transaction Data
    # =========================================================================
    print("\n[STEP 1] Generating base transaction data...")
    inventory_df, _ = generate_inventory_data()
    txn_data = generate_complete_transaction_data(inventory_df, num_transactions=200, seed=42)
    
    transactions_df = txn_data['store_sales_header']
    line_items_df = txn_data['store_sales_line_items']
    
    print(f"  ✓ Generated {len(transactions_df)} transactions")
    print(f"  ✓ Generated {len(line_items_df)} line items")
    
    # =========================================================================
    # STEP 2: Generate Refund Data
    # =========================================================================
    print("\n[STEP 2] Generating refund data...")
    refund_data = generate_complete_refund_data(
        transactions_df, line_items_df, num_customers=100, fraud_rate=0.20, seed=42
    )
    
    customers_df = refund_data['customer_details']
    refunds_df = refund_data['refund_transactions']
    
    print(f"\n  Refund Data Summary:")
    print(f"    - Total refunds: {len(refunds_df)}")
    print(f"    - Actual fraudulent: {refunds_df['is_fraudulent'].sum()}")
    print(f"    - Fraud rate: {refunds_df['is_fraudulent'].mean()*100:.1f}%")
    
    # =========================================================================
    # STEP 3: Validate Refund Legitimacy
    # =========================================================================
    print("\n[STEP 3] Validating refund legitimacy...")
    
    validator = RefundValidator(max_refund_days=30)
    validation_issues, validation_summary = validator.validate_refunds(refunds_df)
    
    print("\n  Validation Results:")
    for _, row in validation_summary.iterrows():
        print(f"    - {row['check']}: {row['issues_found']} issues")
    
    # =========================================================================
    # STEP 4: Run Fraud Detection Logic
    # =========================================================================
    print("\n[STEP 4] Running fraud detection logic...")
    
    detector = FraudDetector(
        high_freq_threshold=5,
        high_value_threshold=100.0,
        high_value_count=3,
        repeated_product_threshold=3
    )
    
    fraud_results = detector.run_fraud_detection(refunds_df)
    fraud_summary = detector.get_fraud_summary(fraud_results)
    
    print("\n  Fraud Detection Results:")
    for _, row in fraud_summary.iterrows():
        print(f"    - {row['fraud_type']}: {row['count']}")
    
    # =========================================================================
    # STEP 5: Run Anomaly Detection
    # =========================================================================
    print("\n[STEP 5] Running anomaly detection...")
    
    anomaly_detector = AnomalyDetector(zscore_threshold=3.0, iqr_multiplier=1.5)
    anomaly_results = anomaly_detector.run_anomaly_detection(refunds_df, customers_df)
    anomaly_summary = anomaly_detector.get_anomaly_summary(anomaly_results)
    
    print("\n  Anomaly Detection Results:")
    for _, row in anomaly_summary.iterrows():
        print(f"    - {row['anomaly_type']}: {row['count']}")
    
    # =========================================================================
    # STEP 6: Create Fraud Flags Table
    # =========================================================================
    print("\n[STEP 6] Creating fraud flags table...")
    
    fraud_flags_df = FraudFlagger.create_fraud_flags_table(
        refunds_df, validation_issues, fraud_results, anomaly_results
    )
    
    print(f"  ✓ Created fraud flags for {len(fraud_flags_df)} refunds")
    
    # Severity breakdown
    severity_counts = fraud_flags_df['severity'].value_counts()
    print("\n  Severity Breakdown:")
    for severity in ['Critical', 'High', 'Medium', 'Low']:
        count = severity_counts.get(severity, 0)
        print(f"    - {severity}: {count}")
    
    # =========================================================================
    # STEP 7: Prioritize Investigations
    # =========================================================================
    print("\n[STEP 7] Top 10 highest priority investigations...")
    
    top_10 = FraudFlagger.prioritize_investigations(fraud_flags_df, top_n=10)
    
    print("\n  Top 10 Cases:")
    print("  " + "-" * 76)
    
    for idx, row in top_10.iterrows():
        print(f"\n  {idx + 1}. Refund ID: {row['refund_id']}")
        print(f"     Customer: {row['customer_id']}")
        print(f"     Amount: ${row['refund_amount']:.2f}")
        print(f"     Fraud Score: {row['fraud_score']}/100")
        print(f"     Severity: {row['severity']}")
        print(f"     Flags: {row['flags'][:80]}...")
    
    # =========================================================================
    # STEP 8: Generate Reports
    # =========================================================================
    print("\n[STEP 8] Generating reports...")
    
    # Create output directory
    output_dir = Path('fraud_detection_output')
    output_dir.mkdir(exist_ok=True)
    
    # Generate fraud report
    fraud_report = FraudFlagger.generate_fraud_report(fraud_flags_df)
    with open(output_dir / 'fraud_report.txt', 'w', encoding='utf-8') as f:
        f.write(fraud_report)
    print(f"  ✓ Fraud report saved to {output_dir}/fraud_report.txt")
    
    # Export fraud flags
    fraud_flags_df.to_csv(output_dir / 'fraud_flags.csv', index=False, encoding='utf-8')
    print(f"  ✓ Fraud flags exported to {output_dir}/fraud_flags.csv")
    
    # Export validation issues
    if len(validation_issues) > 0:
        validation_issues.to_csv(output_dir / 'validation_issues.csv', index=False, encoding='utf-8')
        print(f"  ✓ Validation issues exported to {output_dir}/validation_issues.csv")
    
    # =========================================================================
    # STEP 9: Model Performance Evaluation
    # =========================================================================
    print("\n[STEP 9] Evaluating detection performance...")
    
    # Compare detected fraud with ground truth
    flagged_refund_ids = set(fraud_flags_df['refund_id'].tolist())
    actual_fraud_ids = set(refunds_df[refunds_df['is_fraudulent'] == True]['refund_id'].tolist())
    
    true_positives = len(flagged_refund_ids & actual_fraud_ids)
    false_positives = len(flagged_refund_ids - actual_fraud_ids)
    false_negatives = len(actual_fraud_ids - flagged_refund_ids)
    true_negatives = len(refunds_df) - true_positives - false_positives - false_negatives
    
    precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0
    recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0
    f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    
    print("\n  Detection Performance:")
    print(f"    - True Positives: {true_positives}")
    print(f"    - False Positives: {false_positives}")
    print(f"    - False Negatives: {false_negatives}")
    print(f"    - Precision: {precision:.2%}")
    print(f"    - Recall: {recall:.2%}")
    print(f"    - F1 Score: {f1_score:.2%}")
    
    # =========================================================================
    # STEP 10: Summary
    # =========================================================================
    print("\n" + "=" * 80)
    print("FRAUD DETECTION COMPLETE")
    print("=" * 80)
    
    print(f"\nResults saved to: {output_dir}/")
    print("  - fraud_report.txt")
    print("  - fraud_flags.csv")
    print("  - validation_issues.csv")
    
    print("\n" + "=" * 80)
    
    # Return all data for further analysis
    return {
        'customers': customers_df,
        'transactions': transactions_df,
        'line_items': line_items_df,
        'refunds': refunds_df,
        'validation_issues': validation_issues,
        'fraud_results': fraud_results,
        'anomaly_results': anomaly_results,
        'fraud_flags': fraud_flags_df
    }


if __name__ == "__main__":
    try:
        results = main()
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
