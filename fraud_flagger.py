# -*- coding: utf-8 -*-
"""
Fraud Flagger Module
Creates fraud flags and scores for suspicious refunds.
"""

import pandas as pd
from datetime import datetime
from typing import Dict


class FraudFlagger:
    """Creates fraud flags and scores for refunds."""
    
    @staticmethod
    def calculate_fraud_score(flags: list) -> int:
        """
        Calculate composite fraud score (0-100).
        
        Args:
            flags (list): List of triggered fraud/anomaly flags
            
        Returns:
            int: Fraud score (0-100)
        """
        # Base score per flag type
        flag_scores = {
            'High-Frequency Refunds': 25,
            'High-Value Frequent Refunds': 20,
            'Payment Mode Mismatch': 15,
            'Repeated Product Refunds': 20,
            'Z-Score Outlier': 15,
            'IQR Outlier': 15,
            'High Refund Ratio': 20,
            'Refund exceeds original amount': 30,
            'Customer ID mismatch': 25,
            'Outside date window': 10
        }
        
        score = 0
        for flag in flags:
            score += flag_scores.get(flag, 10)
        
        # Cap at 100
        return min(score, 100)
    
    @staticmethod
    def determine_severity(fraud_score: int) -> str:
        """
        Determine severity level based on fraud score.
        
        Args:
            fraud_score (int): Fraud score (0-100)
            
        Returns:
            str: Severity level
        """
        if fraud_score >= 75:
            return 'Critical'
        elif fraud_score >= 50:
            return 'High'
        elif fraud_score >= 25:
            return 'Medium'
        else:
            return 'Low'
    
    @staticmethod
    def create_fraud_flags_table(
        refunds_df: pd.DataFrame,
        validation_issues: pd.DataFrame,
        fraud_results: Dict[str, pd.DataFrame],
        anomaly_results: Dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        """
        Create comprehensive fraud flags table.
        
        Args:
            refunds_df (pd.DataFrame): All refund transactions
            validation_issues (pd.DataFrame): Validation issues
            fraud_results (Dict): Fraud detection results
            anomaly_results (Dict): Anomaly detection results
            
        Returns:
            pd.DataFrame: Fraud flags table
        """
        # Collect all flagged refund IDs and their flags
        refund_flags = {}
        
        # Add validation issues
        if len(validation_issues) > 0:
            for _, row in validation_issues.iterrows():
                refund_id = row['refund_id']
                if refund_id not in refund_flags:
                    refund_flags[refund_id] = []
                refund_flags[refund_id].append(row['validation_issue'])
        
        # Add fraud detections
        for fraud_type, flagged_df in fraud_results.items():
            if fraud_type == 'all_fraud':
                continue
            if len(flagged_df) > 0:
                for _, row in flagged_df.iterrows():
                    refund_id = row['refund_id']
                    if refund_id not in refund_flags:
                        refund_flags[refund_id] = []
                    refund_flags[refund_id].append(row['fraud_type'])
        
        # Add anomaly detections
        for anomaly_type, anomalies_df in anomaly_results.items():
            if anomaly_type == 'all_anomalies':
                continue
            if len(anomalies_df) > 0:
                for _, row in anomalies_df.iterrows():
                    refund_id = row['refund_id']
                    if refund_id not in refund_flags:
                        refund_flags[refund_id] = []
                    refund_flags[refund_id].append(row['anomaly_type'])
        
        # Create fraud flags table
        fraud_flags_records = []
        
        for refund_id, flags in refund_flags.items():
            # Get refund details
            refund = refunds_df[refunds_df['refund_id'] == refund_id].iloc[0]
            
            # Remove duplicates from flags
            unique_flags = list(set(flags))
            
            # Calculate fraud score
            fraud_score = FraudFlagger.calculate_fraud_score(unique_flags)
            
            # Determine severity
            severity = FraudFlagger.determine_severity(fraud_score)
            
            # Calculate investigation priority (1-10, 10 = highest)
            priority = min(10, max(1, fraud_score // 10))
            
            fraud_flags_records.append({
                'refund_id': refund_id,
                'customer_id': refund['customer_id'],
                'refund_amount': refund['refund_amount'],
                'fraud_score': fraud_score,
                'flags': ', '.join(unique_flags),
                'flag_count': len(unique_flags),
                'severity': severity,
                'investigation_priority': priority,
                'flagged_timestamp': datetime.now()
            })
        
        fraud_flags_df = pd.DataFrame(fraud_flags_records)
        
        # Sort by fraud score (descending)
        fraud_flags_df = fraud_flags_df.sort_values('fraud_score', ascending=False)
        
        return fraud_flags_df.reset_index(drop=True)
    
    @staticmethod
    def prioritize_investigations(fraud_flags_df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
        """
        Get top priority investigations.
        
        Args:
            fraud_flags_df (pd.DataFrame): Fraud flags table
            top_n (int): Number of top priorities to return
            
        Returns:
            pd.DataFrame: Top priority cases
        """
        return fraud_flags_df.nlargest(top_n, 'fraud_score')
    
    @staticmethod
    def generate_fraud_report(fraud_flags_df: pd.DataFrame) -> str:
        """
        Generate fraud detection report.
        
        Args:
            fraud_flags_df (pd.DataFrame): Fraud flags table
            
        Returns:
            str: Formatted report
        """
        report = []
        report.append("=" * 80)
        report.append("FRAUD DETECTION REPORT")
        report.append("=" * 80)
        report.append("")
        
        # Summary statistics
        report.append("SUMMARY STATISTICS")
        report.append("-" * 80)
        report.append(f"Total flagged refunds: {len(fraud_flags_df)}")
        report.append(f"Average fraud score: {fraud_flags_df['fraud_score'].mean():.1f}")
        report.append(f"Max fraud score: {fraud_flags_df['fraud_score'].max()}")
        report.append("")
        
        # Severity breakdown
        severity_counts = fraud_flags_df['severity'].value_counts()
        report.append("SEVERITY BREAKDOWN")
        report.append("-" * 80)
        for severity in ['Critical', 'High', 'Medium', 'Low']:
            count = severity_counts.get(severity, 0)
            report.append(f"{severity}: {count}")
        report.append("")
        
        # Top 10 highest priority cases
        report.append("TOP 10 HIGHEST PRIORITY CASES")
        report.append("-" * 80)
        
        top_10 = fraud_flags_df.nlargest(10, 'fraud_score')
        
        for idx, row in top_10.iterrows():
            report.append(f"\n{idx + 1}. Refund ID: {row['refund_id']}")
            report.append(f"   Customer: {row['customer_id']}")
            report.append(f"   Amount: ${row['refund_amount']:.2f}")
            report.append(f"   Fraud Score: {row['fraud_score']}/100")
            report.append(f"   Severity: {row['severity']}")
            report.append(f"   Flags: {row['flags']}")
        
        report.append("")
        report.append("=" * 80)
        
        return "\n".join(report)


def create_fraud_flags(
    refunds_df: pd.DataFrame,
    validation_issues: pd.DataFrame,
    fraud_results: Dict[str, pd.DataFrame],
    anomaly_results: Dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """
    Convenience function to create fraud flags.
    
    Args:
        refunds_df (pd.DataFrame): All refund transactions
        validation_issues (pd.DataFrame): Validation issues
        fraud_results (Dict): Fraud detection results
        anomaly_results (Dict): Anomaly detection results
        
    Returns:
        pd.DataFrame: Fraud flags table
    """
    return FraudFlagger.create_fraud_flags_table(
        refunds_df, validation_issues, fraud_results, anomaly_results
    )
