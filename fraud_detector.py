# -*- coding: utf-8 -*-
"""
Fraud Detector Module
Implements fraud detection logic for refund transactions.
"""

import pandas as pd
from datetime import timedelta
from typing import Dict, List


class FraudDetector:
    """Detects fraudulent refund patterns."""
    
    def __init__(
        self,
        high_freq_threshold: int = 5,
        high_value_threshold: float = 100.0,
        high_value_count: int = 3,
        repeated_product_threshold: int = 3,
        time_window_days: int = 30
    ):
        """
        Initialize the fraud detector.
        
        Args:
            high_freq_threshold (int): Number of refunds to flag as high-frequency
            high_value_threshold (float): Dollar amount to consider high-value
            high_value_count (int): Number of high-value refunds to flag
            repeated_product_threshold (int): Number of same-product refunds to flag
            time_window_days (int): Time window for frequency analysis
        """
        self.high_freq_threshold = high_freq_threshold
        self.high_value_threshold = high_value_threshold
        self.high_value_count = high_value_count
        self.repeated_product_threshold = repeated_product_threshold
        self.time_window_days = time_window_days
    
    def detect_high_frequency_refunds(self, refunds_df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect customers with high-frequency refunds.
        
        Args:
            refunds_df (pd.DataFrame): Refund transactions
            
        Returns:
            pd.DataFrame: Flagged refunds
        """
        # Count refunds per customer in time window
        refund_counts = refunds_df.groupby('customer_id').size().reset_index(name='refund_count')
        
        # Flag customers exceeding threshold
        high_freq_customers = refund_counts[
            refund_counts['refund_count'] > self.high_freq_threshold
        ]['customer_id'].tolist()
        
        # Get all refunds from these customers
        flagged = refunds_df[refunds_df['customer_id'].isin(high_freq_customers)].copy()
        flagged['fraud_type'] = 'High-Frequency Refunds'
        flagged['fraud_detail'] = flagged['customer_id'].map(
            refund_counts.set_index('customer_id')['refund_count']
        ).apply(lambda x: f'{x} refunds')
        
        return flagged
    
    def detect_high_value_refunds(self, refunds_df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect customers with multiple high-value refunds.
        
        Args:
            refunds_df (pd.DataFrame): Refund transactions
            
        Returns:
            pd.DataFrame: Flagged refunds
        """
        # Filter high-value refunds
        high_value = refunds_df[refunds_df['refund_amount'] > self.high_value_threshold].copy()
        
        # Count high-value refunds per customer
        high_value_counts = high_value.groupby('customer_id').size().reset_index(name='high_value_count')
        
        # Flag customers with multiple high-value refunds
        flagged_customers = high_value_counts[
            high_value_counts['high_value_count'] >= self.high_value_count
        ]['customer_id'].tolist()
        
        flagged = high_value[high_value['customer_id'].isin(flagged_customers)].copy()
        flagged['fraud_type'] = 'High-Value Frequent Refunds'
        flagged['fraud_detail'] = flagged['customer_id'].map(
            high_value_counts.set_index('customer_id')['high_value_count']
        ).apply(lambda x: f'{x} high-value refunds')
        
        return flagged
    
    def detect_payment_mismatches(self, refunds_df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect refunds with payment mode mismatches.
        
        Args:
            refunds_df (pd.DataFrame): Refund transactions
            
        Returns:
            pd.DataFrame: Flagged refunds
        """
        if 'original_payment_mode' not in refunds_df.columns:
            return pd.DataFrame()
        
        flagged = refunds_df[
            refunds_df['payment_mode'] != refunds_df['original_payment_mode']
        ].copy()
        
        flagged['fraud_type'] = 'Payment Mode Mismatch'
        flagged['fraud_detail'] = (
            'Original: ' + flagged['original_payment_mode'] + 
            ', Refund: ' + flagged['payment_mode']
        )
        
        return flagged
    
    def detect_repeated_product_refunds(self, refunds_df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect customers repeatedly refunding the same product.
        
        Args:
            refunds_df (pd.DataFrame): Refund transactions
            
        Returns:
            pd.DataFrame: Flagged refunds
        """
        # Count refunds per customer-product combination
        product_refunds = refunds_df.groupby(['customer_id', 'product_id']).size().reset_index(name='product_refund_count')
        
        # Flag combinations exceeding threshold
        flagged_combos = product_refunds[
            product_refunds['product_refund_count'] > self.repeated_product_threshold
        ]
        
        # Get all refunds for these combinations
        flagged = refunds_df.merge(
            flagged_combos[['customer_id', 'product_id', 'product_refund_count']],
            on=['customer_id', 'product_id'],
            how='inner'
        )
        
        flagged['fraud_type'] = 'Repeated Product Refunds'
        flagged['fraud_detail'] = flagged['product_refund_count'].apply(
            lambda x: f'Product refunded {x} times'
        )
        
        return flagged
    
    def run_fraud_detection(self, refunds_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Run complete fraud detection pipeline.
        
        Args:
            refunds_df (pd.DataFrame): Refund transactions
            
        Returns:
            Dict[str, pd.DataFrame]: Dictionary of fraud types and flagged refunds
        """
        results = {}
        
        # Run all fraud detection checks
        results['high_frequency'] = self.detect_high_frequency_refunds(refunds_df)
        results['high_value'] = self.detect_high_value_refunds(refunds_df)
        results['payment_mismatch'] = self.detect_payment_mismatches(refunds_df)
        results['repeated_product'] = self.detect_repeated_product_refunds(refunds_df)
        
        # Combine all flagged refunds
        all_flagged = []
        for fraud_type, flagged_df in results.items():
            if len(flagged_df) > 0:
                all_flagged.append(flagged_df)
        
        if all_flagged:
            results['all_fraud'] = pd.concat(all_flagged, ignore_index=True)
        else:
            results['all_fraud'] = pd.DataFrame()
        
        return results
    
    def get_fraud_summary(self, fraud_results: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Generate fraud detection summary.
        
        Args:
            fraud_results (Dict[str, pd.DataFrame]): Fraud detection results
            
        Returns:
            pd.DataFrame: Summary statistics
        """
        summary = pd.DataFrame({
            'fraud_type': [
                'High-Frequency Refunds',
                'High-Value Frequent Refunds',
                'Payment Mode Mismatch',
                'Repeated Product Refunds',
                'Total Unique Flagged'
            ],
            'count': [
                len(fraud_results.get('high_frequency', [])),
                len(fraud_results.get('high_value', [])),
                len(fraud_results.get('payment_mismatch', [])),
                len(fraud_results.get('repeated_product', [])),
                len(fraud_results.get('all_fraud', []).drop_duplicates(subset=['refund_id'])) if 'all_fraud' in fraud_results else 0
            ]
        })
        
        return summary


def detect_fraud_patterns(
    refunds_df: pd.DataFrame,
    high_freq_threshold: int = 5,
    high_value_threshold: float = 100.0
) -> Dict[str, pd.DataFrame]:
    """
    Convenience function to detect fraud patterns.
    
    Args:
        refunds_df (pd.DataFrame): Refund transactions
        high_freq_threshold (int): High-frequency threshold
        high_value_threshold (float): High-value threshold
        
    Returns:
        Dict[str, pd.DataFrame]: Fraud detection results
    """
    detector = FraudDetector(high_freq_threshold, high_value_threshold)
    return detector.run_fraud_detection(refunds_df)
