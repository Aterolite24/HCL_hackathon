# -*- coding: utf-8 -*-
"""
Anomaly Detector Module
Implements statistical anomaly detection without ML libraries.
"""

import pandas as pd
import numpy as np
from typing import Tuple, Dict


class AnomalyDetector:
    """Detects anomalies using statistical methods."""
    
    def __init__(self, zscore_threshold: float = 3.0, iqr_multiplier: float = 1.5):
        """
        Initialize the anomaly detector.
        
        Args:
            zscore_threshold (float): Z-score threshold for outliers
            iqr_multiplier (float): IQR multiplier for outlier bounds
        """
        self.zscore_threshold = zscore_threshold
        self.iqr_multiplier = iqr_multiplier
    
    def detect_zscore_anomalies(
        self, 
        refunds_df: pd.DataFrame, 
        column: str = 'refund_amount'
    ) -> pd.DataFrame:
        """
        Detect anomalies using Z-score method.
        
        Z-score = (x - mean) / std_dev
        Anomaly if |z_score| > threshold
        
        Args:
            refunds_df (pd.DataFrame): Refund transactions
            column (str): Column to analyze
            
        Returns:
            pd.DataFrame: Anomalous refunds
        """
        if column not in refunds_df.columns:
            return pd.DataFrame()
        
        # Calculate mean and standard deviation
        mean = refunds_df[column].mean()
        std = refunds_df[column].std()
        
        if std == 0:
            return pd.DataFrame()
        
        # Calculate Z-scores
        refunds_df['z_score'] = (refunds_df[column] - mean) / std
        
        # Flag anomalies
        anomalies = refunds_df[
            abs(refunds_df['z_score']) > self.zscore_threshold
        ].copy()
        
        anomalies['anomaly_type'] = 'Z-Score Outlier'
        anomalies['anomaly_detail'] = anomalies['z_score'].apply(
            lambda x: f'Z-score: {x:.2f}'
        )
        
        return anomalies
    
    def detect_iqr_anomalies(
        self, 
        refunds_df: pd.DataFrame, 
        column: str = 'refund_amount'
    ) -> pd.DataFrame:
        """
        Detect anomalies using IQR (Interquartile Range) method.
        
        IQR = Q3 - Q1
        Lower bound = Q1 - 1.5 × IQR
        Upper bound = Q3 + 1.5 × IQR
        Anomaly if x < lower or x > upper
        
        Args:
            refunds_df (pd.DataFrame): Refund transactions
            column (str): Column to analyze
            
        Returns:
            pd.DataFrame: Anomalous refunds
        """
        if column not in refunds_df.columns:
            return pd.DataFrame()
        
        # Calculate quartiles
        Q1 = refunds_df[column].quantile(0.25)
        Q3 = refunds_df[column].quantile(0.75)
        IQR = Q3 - Q1
        
        # Calculate bounds
        lower_bound = Q1 - self.iqr_multiplier * IQR
        upper_bound = Q3 + self.iqr_multiplier * IQR
        
        # Flag anomalies
        anomalies = refunds_df[
            (refunds_df[column] < lower_bound) | 
            (refunds_df[column] > upper_bound)
        ].copy()
        
        anomalies['anomaly_type'] = 'IQR Outlier'
        anomalies['anomaly_detail'] = anomalies[column].apply(
            lambda x: f'Value: ${x:.2f}, Bounds: [${lower_bound:.2f}, ${upper_bound:.2f}]'
        )
        
        return anomalies
    
    def detect_ratio_anomalies(
        self, 
        refunds_df: pd.DataFrame,
        customers_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Detect anomalies based on historical refund ratios.
        
        refund_ratio = total_refunds / total_purchases
        Anomaly if ratio > historical_avg + threshold × std
        
        Args:
            refunds_df (pd.DataFrame): Refund transactions
            customers_df (pd.DataFrame): Customer details
            
        Returns:
            pd.DataFrame: Anomalous refunds
        """
        # Calculate refund amounts per customer
        customer_refunds = refunds_df.groupby('customer_id')['refund_amount'].sum().reset_index()
        customer_refunds.columns = ['customer_id', 'current_refunds']
        
        # Merge with customer data
        customer_stats = customers_df.merge(customer_refunds, on='customer_id', how='left')
        customer_stats['current_refunds'].fillna(0, inplace=True)
        
        # Calculate refund ratio
        customer_stats['refund_ratio'] = (
            customer_stats['current_refunds'] / customer_stats['total_purchases']
        )
        
        # Calculate historical statistics
        avg_ratio = customer_stats['refund_ratio'].mean()
        std_ratio = customer_stats['refund_ratio'].std()
        
        # Flag anomalies (ratio > mean + 2 × std)
        threshold = avg_ratio + 2 * std_ratio
        anomalous_customers = customer_stats[
            customer_stats['refund_ratio'] > threshold
        ]['customer_id'].tolist()
        
        # Get all refunds from anomalous customers
        anomalies = refunds_df[refunds_df['customer_id'].isin(anomalous_customers)].copy()
        
        # Add ratio information
        ratio_map = customer_stats.set_index('customer_id')['refund_ratio'].to_dict()
        anomalies['refund_ratio'] = anomalies['customer_id'].map(ratio_map)
        anomalies['anomaly_type'] = 'High Refund Ratio'
        anomalies['anomaly_detail'] = anomalies['refund_ratio'].apply(
            lambda x: f'Ratio: {x:.2%} (threshold: {threshold:.2%})'
        )
        
        return anomalies
    
    def run_anomaly_detection(
        self, 
        refunds_df: pd.DataFrame,
        customers_df: pd.DataFrame = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Run complete anomaly detection pipeline.
        
        Args:
            refunds_df (pd.DataFrame): Refund transactions
            customers_df (pd.DataFrame): Customer details (optional)
            
        Returns:
            Dict[str, pd.DataFrame]: Dictionary of anomaly types and flagged refunds
        """
        results = {}
        
        # Run Z-score detection
        results['zscore'] = self.detect_zscore_anomalies(refunds_df, 'refund_amount')
        
        # Run IQR detection
        results['iqr'] = self.detect_iqr_anomalies(refunds_df, 'refund_amount')
        
        # Run ratio detection if customer data available
        if customers_df is not None:
            results['ratio'] = self.detect_ratio_anomalies(refunds_df, customers_df)
        else:
            results['ratio'] = pd.DataFrame()
        
        # Combine all anomalies
        all_anomalies = []
        for anomaly_type, anomalies_df in results.items():
            if len(anomalies_df) > 0:
                all_anomalies.append(anomalies_df)
        
        if all_anomalies:
            results['all_anomalies'] = pd.concat(all_anomalies, ignore_index=True)
        else:
            results['all_anomalies'] = pd.DataFrame()
        
        return results
    
    def get_anomaly_summary(self, anomaly_results: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Generate anomaly detection summary.
        
        Args:
            anomaly_results (Dict[str, pd.DataFrame]): Anomaly detection results
            
        Returns:
            pd.DataFrame: Summary statistics
        """
        summary = pd.DataFrame({
            'anomaly_type': [
                'Z-Score Outliers',
                'IQR Outliers',
                'High Refund Ratio',
                'Total Unique Anomalies'
            ],
            'count': [
                len(anomaly_results.get('zscore', [])),
                len(anomaly_results.get('iqr', [])),
                len(anomaly_results.get('ratio', [])),
                len(anomaly_results.get('all_anomalies', []).drop_duplicates(subset=['refund_id'])) if 'all_anomalies' in anomaly_results else 0
            ]
        })
        
        return summary


def detect_anomalies(
    refunds_df: pd.DataFrame,
    customers_df: pd.DataFrame = None,
    zscore_threshold: float = 3.0
) -> Dict[str, pd.DataFrame]:
    """
    Convenience function to detect anomalies.
    
    Args:
        refunds_df (pd.DataFrame): Refund transactions
        customers_df (pd.DataFrame): Customer details
        zscore_threshold (float): Z-score threshold
        
    Returns:
        Dict[str, pd.DataFrame]: Anomaly detection results
    """
    detector = AnomalyDetector(zscore_threshold)
    return detector.run_anomaly_detection(refunds_df, customers_df)
