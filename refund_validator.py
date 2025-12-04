# -*- coding: utf-8 -*-
"""
Refund Validator Module
Validates refund legitimacy with various checks.
"""

import pandas as pd
from datetime import timedelta
from typing import Tuple


class RefundValidator:
    """Validates refund transactions for legitimacy."""
    
    def __init__(self, max_refund_days: int = 30):
        """
        Initialize the refund validator.
        
        Args:
            max_refund_days (int): Maximum days allowed for refunds
        """
        self.max_refund_days = max_refund_days
    
    def check_refund_amount(self, refunds_df: pd.DataFrame) -> pd.DataFrame:
        """
        Check for refunds exceeding original amount.
        
        Args:
            refunds_df (pd.DataFrame): Refund transactions
            
        Returns:
            pd.DataFrame: Invalid refunds (refund > original)
        """
        invalid = refunds_df[
            refunds_df['refund_amount'] > refunds_df['original_amount']
        ].copy()
        
        invalid['validation_issue'] = 'Refund exceeds original amount'
        
        return invalid
    
    def check_customer_match(self, refunds_df: pd.DataFrame) -> pd.DataFrame:
        """
        Check for mismatched customer IDs.
        
        Args:
            refunds_df (pd.DataFrame): Refund transactions
            
        Returns:
            pd.DataFrame: Invalid refunds (customer mismatch)
        """
        if 'original_customer_id' not in refunds_df.columns:
            return pd.DataFrame()
        
        invalid = refunds_df[
            refunds_df['customer_id'] != refunds_df['original_customer_id']
        ].copy()
        
        invalid['validation_issue'] = 'Customer ID mismatch'
        
        return invalid
    
    def check_date_window(self, refunds_df: pd.DataFrame) -> pd.DataFrame:
        """
        Check for refunds outside allowed date window.
        
        Args:
            refunds_df (pd.DataFrame): Refund transactions
            
        Returns:
            pd.DataFrame: Invalid refunds (outside date window)
        """
        if 'purchase_timestamp' not in refunds_df.columns:
            return pd.DataFrame()
        
        # Calculate days between purchase and refund
        refunds_df['days_since_purchase'] = (
            refunds_df['refund_timestamp'] - refunds_df['purchase_timestamp']
        ).dt.days
        
        invalid = refunds_df[
            refunds_df['days_since_purchase'] > self.max_refund_days
        ].copy()
        
        invalid['validation_issue'] = f'Refund outside {self.max_refund_days}-day window'
        
        return invalid
    
    def check_payment_mode(self, refunds_df: pd.DataFrame) -> pd.DataFrame:
        """
        Check for payment mode mismatches.
        
        Args:
            refunds_df (pd.DataFrame): Refund transactions
            
        Returns:
            pd.DataFrame: Invalid refunds (payment mismatch)
        """
        if 'original_payment_mode' not in refunds_df.columns:
            return pd.DataFrame()
        
        invalid = refunds_df[
            refunds_df['payment_mode'] != refunds_df['original_payment_mode']
        ].copy()
        
        invalid['validation_issue'] = 'Payment mode mismatch'
        
        return invalid
    
    def validate_refunds(self, refunds_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Run complete validation pipeline.
        
        Args:
            refunds_df (pd.DataFrame): Refund transactions
            
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: (validation_issues, summary)
        """
        issues = []
        
        # Run all validation checks
        amount_issues = self.check_refund_amount(refunds_df)
        if len(amount_issues) > 0:
            issues.append(amount_issues)
        
        customer_issues = self.check_customer_match(refunds_df)
        if len(customer_issues) > 0:
            issues.append(customer_issues)
        
        date_issues = self.check_date_window(refunds_df)
        if len(date_issues) > 0:
            issues.append(date_issues)
        
        payment_issues = self.check_payment_mode(refunds_df)
        if len(payment_issues) > 0:
            issues.append(payment_issues)
        
        # Combine all issues
        if issues:
            all_issues = pd.concat(issues, ignore_index=True)
        else:
            all_issues = pd.DataFrame()
        
        # Create summary
        summary = pd.DataFrame({
            'check': ['Amount Exceeds Original', 'Customer Mismatch', 'Outside Date Window', 'Payment Mismatch'],
            'issues_found': [
                len(amount_issues),
                len(customer_issues),
                len(date_issues),
                len(payment_issues)
            ]
        })
        
        return all_issues, summary


def validate_refund_legitimacy(
    refunds_df: pd.DataFrame,
    max_refund_days: int = 30
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Convenience function to validate refunds.
    
    Args:
        refunds_df (pd.DataFrame): Refund transactions
        max_refund_days (int): Maximum days allowed for refunds
        
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: (validation_issues, summary)
    """
    validator = RefundValidator(max_refund_days)
    return validator.validate_refunds(refunds_df)
