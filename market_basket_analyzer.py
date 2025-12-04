# -*- coding: utf-8 -*-
"""
Market Basket Analyzer Module
Implements market basket analysis with support, confidence, and lift calculations.
"""

import pandas as pd
import numpy as np
from itertools import combinations
from typing import Dict, List, Tuple, Set
from collections import defaultdict


class MarketBasketAnalyzer:
    """Performs market basket analysis on transaction data."""
    
    def __init__(self, min_support: float = 0.01, min_confidence: float = 0.1):
        """
        Initialize the market basket analyzer.
        
        Args:
            min_support (float): Minimum support threshold (0-1)
            min_confidence (float): Minimum confidence threshold (0-1)
        """
        self.min_support = min_support
        self.min_confidence = min_confidence
        self.total_transactions = 0
        self.item_counts = defaultdict(int)
        self.pair_counts = defaultdict(int)
        self.transaction_baskets = []
        
    def build_transaction_baskets(
        self, 
        line_items_df: pd.DataFrame,
        transaction_col: str = 'transaction_id',
        product_col: str = 'product_id'
    ) -> List[Set[str]]:
        """
        Build transaction baskets from line items.
        
        Args:
            line_items_df (pd.DataFrame): Line items data
            transaction_col (str): Transaction ID column name
            product_col (str): Product ID column name
            
        Returns:
            List[Set[str]]: List of product sets per transaction
        """
        baskets = []
        
        for txn_id, group in line_items_df.groupby(transaction_col):
            basket = set(group[product_col].unique())
            baskets.append(basket)
        
        self.transaction_baskets = baskets
        self.total_transactions = len(baskets)
        
        return baskets
    
    def calculate_support(self, itemset: Tuple[str, ...]) -> float:
        """
        Calculate support for an itemset.
        
        Support(A) = Transactions containing A / Total transactions
        
        Args:
            itemset (Tuple[str, ...]): Itemset (can be single item or pair)
            
        Returns:
            float: Support value (0-1)
        """
        if self.total_transactions == 0:
            return 0.0
        
        itemset = set(itemset) if not isinstance(itemset, set) else itemset
        
        count = sum(1 for basket in self.transaction_baskets if itemset.issubset(basket))
        
        return count / self.total_transactions
    
    def calculate_confidence(self, antecedent: str, consequent: str) -> float:
        """
        Calculate confidence for a rule A → B.
        
        Confidence(A → B) = Support(A, B) / Support(A)
        
        Args:
            antecedent (str): Product A
            consequent (str): Product B
            
        Returns:
            float: Confidence value (0-1)
        """
        support_a = self.calculate_support((antecedent,))
        
        if support_a == 0:
            return 0.0
        
        support_ab = self.calculate_support((antecedent, consequent))
        
        return support_ab / support_a
    
    def calculate_lift(self, antecedent: str, consequent: str) -> float:
        """
        Calculate lift for a rule A → B.
        
        Lift(A → B) = Confidence(A → B) / Support(B)
        
        Interpretation:
        - Lift > 1: Positive correlation (buy together)
        - Lift = 1: No correlation (independent)
        - Lift < 1: Negative correlation (substitutes)
        
        Args:
            antecedent (str): Product A
            consequent (str): Product B
            
        Returns:
            float: Lift value
        """
        confidence = self.calculate_confidence(antecedent, consequent)
        support_b = self.calculate_support((consequent,))
        
        if support_b == 0:
            return 0.0
        
        return confidence / support_b
    
    def generate_product_pairs(self) -> List[Tuple[str, str]]:
        """
        Generate all unique product pairs from baskets.
        
        Returns:
            List[Tuple[str, str]]: List of product pairs
        """
        all_products = set()
        for basket in self.transaction_baskets:
            all_products.update(basket)
        
        # Generate all 2-item combinations
        pairs = list(combinations(sorted(all_products), 2))
        
        return pairs
    
    def analyze(
        self, 
        line_items_df: pd.DataFrame,
        products_df: pd.DataFrame = None
    ) -> pd.DataFrame:
        """
        Perform complete market basket analysis.
        
        Args:
            line_items_df (pd.DataFrame): Line items data
            products_df (pd.DataFrame): Products data (for names)
            
        Returns:
            pd.DataFrame: Analysis results with support, confidence, lift
        """
        # Build transaction baskets
        self.build_transaction_baskets(line_items_df)
        
        # Generate product pairs
        pairs = self.generate_product_pairs()
        
        # Calculate metrics for each pair
        results = []
        
        for product_a, product_b in pairs:
            support_ab = self.calculate_support((product_a, product_b))
            
            # Filter by minimum support
            if support_ab < self.min_support:
                continue
            
            # Calculate confidence both ways
            confidence_ab = self.calculate_confidence(product_a, product_b)
            confidence_ba = self.calculate_confidence(product_b, product_a)
            
            # Filter by minimum confidence
            if confidence_ab < self.min_confidence and confidence_ba < self.min_confidence:
                continue
            
            # Calculate lift both ways
            lift_ab = self.calculate_lift(product_a, product_b)
            lift_ba = self.calculate_lift(product_b, product_a)
            
            # Add both directions
            results.append({
                'product_a': product_a,
                'product_b': product_b,
                'support': support_ab,
                'confidence': confidence_ab,
                'lift': lift_ab,
                'direction': f'{product_a} → {product_b}'
            })
            
            results.append({
                'product_a': product_b,
                'product_b': product_a,
                'support': support_ab,
                'confidence': confidence_ba,
                'lift': lift_ba,
                'direction': f'{product_b} → {product_a}'
            })
        
        results_df = pd.DataFrame(results)
        
        # Add product names if available
        if products_df is not None:
            product_names = dict(zip(products_df['product_id'], products_df['product_name']))
            results_df['product_a_name'] = results_df['product_a'].map(product_names)
            results_df['product_b_name'] = results_df['product_b'].map(product_names)
        
        return results_df
    
    def find_top_affinities(
        self, 
        results_df: pd.DataFrame, 
        top_n: int = 10,
        metric: str = 'lift'
    ) -> pd.DataFrame:
        """
        Find top N product affinities.
        
        Args:
            results_df (pd.DataFrame): Analysis results
            top_n (int): Number of top affinities to return
            metric (str): Metric to sort by ('lift', 'confidence', 'support')
            
        Returns:
            pd.DataFrame: Top N affinities
        """
        # Sort by metric and get top N
        top_affinities = results_df.nlargest(top_n, metric)
        
        return top_affinities.reset_index(drop=True)
    
    def get_recommendations(
        self, 
        results_df: pd.DataFrame,
        product_id: str,
        top_n: int = 5
    ) -> pd.DataFrame:
        """
        Get product recommendations for a given product.
        
        Args:
            results_df (pd.DataFrame): Analysis results
            product_id (str): Product to get recommendations for
            top_n (int): Number of recommendations
            
        Returns:
            pd.DataFrame: Recommended products
        """
        # Filter for rules where product_id is the antecedent
        recommendations = results_df[
            results_df['product_a'] == product_id
        ].nlargest(top_n, 'lift')
        
        return recommendations.reset_index(drop=True)


def perform_market_basket_analysis(
    line_items_df: pd.DataFrame,
    products_df: pd.DataFrame = None,
    min_support: float = 0.01,
    min_confidence: float = 0.1
) -> Tuple[pd.DataFrame, MarketBasketAnalyzer]:
    """
    Convenience function to perform market basket analysis.
    
    Args:
        line_items_df (pd.DataFrame): Line items data
        products_df (pd.DataFrame): Products data
        min_support (float): Minimum support threshold
        min_confidence (float): Minimum confidence threshold
        
    Returns:
        Tuple[pd.DataFrame, MarketBasketAnalyzer]: (results, analyzer)
    """
    analyzer = MarketBasketAnalyzer(min_support, min_confidence)
    results_df = analyzer.analyze(line_items_df, products_df)
    
    return results_df, analyzer
