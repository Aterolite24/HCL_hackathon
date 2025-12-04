# -*- coding: utf-8 -*-
"""
Incremental Affinity Updater Module
Implements real-time incremental updates for market basket analysis.
"""

import pandas as pd
from typing import Dict, Set, Tuple
from collections import defaultdict


class AffinityCache:
    """Maintains running statistics for incremental affinity updates."""
    
    def __init__(self):
        """Initialize the affinity cache."""
        self.total_transactions = 0
        self.item_counts = defaultdict(int)  # Count of each item
        self.pair_counts = defaultdict(int)  # Count of each pair
        self.all_products = set()
        
    def update_with_new_transaction(self, basket: Set[str]):
        """
        Update statistics with a new transaction.
        
        Args:
            basket (Set[str]): Set of product IDs in the transaction
        """
        self.total_transactions += 1
        
        # Update item counts
        for item in basket:
            self.item_counts[item] += 1
            self.all_products.add(item)
        
        # Update pair counts
        basket_list = sorted(list(basket))
        for i in range(len(basket_list)):
            for j in range(i + 1, len(basket_list)):
                pair = (basket_list[i], basket_list[j])
                self.pair_counts[pair] += 1
    
    def update_with_batch(self, baskets: list):
        """
        Update statistics with a batch of transactions.
        
        Args:
            baskets (list): List of product sets
        """
        for basket in baskets:
            self.update_with_new_transaction(basket)
    
    def get_support(self, itemset: Tuple[str, ...]) -> float:
        """
        Get support for an itemset.
        
        Args:
            itemset (Tuple[str, ...]): Itemset (single item or pair)
            
        Returns:
            float: Support value
        """
        if self.total_transactions == 0:
            return 0.0
        
        if len(itemset) == 1:
            return self.item_counts[itemset[0]] / self.total_transactions
        elif len(itemset) == 2:
            pair = tuple(sorted(itemset))
            return self.pair_counts[pair] / self.total_transactions
        else:
            return 0.0
    
    def get_confidence(self, antecedent: str, consequent: str) -> float:
        """
        Get confidence for a rule A → B.
        
        Args:
            antecedent (str): Product A
            consequent (str): Product B
            
        Returns:
            float: Confidence value
        """
        support_a = self.get_support((antecedent,))
        
        if support_a == 0:
            return 0.0
        
        support_ab = self.get_support((antecedent, consequent))
        
        return support_ab / support_a
    
    def get_lift(self, antecedent: str, consequent: str) -> float:
        """
        Get lift for a rule A → B.
        
        Args:
            antecedent (str): Product A
            consequent (str): Product B
            
        Returns:
            float: Lift value
        """
        confidence = self.get_confidence(antecedent, consequent)
        support_b = self.get_support((consequent,))
        
        if support_b == 0:
            return 0.0
        
        return confidence / support_b
    
    def get_updated_affinities(
        self, 
        min_support: float = 0.01,
        min_confidence: float = 0.1
    ) -> pd.DataFrame:
        """
        Get current affinity scores.
        
        Args:
            min_support (float): Minimum support threshold
            min_confidence (float): Minimum confidence threshold
            
        Returns:
            pd.DataFrame: Current affinities
        """
        results = []
        
        # Get all pairs that meet minimum support
        for pair, count in self.pair_counts.items():
            support = count / self.total_transactions
            
            if support < min_support:
                continue
            
            product_a, product_b = pair
            
            # Calculate metrics both ways
            confidence_ab = self.get_confidence(product_a, product_b)
            confidence_ba = self.get_confidence(product_b, product_a)
            
            if confidence_ab < min_confidence and confidence_ba < min_confidence:
                continue
            
            lift_ab = self.get_lift(product_a, product_b)
            lift_ba = self.get_lift(product_b, product_a)
            
            # Add both directions
            results.append({
                'product_a': product_a,
                'product_b': product_b,
                'support': support,
                'confidence': confidence_ab,
                'lift': lift_ab,
                'direction': f'{product_a} → {product_b}'
            })
            
            results.append({
                'product_a': product_b,
                'product_b': product_a,
                'support': support,
                'confidence': confidence_ba,
                'lift': lift_ba,
                'direction': f'{product_b} → {product_a}'
            })
        
        return pd.DataFrame(results)
    
    def get_statistics(self) -> Dict:
        """
        Get cache statistics.
        
        Returns:
            Dict: Statistics summary
        """
        return {
            'total_transactions': self.total_transactions,
            'unique_products': len(self.all_products),
            'unique_pairs': len(self.pair_counts),
            'most_frequent_item': max(self.item_counts.items(), key=lambda x: x[1])[0] if self.item_counts else None,
            'most_frequent_pair': max(self.pair_counts.items(), key=lambda x: x[1])[0] if self.pair_counts else None
        }


class IncrementalAffinityUpdater:
    """Manages incremental affinity updates for streaming transactions."""
    
    def __init__(self, min_support: float = 0.01, min_confidence: float = 0.1):
        """
        Initialize the incremental updater.
        
        Args:
            min_support (float): Minimum support threshold
            min_confidence (float): Minimum confidence threshold
        """
        self.cache = AffinityCache()
        self.min_support = min_support
        self.min_confidence = min_confidence
    
    def initialize_from_line_items(self, line_items_df: pd.DataFrame):
        """
        Initialize cache from existing line items.
        
        Args:
            line_items_df (pd.DataFrame): Line items data
        """
        baskets = []
        
        for txn_id, group in line_items_df.groupby('transaction_id'):
            basket = set(group['product_id'].unique())
            baskets.append(basket)
        
        self.cache.update_with_batch(baskets)
    
    def process_new_transaction(self, line_items: pd.DataFrame):
        """
        Process a new transaction and update affinities.
        
        Args:
            line_items (pd.DataFrame): Line items for the new transaction
        """
        basket = set(line_items['product_id'].unique())
        self.cache.update_with_new_transaction(basket)
    
    def process_new_batch(self, line_items_df: pd.DataFrame):
        """
        Process a batch of new transactions.
        
        Args:
            line_items_df (pd.DataFrame): Line items for new transactions
        """
        baskets = []
        
        for txn_id, group in line_items_df.groupby('transaction_id'):
            basket = set(group['product_id'].unique())
            baskets.append(basket)
        
        self.cache.update_with_batch(baskets)
    
    def get_current_affinities(self) -> pd.DataFrame:
        """
        Get current affinity scores.
        
        Returns:
            pd.DataFrame: Current affinities
        """
        return self.cache.get_updated_affinities(
            self.min_support, 
            self.min_confidence
        )
    
    def get_top_affinities(self, top_n: int = 10) -> pd.DataFrame:
        """
        Get top N affinities.
        
        Args:
            top_n (int): Number of top affinities
            
        Returns:
            pd.DataFrame: Top affinities
        """
        affinities = self.get_current_affinities()
        
        if len(affinities) == 0:
            return pd.DataFrame()
        
        return affinities.nlargest(top_n, 'lift').reset_index(drop=True)
