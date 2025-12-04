# -*- coding: utf-8 -*-
"""
Column Mapper Module
Maps source columns to target schema and handles missing columns.
"""

import pandas as pd
from typing import Dict, List, Any, Optional


class ColumnMapper:
    """Maps source columns to target schema based on configuration."""
    
    @staticmethod
    def apply_column_mapping(df: pd.DataFrame, column_mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Rename columns based on mapping configuration.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            column_mapping (Dict[str, str]): Mapping of target_column: source_column
            
        Returns:
            pd.DataFrame: DataFrame with renamed columns
        """
        # Create reverse mapping (source -> target)
        reverse_mapping = {v: k for k, v in column_mapping.items()}
        
        # Only rename columns that exist in the DataFrame
        existing_mapping = {
            src: tgt for src, tgt in reverse_mapping.items() 
            if src in df.columns
        }
        
        return df.rename(columns=existing_mapping)
    
    @staticmethod
    def add_default_columns(df: pd.DataFrame, defaults: Dict[str, Any]) -> pd.DataFrame:
        """
        Add missing columns with default values.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            defaults (Dict[str, Any]): Dictionary of column: default_value
            
        Returns:
            pd.DataFrame: DataFrame with default columns added
        """
        for column, default_value in defaults.items():
            if column not in df.columns:
                df[column] = default_value
        
        return df
    
    @staticmethod
    def validate_required_columns(df: pd.DataFrame, required_columns: List[str]) -> bool:
        """
        Validate that all required columns exist in DataFrame.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            required_columns (List[str]): List of required column names
            
        Returns:
            bool: True if all required columns exist
            
        Raises:
            ValueError: If required columns are missing
        """
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        return True
    
    @staticmethod
    def select_columns(df: pd.DataFrame, columns: List[str], 
                      drop_others: bool = False) -> pd.DataFrame:
        """
        Select specific columns from DataFrame.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            columns (List[str]): Columns to select
            drop_others (bool): If True, drop columns not in the list
            
        Returns:
            pd.DataFrame: DataFrame with selected columns
        """
        if drop_others:
            # Only keep specified columns that exist
            existing_columns = [col for col in columns if col in df.columns]
            return df[existing_columns]
        else:
            # Add missing columns as None
            for col in columns:
                if col not in df.columns:
                    df[col] = None
            return df
    
    @staticmethod
    def reorder_columns(df: pd.DataFrame, column_order: List[str]) -> pd.DataFrame:
        """
        Reorder columns according to specified order.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            column_order (List[str]): Desired column order
            
        Returns:
            pd.DataFrame: DataFrame with reordered columns
        """
        # Get columns that exist in both the order list and DataFrame
        existing_ordered = [col for col in column_order if col in df.columns]
        
        # Get remaining columns not in the order list
        remaining = [col for col in df.columns if col not in column_order]
        
        # Combine: ordered columns first, then remaining
        final_order = existing_ordered + remaining
        
        return df[final_order]
    
    @staticmethod
    def apply_mapping_config(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """
        Apply complete mapping configuration to DataFrame.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            config (Dict[str, Any]): Configuration containing mapping rules
            
        Returns:
            pd.DataFrame: Transformed DataFrame
        """
        # Apply column mapping
        if 'column_mapping' in config:
            df = ColumnMapper.apply_column_mapping(df, config['column_mapping'])
        
        # Add default columns
        if 'defaults' in config:
            df = ColumnMapper.add_default_columns(df, config['defaults'])
        
        # Validate required columns
        if 'required_columns' in config:
            ColumnMapper.validate_required_columns(df, config['required_columns'])
        
        # Select specific columns
        if 'select_columns' in config:
            drop_others = config.get('drop_other_columns', False)
            df = ColumnMapper.select_columns(df, config['select_columns'], drop_others)
        
        # Reorder columns
        if 'column_order' in config:
            df = ColumnMapper.reorder_columns(df, config['column_order'])
        
        return df


def apply_column_mapping(df: pd.DataFrame, column_mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Convenience function to apply column mapping.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        column_mapping (Dict[str, str]): Mapping of target_column: source_column
        
    Returns:
        pd.DataFrame: DataFrame with renamed columns
    """
    return ColumnMapper.apply_column_mapping(df, column_mapping)
