# -*- coding: utf-8 -*-
"""
Data Transformer Module
Applies transformations to DataFrame columns based on configuration.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Callable
from datetime import datetime


class DataTransformer:
    """Applies transformations to DataFrame columns."""
    
    @staticmethod
    def apply_transformations(df: pd.DataFrame, transformations: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Apply a list of transformations to DataFrame.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            transformations (List[Dict[str, Any]]): List of transformation configs
            
        Returns:
            pd.DataFrame: Transformed DataFrame
        """
        for transform_config in transformations:
            df = DataTransformer.apply_single_transformation(df, transform_config)
        
        return df
    
    @staticmethod
    def apply_single_transformation(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """
        Apply a single transformation to a column.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            config (Dict[str, Any]): Transformation configuration
            
        Returns:
            pd.DataFrame: Transformed DataFrame
        """
        column = config.get('column')
        transform_type = config.get('type')
        
        if not column or column not in df.columns:
            return df
        
        # Route to appropriate transformer
        transformers = {
            'datetime': DataTransformer.transform_datetime,
            'integer': DataTransformer.transform_integer,
            'float': DataTransformer.transform_float,
            'string': DataTransformer.transform_string,
            'boolean': DataTransformer.transform_boolean,
            'categorical': DataTransformer.transform_categorical,
            'custom': DataTransformer.transform_custom
        }
        
        transformer = transformers.get(transform_type)
        if transformer:
            df[column] = transformer(df[column], config)
        
        return df
    
    @staticmethod
    def transform_datetime(series: pd.Series, config: Dict[str, Any]) -> pd.Series:
        """
        Transform column to datetime.
        
        Args:
            series (pd.Series): Input series
            config (Dict[str, Any]): Configuration with optional 'format' key
            
        Returns:
            pd.Series: Transformed series
        """
        date_format = config.get('format')
        
        if date_format:
            return pd.to_datetime(series, format=date_format, errors='coerce')
        else:
            return pd.to_datetime(series, errors='coerce')
    
    @staticmethod
    def transform_integer(series: pd.Series, config: Dict[str, Any]) -> pd.Series:
        """
        Transform column to integer.
        
        Args:
            series (pd.Series): Input series
            config (Dict[str, Any]): Configuration
            
        Returns:
            pd.Series: Transformed series
        """
        return pd.to_numeric(series, errors='coerce').fillna(0).astype(int)
    
    @staticmethod
    def transform_float(series: pd.Series, config: Dict[str, Any]) -> pd.Series:
        """
        Transform column to float.
        
        Args:
            series (pd.Series): Input series
            config (Dict[str, Any]): Configuration with optional 'decimals' key
            
        Returns:
            pd.Series: Transformed series
        """
        result = pd.to_numeric(series, errors='coerce')
        
        decimals = config.get('decimals')
        if decimals is not None:
            result = result.round(decimals)
        
        return result
    
    @staticmethod
    def transform_string(series: pd.Series, config: Dict[str, Any]) -> pd.Series:
        """
        Transform column to string with optional operations.
        
        Args:
            series (pd.Series): Input series
            config (Dict[str, Any]): Configuration with optional 'operation' key
            
        Returns:
            pd.Series: Transformed series
        """
        result = series.astype(str)
        
        operation = config.get('operation')
        if operation == 'upper':
            result = result.str.upper()
        elif operation == 'lower':
            result = result.str.lower()
        elif operation == 'title':
            result = result.str.title()
        elif operation == 'strip':
            result = result.str.strip()
        
        return result
    
    @staticmethod
    def transform_boolean(series: pd.Series, config: Dict[str, Any]) -> pd.Series:
        """
        Transform column to boolean.
        
        Args:
            series (pd.Series): Input series
            config (Dict[str, Any]): Configuration with optional 'true_values' key
            
        Returns:
            pd.Series: Transformed series
        """
        true_values = config.get('true_values', ['true', 'yes', '1', 1, True])
        
        return series.isin(true_values)
    
    @staticmethod
    def transform_categorical(series: pd.Series, config: Dict[str, Any]) -> pd.Series:
        """
        Transform column to categorical.
        
        Args:
            series (pd.Series): Input series
            config (Dict[str, Any]): Configuration with optional 'categories' key
            
        Returns:
            pd.Series: Transformed series
        """
        categories = config.get('categories')
        
        if categories:
            return pd.Categorical(series, categories=categories)
        else:
            return pd.Categorical(series)
    
    @staticmethod
    def transform_custom(series: pd.Series, config: Dict[str, Any]) -> pd.Series:
        """
        Apply custom transformation using Python expression.
        
        Args:
            series (pd.Series): Input series
            config (Dict[str, Any]): Configuration with 'expression' key
            
        Returns:
            pd.Series: Transformed series
        """
        expression = config.get('expression')
        
        if not expression:
            return series
        
        # Create a safe namespace for eval
        namespace = {
            'pd': pd,
            'np': np,
            'x': series,
            'len': len,
            'str': str,
            'int': int,
            'float': float
        }
        
        try:
            return eval(expression, namespace)
        except Exception as e:
            print(f"Warning: Custom transformation failed: {e}")
            return series
    
    @staticmethod
    def apply_conditional_transformation(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """
        Apply transformation conditionally based on a filter.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            config (Dict[str, Any]): Configuration with 'condition' and 'transformation' keys
            
        Returns:
            pd.DataFrame: Transformed DataFrame
        """
        condition = config.get('condition')
        transformation = config.get('transformation')
        
        if not condition or not transformation:
            return df
        
        # Apply transformation only to rows matching condition
        mask = df.eval(condition)
        df.loc[mask] = DataTransformer.apply_single_transformation(
            df.loc[mask], transformation
        )
        
        return df
    
    @staticmethod
    def fill_missing_values(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """
        Fill missing values based on configuration.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            config (Dict[str, Any]): Configuration with column: fill_value mapping
            
        Returns:
            pd.DataFrame: DataFrame with filled values
        """
        for column, fill_config in config.items():
            if column not in df.columns:
                continue
            
            if isinstance(fill_config, dict):
                method = fill_config.get('method', 'value')
                
                if method == 'value':
                    df[column].fillna(fill_config.get('value'), inplace=True)
                elif method == 'forward':
                    df[column].fillna(method='ffill', inplace=True)
                elif method == 'backward':
                    df[column].fillna(method='bfill', inplace=True)
                elif method == 'mean':
                    df[column].fillna(df[column].mean(), inplace=True)
                elif method == 'median':
                    df[column].fillna(df[column].median(), inplace=True)
            else:
                # Simple value fill
                df[column].fillna(fill_config, inplace=True)
        
        return df


def apply_transformations(df: pd.DataFrame, transformations: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convenience function to apply transformations.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        transformations (List[Dict[str, Any]]): List of transformation configs
        
    Returns:
        pd.DataFrame: Transformed DataFrame
    """
    return DataTransformer.apply_transformations(df, transformations)
