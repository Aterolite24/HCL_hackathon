# -*- coding: utf-8 -*-
"""
Ingestion Engine Module
Orchestrates the complete file ingestion pipeline.
"""

import pandas as pd
from typing import Dict, List, Any, Optional
from pathlib import Path

from config_loader import ConfigLoader
from file_reader import FileReader
from column_mapper import ColumnMapper
from data_transformer import DataTransformer


class IngestionEngine:
    """Orchestrates the complete file ingestion pipeline."""
    
    def __init__(self, config_path: str):
        """
        Initialize the ingestion engine.
        
        Args:
            config_path (str): Path to configuration file
        """
        self.config_loader = ConfigLoader(config_path)
        self.config = self.config_loader.load()
        self.ingested_data = {}
    
    def ingest_all(self) -> Dict[str, pd.DataFrame]:
        """
        Ingest all files defined in configuration.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionary of name: DataFrame
        """
        file_configs = self.config_loader.get_file_configs()
        
        for file_config in file_configs:
            name = file_config['name']
            print(f"Ingesting: {name}...")
            
            try:
                df = self.ingest_single_file(file_config)
                self.ingested_data[name] = df
                print(f"  ✓ Successfully ingested {len(df)} records")
            except Exception as e:
                print(f"  ✗ Error ingesting {name}: {e}")
                raise
        
        return self.ingested_data
    
    def ingest_single_file(self, config: Dict[str, Any]) -> pd.DataFrame:
        """
        Ingest a single file through the complete pipeline.
        
        Args:
            config (Dict[str, Any]): File configuration
            
        Returns:
            pd.DataFrame: Ingested and transformed DataFrame
        """
        # Step 1: Read file
        df = FileReader.read_with_config(config)
        
        # Step 2: Apply column mapping
        if 'column_mapping' in config or 'defaults' in config:
            df = ColumnMapper.apply_mapping_config(df, config)
        
        # Step 3: Apply transformations
        if 'transformations' in config:
            df = DataTransformer.apply_transformations(df, config['transformations'])
        
        # Step 4: Fill missing values
        if 'fill_missing' in config:
            df = DataTransformer.fill_missing_values(df, config['fill_missing'])
        
        # Step 5: Validate
        if 'validation_rules' in config:
            self.validate_data(df, config['validation_rules'])
        
        return df
    
    def validate_data(self, df: pd.DataFrame, validation_rules: List[Dict[str, Any]]):
        """
        Validate data based on rules.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            validation_rules (List[Dict[str, Any]]): List of validation rules
            
        Raises:
            ValueError: If validation fails
        """
        for rule in validation_rules:
            column = rule.get('column')
            rule_type = rule.get('rule')
            
            if column not in df.columns:
                continue
            
            if rule_type == 'non_negative':
                if (df[column] < 0).any():
                    raise ValueError(f"Validation failed: {column} contains negative values")
            
            elif rule_type == 'non_null':
                if df[column].isnull().any():
                    raise ValueError(f"Validation failed: {column} contains null values")
            
            elif rule_type == 'in_list':
                valid_values = rule.get('values', [])
                if not df[column].isin(valid_values).all():
                    raise ValueError(f"Validation failed: {column} contains invalid values")
            
            elif rule_type == 'range':
                min_val = rule.get('min')
                max_val = rule.get('max')
                
                if min_val is not None and (df[column] < min_val).any():
                    raise ValueError(f"Validation failed: {column} contains values below {min_val}")
                
                if max_val is not None and (df[column] > max_val).any():
                    raise ValueError(f"Validation failed: {column} contains values above {max_val}")
            
            elif rule_type == 'unique':
                if df[column].duplicated().any():
                    raise ValueError(f"Validation failed: {column} contains duplicate values")
    
    def get_ingested_data(self, name: str) -> Optional[pd.DataFrame]:
        """
        Get ingested data by name.
        
        Args:
            name (str): Name of the ingested dataset
            
        Returns:
            Optional[pd.DataFrame]: DataFrame if found, None otherwise
        """
        return self.ingested_data.get(name)
    
    def get_all_ingested_data(self) -> Dict[str, pd.DataFrame]:
        """
        Get all ingested data.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionary of all ingested datasets
        """
        return self.ingested_data
    
    def get_ingestion_summary(self) -> Dict[str, Any]:
        """
        Get summary of ingestion results.
        
        Returns:
            Dict[str, Any]: Summary statistics
        """
        summary = {
            'total_files': len(self.ingested_data),
            'datasets': {}
        }
        
        for name, df in self.ingested_data.items():
            summary['datasets'][name] = {
                'records': len(df),
                'columns': len(df.columns),
                'column_names': list(df.columns)
            }
        
        return summary


def ingest_files(config_path: str) -> Dict[str, pd.DataFrame]:
    """
    Convenience function to ingest all files from configuration.
    
    Args:
        config_path (str): Path to configuration file
        
    Returns:
        Dict[str, pd.DataFrame]: Dictionary of ingested datasets
    """
    engine = IngestionEngine(config_path)
    return engine.ingest_all()
