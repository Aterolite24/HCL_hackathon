# -*- coding: utf-8 -*-
"""
File Reader Module
Handles reading files in various formats (CSV, Excel, JSON, Parquet).
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional


class FileReader:
    """Reads files in various formats and returns pandas DataFrames."""
    
    @staticmethod
    def read_file(file_path: str, file_format: str, **kwargs) -> pd.DataFrame:
        """
        Read a file based on its format.
        
        Args:
            file_path (str): Path to the file
            file_format (str): Format of the file ('csv', 'excel', 'json', 'parquet')
            **kwargs: Additional arguments for the specific reader
            
        Returns:
            pd.DataFrame: Loaded data
            
        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If format is unsupported
        """
        path = Path(file_path)
        
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Route to appropriate reader
        readers = {
            'csv': FileReader.read_csv,
            'excel': FileReader.read_excel,
            'json': FileReader.read_json,
            'parquet': FileReader.read_parquet
        }
        
        reader = readers.get(file_format.lower())
        if not reader:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        return reader(file_path, **kwargs)
    
    @staticmethod
    def read_csv(file_path: str, **kwargs) -> pd.DataFrame:
        """
        Read CSV file.
        
        Args:
            file_path (str): Path to CSV file
            **kwargs: Additional arguments for pd.read_csv
            
        Returns:
            pd.DataFrame: Loaded data
        """
        # Default CSV reading options
        default_kwargs = {
            'encoding': 'utf-8',
            'parse_dates': True,
            'infer_datetime_format': True
        }
        default_kwargs.update(kwargs)
        
        return pd.read_csv(file_path, **default_kwargs)
    
    @staticmethod
    def read_excel(file_path: str, sheet_name: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        Read Excel file.
        
        Args:
            file_path (str): Path to Excel file
            sheet_name (Optional[str]): Name of sheet to read (default: first sheet)
            **kwargs: Additional arguments for pd.read_excel
            
        Returns:
            pd.DataFrame: Loaded data
        """
        # Default Excel reading options
        default_kwargs = {
            'sheet_name': sheet_name or 0,
            'parse_dates': True
        }
        default_kwargs.update(kwargs)
        
        return pd.read_excel(file_path, **default_kwargs)
    
    @staticmethod
    def read_json(file_path: str, **kwargs) -> pd.DataFrame:
        """
        Read JSON file.
        
        Args:
            file_path (str): Path to JSON file
            **kwargs: Additional arguments for pd.read_json
            
        Returns:
            pd.DataFrame: Loaded data
        """
        # Default JSON reading options
        default_kwargs = {
            'encoding': 'utf-8',
            'orient': 'records'  # Assume array of objects by default
        }
        default_kwargs.update(kwargs)
        
        return pd.read_json(file_path, **default_kwargs)
    
    @staticmethod
    def read_parquet(file_path: str, **kwargs) -> pd.DataFrame:
        """
        Read Parquet file.
        
        Args:
            file_path (str): Path to Parquet file
            **kwargs: Additional arguments for pd.read_parquet
            
        Returns:
            pd.DataFrame: Loaded data
        """
        return pd.read_parquet(file_path, **kwargs)
    
    @staticmethod
    def auto_detect_format(file_path: str) -> str:
        """
        Auto-detect file format based on extension.
        
        Args:
            file_path (str): Path to file
            
        Returns:
            str: Detected format
            
        Raises:
            ValueError: If format cannot be detected
        """
        path = Path(file_path)
        extension = path.suffix.lower()
        
        format_map = {
            '.csv': 'csv',
            '.xlsx': 'excel',
            '.xls': 'excel',
            '.json': 'json',
            '.parquet': 'parquet',
            '.pq': 'parquet'
        }
        
        file_format = format_map.get(extension)
        if not file_format:
            raise ValueError(f"Cannot auto-detect format for extension: {extension}")
        
        return file_format
    
    @staticmethod
    def read_with_config(config: Dict[str, Any]) -> pd.DataFrame:
        """
        Read file using configuration dictionary.
        
        Args:
            config (Dict[str, Any]): Configuration containing file_path, file_format, etc.
            
        Returns:
            pd.DataFrame: Loaded data
        """
        file_path = config['file_path']
        file_format = config['file_format']
        
        # Extract reader options from config
        reader_options = config.get('reader_options', {})
        
        return FileReader.read_file(file_path, file_format, **reader_options)


def read_file(file_path: str, file_format: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """
    Convenience function to read a file.
    
    Args:
        file_path (str): Path to file
        file_format (Optional[str]): Format of file (auto-detected if None)
        **kwargs: Additional arguments for the reader
        
    Returns:
        pd.DataFrame: Loaded data
    """
    if file_format is None:
        file_format = FileReader.auto_detect_format(file_path)
    
    return FileReader.read_file(file_path, file_format, **kwargs)
