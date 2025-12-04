# -*- coding: utf-8 -*-
"""
Configuration Loader Module
Loads and validates YAML/JSON configuration files for ingestion.
"""

import yaml
import json
from pathlib import Path
from typing import Dict, List, Any, Optional


class ConfigLoader:
    """Loads and validates ingestion configuration files."""
    
    def __init__(self, config_path: str):
        """
        Initialize the configuration loader.
        
        Args:
            config_path (str): Path to the configuration file (YAML or JSON)
        """
        self.config_path = Path(config_path)
        self.config = None
        
    def load(self) -> Dict[str, Any]:
        """
        Load configuration from file.
        
        Returns:
            Dict[str, Any]: Loaded configuration
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config format is invalid
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        # Determine file type and load
        if self.config_path.suffix in ['.yaml', '.yml']:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
        elif self.config_path.suffix == '.json':
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
        else:
            raise ValueError(f"Unsupported config format: {self.config_path.suffix}")
        
        # Validate configuration
        self.validate_config()
        
        return self.config
    
    def validate_config(self):
        """
        Validate the configuration structure.
        
        Raises:
            ValueError: If configuration is invalid
        """
        if not self.config:
            raise ValueError("Configuration is empty")
        
        if 'ingestion_configs' not in self.config:
            raise ValueError("Configuration must contain 'ingestion_configs' key")
        
        if not isinstance(self.config['ingestion_configs'], list):
            raise ValueError("'ingestion_configs' must be a list")
        
        # Validate each ingestion config
        for idx, config in enumerate(self.config['ingestion_configs']):
            self._validate_single_config(config, idx)
    
    def _validate_single_config(self, config: Dict[str, Any], idx: int):
        """
        Validate a single ingestion configuration.
        
        Args:
            config (Dict[str, Any]): Single ingestion config
            idx (int): Index of the config in the list
            
        Raises:
            ValueError: If configuration is invalid
        """
        required_fields = ['name', 'file_path', 'file_format']
        
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Config {idx}: Missing required field '{field}'")
        
        # Validate file_format
        valid_formats = ['csv', 'excel', 'json', 'parquet']
        if config['file_format'] not in valid_formats:
            raise ValueError(
                f"Config {idx}: Invalid file_format '{config['file_format']}'. "
                f"Must be one of: {valid_formats}"
            )
    
    def get_file_configs(self) -> List[Dict[str, Any]]:
        """
        Get list of file configurations.
        
        Returns:
            List[Dict[str, Any]]: List of ingestion configurations
        """
        if not self.config:
            self.load()
        
        return self.config.get('ingestion_configs', [])
    
    def get_config_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific configuration by name.
        
        Args:
            name (str): Name of the configuration
            
        Returns:
            Optional[Dict[str, Any]]: Configuration if found, None otherwise
        """
        configs = self.get_file_configs()
        
        for config in configs:
            if config.get('name') == name:
                return config
        
        return None
    
    def get_global_settings(self) -> Dict[str, Any]:
        """
        Get global settings from configuration.
        
        Returns:
            Dict[str, Any]: Global settings
        """
        if not self.config:
            self.load()
        
        return self.config.get('global_settings', {})


def load_ingestion_config(config_path: str) -> Dict[str, Any]:
    """
    Convenience function to load ingestion configuration.
    
    Args:
        config_path (str): Path to configuration file
        
    Returns:
        Dict[str, Any]: Loaded configuration
    """
    loader = ConfigLoader(config_path)
    return loader.load()


def validate_config_schema(config: Dict[str, Any]) -> bool:
    """
    Validate configuration schema.
    
    Args:
        config (Dict[str, Any]): Configuration to validate
        
    Returns:
        bool: True if valid
        
    Raises:
        ValueError: If configuration is invalid
    """
    loader = ConfigLoader.__new__(ConfigLoader)
    loader.config = config
    loader.validate_config()
    return True
