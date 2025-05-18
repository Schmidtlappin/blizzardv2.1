"""
Configuration management for the Blizzard 2.1 system.

This module provides functionality for loading and managing configuration
from JSON/YAML files and environment variables.
"""

import os
import yaml
from typing import Dict, Any, Optional
from dotenv import load_dotenv

from src.core.exceptions import ConfigurationError

class Settings:
    """
    Settings manager for the Blizzard system.
    
    This class loads and manages configuration from YAML files and environment variables.
    """
    
    _instance = None
    
    @classmethod
    def get_instance(cls):
        """Get or create the singleton instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        """Initialize the settings manager."""
        self.config = {}
        self._load_defaults()
        self._load_environment()
    
    def _load_defaults(self, config_dir: str = 'config/defaults'):
        """Load default configuration from YAML files."""
        if not os.path.isdir(config_dir):
            return
        
        for filename in os.listdir(config_dir):
            if filename.endswith('.yaml') or filename.endswith('.yml'):
                filepath = os.path.join(config_dir, filename)
                try:
                    with open(filepath, 'r') as f:
                        section = os.path.splitext(filename)[0]
                        self.config[section] = yaml.safe_load(f)
                except Exception as e:
                    raise ConfigurationError(f"Error loading config file {filepath}: {e}")
    
    def _load_environment(self):
        """Load configuration from environment variables."""
        load_dotenv()  # Load .env file if present
        
        # Example: Convert DATABASE_HOST to config['database']['host']
        for key, value in os.environ.items():
            if '_' in key:
                section, option = key.lower().split('_', 1)
                if section not in self.config:
                    self.config[section] = {}
                self.config[section][option] = value
    
    def get(self, section: str, option: str, default: Any = None) -> Any:
        """Get a configuration value."""
        try:
            return self.config.get(section, {}).get(option, default)
        except Exception:
            return default
    
    def set(self, section: str, option: str, value: Any) -> None:
        """Set a configuration value."""
        if section not in self.config:
            self.config[section] = {}
        self.config[section][option] = value
        
    @classmethod
    def from_file(cls, filepath: str) -> 'Settings':
        """
        Create a Settings instance from a configuration file.
        
        Args:
            filepath: Path to the configuration file (YAML, JSON, or key=value format)
            
        Returns:
            Settings instance
        """
        instance = cls()
        
        try:
            # Check if the file exists
            if not os.path.isfile(filepath):
                raise ConfigurationError(f"Config file not found: {filepath}")
                
            # Parse the file based on extension
            _, ext = os.path.splitext(filepath)
            
            if ext.lower() in ('.yaml', '.yml'):
                # Parse YAML file
                with open(filepath, 'r') as f:
                    config_data = yaml.safe_load(f)
                    if isinstance(config_data, dict):
                        instance.config.update(config_data)
                    else:
                        raise ConfigurationError(f"Invalid YAML config format in {filepath}")
                        
            elif ext.lower() == '.json':
                # Parse JSON file
                import json
                with open(filepath, 'r') as f:
                    config_data = json.load(f)
                    if isinstance(config_data, dict):
                        instance.config.update(config_data)
                    else:
                        raise ConfigurationError(f"Invalid JSON config format in {filepath}")
                        
            else:
                # Assume key=value format or credential format
                with open(filepath, 'r') as f:
                    # Try to parse as key=value or credential format
                    is_db_credential = False
                    db_params = {}
                    
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                            
                        # Check for common database credential format
                        if line.lower().startswith('host=') or line.lower().startswith('dbname='):
                            is_db_credential = True
                            
                            # Parse PostgreSQL connection string format
                            for part in line.split():
                                if '=' in part:
                                    key, value = part.split('=', 1)
                                    db_params[key.strip().lower()] = value.strip()
                        elif '=' in line:
                            # Parse as key=value
                            key, value = line.split('=', 1)
                            key = key.strip().lower()
                            value = value.strip()
                            
                            # Put into database section if it looks like a DB parameter
                            if key in ('host', 'port', 'dbname', 'user', 'password'):
                                if 'database' not in instance.config:
                                    instance.config['database'] = {}
                                instance.config['database'][key] = value
                            else:
                                # Default to general section
                                if 'general' not in instance.config:
                                    instance.config['general'] = {}
                                instance.config['general'][key] = value
                    
                    if is_db_credential:
                        # Store the parsed database parameters
                        instance.config['database'] = db_params
            
            return instance
            
        except Exception as e:
            if not isinstance(e, ConfigurationError):
                raise ConfigurationError(f"Error loading config file {filepath}: {e}")
            raise
            
    def get_db_params(self) -> Dict[str, str]:
        """
        Get database connection parameters.
        
        Returns:
            Dictionary with database connection parameters
        """
        db_params = self.config.get('database', {})
        
        # Add defaults for missing parameters
        if 'host' not in db_params:
            db_params['host'] = 'localhost'
        if 'port' not in db_params:
            db_params['port'] = '5432'
        if 'name' not in db_params and 'dbname' not in db_params:
            db_params['dbname'] = 'irs990'
        elif 'name' in db_params and 'dbname' not in db_params:
            db_params['dbname'] = db_params['name']
            
        return db_params
        
    def get_processing_params(self) -> Dict[str, Any]:
        """
        Get processing parameters.
        
        Returns:
            Dictionary with processing parameters
        """
        return self.config.get('processing', {})
    
    def get_storage_params(self) -> Dict[str, Any]:
        """
        Get storage parameters.
        
        Returns:
            Dictionary with storage parameters
        """
        return self.config.get('storage', {})
