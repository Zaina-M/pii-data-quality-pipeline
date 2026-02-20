"""
Configuration Management Module
Loads settings from YAML config file and environment variables.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional


DEFAULT_CONFIG = {
    'pipeline': {
        'input_file': 'data/customers_raw.csv',
        'output_dir': 'output',
        'log_file': 'output/pipeline.log',
        'log_level': 'INFO',
    },
    'validation': {
        'name_min_length': 2,
        'name_max_length': 50,
        'address_min_length': 10,
        'address_max_length': 200,
        'max_income': 10_000_000,
        'valid_statuses': ['active', 'inactive', 'suspended'],
    },
    'cleaning': {
        'phone_format': 'XXX-XXX-XXXX',
        'date_format': '%Y-%m-%d',
        'missing_string_fill': '[UNKNOWN]',
        'missing_numeric_fill': 0,
        'missing_status_fill': 'unknown',
    },
    'masking': {
        'mask_char': '*',
        'preserve_email_domain': True,
        'preserve_phone_last_digits': 4,
        'preserve_dob_year': True,
    },
    'pii': {
        'high_risk_columns': ['first_name', 'last_name', 'email', 'phone', 'date_of_birth', 'address'],
        'medium_risk_columns': ['income'],
    },
}


_config_cache: Optional[Dict[str, Any]] = None


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration from YAML file with environment variable overrides.
    
    Priority (highest to lowest):
    1. Environment variables (prefixed with DQV_)
    2. YAML config file
    3. Default values
    """
    global _config_cache
    
    config = DEFAULT_CONFIG.copy()
    
    if config_path is None:
        config_path = os.environ.get('DQV_CONFIG_PATH')
        if config_path is None:
            possible_paths = [
                Path(__file__).parent.parent / 'config.yaml',
                Path(__file__).parent.parent / 'config.yml',
                Path.cwd() / 'config.yaml',
            ]
            for p in possible_paths:
                if p.exists():
                    config_path = str(p)
                    break
    
    if config_path and Path(config_path).exists():
        with open(config_path, 'r') as f:
            file_config = yaml.safe_load(f)
            if file_config:
                config = _deep_merge(config, file_config)
    
    config = _apply_env_overrides(config)
    
    _config_cache = config
    return config


def _deep_merge(base: Dict, override: Dict) -> Dict:
    """Deep merge two dictionaries."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _apply_env_overrides(config: Dict) -> Dict:
    """Apply environment variable overrides to config."""
    env_mappings = {
        'DQV_INPUT_FILE': ('pipeline', 'input_file'),
        'DQV_OUTPUT_DIR': ('pipeline', 'output_dir'),
        'DQV_LOG_LEVEL': ('pipeline', 'log_level'),
        'DQV_LOG_FILE': ('pipeline', 'log_file'),
        'DQV_MAX_INCOME': ('validation', 'max_income'),
        'DQV_NAME_MIN_LENGTH': ('validation', 'name_min_length'),
        'DQV_NAME_MAX_LENGTH': ('validation', 'name_max_length'),
    }
    
    for env_var, path in env_mappings.items():
        value = os.environ.get(env_var)
        if value is not None:
            section, key = path
            if section in config:
                if key in ['max_income', 'name_min_length', 'name_max_length', 
                          'address_min_length', 'address_max_length']:
                    config[section][key] = int(value)
                else:
                    config[section][key] = value
    
    return config


def get_config() -> Dict[str, Any]:
    """Get cached config or load it."""
    global _config_cache
    if _config_cache is None:
        return load_config()
    return _config_cache


def reset_config():
    """Reset config cache (useful for testing)."""
    global _config_cache
    _config_cache = None


class Config:
    """Config accessor class for cleaner access patterns."""
    
    def __init__(self, config_dict: Dict[str, Any] = None):
        self._config = config_dict or get_config()
    
    @property
    def pipeline(self) -> Dict[str, Any]:
        return self._config.get('pipeline', {})
    
    @property
    def validation(self) -> Dict[str, Any]:
        return self._config.get('validation', {})
    
    @property
    def cleaning(self) -> Dict[str, Any]:
        return self._config.get('cleaning', {})
    
    @property
    def masking(self) -> Dict[str, Any]:
        return self._config.get('masking', {})
    
    @property
    def pii(self) -> Dict[str, Any]:
        return self._config.get('pii', {})
    
    @property
    def input_file(self) -> str:
        return self.pipeline.get('input_file', 'data/customers_raw.csv')
    
    @property
    def output_dir(self) -> str:
        return self.pipeline.get('output_dir', 'output')
    
    @property
    def log_file(self) -> str:
        return self.pipeline.get('log_file', 'output/pipeline.log')
    
    @property
    def log_level(self) -> str:
        return self.pipeline.get('log_level', 'INFO')

    @property
    def raw(self) -> Dict[str, Any]:
        return self._config
