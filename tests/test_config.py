"""
Tests for the config module.
"""

import pytest
import os
import tempfile
import yaml
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from config import load_config, get_config, reset_config, Config, DEFAULT_CONFIG


class TestLoadConfig:
    
    def test_load_default_config(self):
        reset_config()
        config = load_config()
        
        assert 'pipeline' in config
        assert 'validation' in config
        assert 'cleaning' in config
        assert 'masking' in config
    
    def test_default_values(self):
        reset_config()
        config = load_config()
        
        assert config['validation']['name_min_length'] == 2
        assert config['validation']['name_max_length'] == 50
        assert config['validation']['max_income'] == 10_000_000
    
    def test_load_from_yaml_file(self):
        reset_config()
        custom_config = {
            'validation': {
                'max_income': 5_000_000
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(custom_config, f)
            config_path = f.name
        
        try:
            config = load_config(config_path)
            assert config['validation']['max_income'] == 5_000_000
        finally:
            os.unlink(config_path)
    
    def test_yaml_overrides_defaults(self):
        reset_config()
        custom_config = {
            'validation': {
                'name_min_length': 3
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(custom_config, f)
            config_path = f.name
        
        try:
            config = load_config(config_path)
            assert config['validation']['name_min_length'] == 3
            assert config['validation']['name_max_length'] == 50
        finally:
            os.unlink(config_path)


class TestEnvironmentOverrides:
    
    def test_env_override_input_file(self):
        reset_config()
        os.environ['DQV_INPUT_FILE'] = 'custom/path.csv'
        
        try:
            config = load_config()
            assert config['pipeline']['input_file'] == 'custom/path.csv'
        finally:
            del os.environ['DQV_INPUT_FILE']
            reset_config()
    
    def test_env_override_max_income(self):
        reset_config()
        os.environ['DQV_MAX_INCOME'] = '1000000'
        
        try:
            config = load_config()
            assert config['validation']['max_income'] == 1000000
        finally:
            del os.environ['DQV_MAX_INCOME']
            reset_config()
    
    def test_env_override_log_level(self):
        reset_config()
        os.environ['DQV_LOG_LEVEL'] = 'DEBUG'
        
        try:
            config = load_config()
            assert config['pipeline']['log_level'] == 'DEBUG'
        finally:
            del os.environ['DQV_LOG_LEVEL']
            reset_config()


class TestGetConfig:
    
    def test_get_config_cached(self):
        reset_config()
        config1 = get_config()
        config2 = get_config()
        
        assert config1 is config2
    
    def test_reset_clears_cache(self):
        config1 = get_config()
        reset_config()
        config2 = get_config()
        
        assert config1 is not config2


class TestConfigClass:
    
    def test_config_properties(self, test_config):
        config = Config(test_config)
        
        assert config.input_file == 'data/customers_raw.csv'
        assert config.output_dir == 'output'
        assert config.log_level == 'DEBUG'
    
    def test_validation_properties(self, test_config):
        config = Config(test_config)
        
        assert config.validation['name_min_length'] == 2
        assert config.validation['max_income'] == 10_000_000
    
    def test_cleaning_properties(self, test_config):
        config = Config(test_config)
        
        assert config.cleaning['missing_string_fill'] == '[UNKNOWN]'
        assert config.cleaning['missing_numeric_fill'] == 0
    
    def test_masking_properties(self, test_config):
        config = Config(test_config)
        
        assert config.masking['mask_char'] == '*'
        assert config.masking['preserve_email_domain'] is True
    
    def test_default_config_structure(self):
        assert 'pipeline' in DEFAULT_CONFIG
        assert 'validation' in DEFAULT_CONFIG
        assert 'cleaning' in DEFAULT_CONFIG
        assert 'masking' in DEFAULT_CONFIG
        assert 'pii' in DEFAULT_CONFIG
