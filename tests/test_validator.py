"""
Tests for the validator module (Pandera implementation).
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from validator import DataValidator, validate_data, is_valid_name, is_valid_email, is_valid_date, is_valid_phone


class TestValidationFunctions:
    
    def test_is_valid_name_valid(self):
        series = pd.Series(['John', 'Jane', 'Mary-Ann', "O'Brien"])
        result = is_valid_name(series)
        assert all(result)
    
    def test_is_valid_name_invalid(self):
        series = pd.Series(['', 'John123', None])
        result = is_valid_name(series)
        assert not any(result)
    
    def test_is_valid_email_valid(self):
        series = pd.Series(['test@example.com', 'user.name@company.org', 'a@b.co'])
        result = is_valid_email(series)
        assert all(result)
    
    def test_is_valid_email_invalid(self):
        series = pd.Series(['invalid', 'no@domain', '@missing.com', 'spaces in@email.com'])
        result = is_valid_email(series)
        assert not any(result)
    
    def test_is_valid_date_valid(self):
        series = pd.Series(['2024-01-15', '1990-12-31', '2000-06-01'])
        result = is_valid_date(series)
        assert all(result)
    
    def test_is_valid_date_invalid(self):
        series = pd.Series(['invalid_date', '01/15/2024', '2024/01/15', ''])
        result = is_valid_date(series)
        assert not any(result)
    
    def test_is_valid_phone_valid(self):
        series = pd.Series(['555-123-4567', '5551234567', '(555) 123-4567'])
        result = is_valid_phone(series)
        assert all(result)
    
    def test_is_valid_phone_invalid(self):
        series = pd.Series(['123', '12345', ''])
        result = is_valid_phone(series)
        assert not any(result)


class TestDataValidator:
    
    def test_validate_valid_data(self, sample_valid_df, test_config):
        validator = DataValidator(sample_valid_df, test_config)
        passed, failed, failures = validator.validate()
        
        assert passed == 3
        assert failed == 0
        assert len(failures) == 0
    
    def test_validate_with_issues(self, sample_invalid_df, test_config):
        validator = DataValidator(sample_invalid_df, test_config)
        passed, failed, failures = validator.validate()
        
        assert failed > 0
        assert len(failures) > 0
    
    def test_validate_missing_first_name(self, test_config):
        df = pd.DataFrame({
            'customer_id': [1],
            'first_name': [''],
            'last_name': ['Doe'],
            'email': ['test@example.com'],
            'phone': ['555-123-4567'],
            'date_of_birth': ['1985-03-15'],
            'address': ['123 Main Street New York NY 10001'],
            'income': [75000.0],
            'account_status': ['active'],
            'created_date': ['2024-01-10']
        })
        validator = DataValidator(df, test_config)
        passed, failed, failures = validator.validate()
        
        assert failed > 0
        failure_cols = [f.column for f in failures]
        assert 'first_name' in failure_cols
    
    def test_validate_invalid_email(self, test_config):
        df = pd.DataFrame({
            'customer_id': [1],
            'first_name': ['John'],
            'last_name': ['Doe'],
            'email': ['not-an-email'],
            'phone': ['555-123-4567'],
            'date_of_birth': ['1985-03-15'],
            'address': ['123 Main Street New York NY 10001'],
            'income': [75000.0],
            'account_status': ['active'],
            'created_date': ['2024-01-10']
        })
        validator = DataValidator(df, test_config)
        passed, failed, failures = validator.validate()
        
        assert failed > 0
        failure_cols = [f.column for f in failures]
        assert 'email' in failure_cols
    
    def test_validate_invalid_status(self, test_config):
        df = pd.DataFrame({
            'customer_id': [1],
            'first_name': ['John'],
            'last_name': ['Doe'],
            'email': ['test@example.com'],
            'phone': ['555-123-4567'],
            'date_of_birth': ['1985-03-15'],
            'address': ['123 Main Street New York NY 10001'],
            'income': [75000.0],
            'account_status': ['invalid_status'],
            'created_date': ['2024-01-10']
        })
        validator = DataValidator(df, test_config)
        passed, failed, failures = validator.validate()
        
        assert failed > 0
        failure_cols = [f.column for f in failures]
        assert 'account_status' in failure_cols
    
    def test_validate_negative_income(self, test_config):
        df = pd.DataFrame({
            'customer_id': [1],
            'first_name': ['John'],
            'last_name': ['Doe'],
            'email': ['test@example.com'],
            'phone': ['555-123-4567'],
            'date_of_birth': ['1985-03-15'],
            'address': ['123 Main Street New York NY 10001'],
            'income': [-5000.0],
            'account_status': ['active'],
            'created_date': ['2024-01-10']
        })
        validator = DataValidator(df, test_config)
        passed, failed, failures = validator.validate()
        
        assert failed > 0
        failure_cols = [f.column for f in failures]
        assert 'income' in failure_cols
    
    def test_generate_report(self, sample_valid_df, test_config):
        validator = DataValidator(sample_valid_df, test_config)
        report = validator.generate_report()
        
        assert 'VALIDATION RESULTS' in report
        assert 'PASS:' in report
        assert 'FAIL:' in report
    
    def test_validate_data_function(self, sample_valid_df, test_config):
        report, all_passed = validate_data(sample_valid_df, test_config)
        
        assert isinstance(report, str)
        assert all_passed is True


class TestValidationWithConfig:
    
    def test_custom_max_income(self):
        config = {
            'validation': {
                'name_min_length': 2,
                'name_max_length': 50,
                'address_min_length': 10,
                'address_max_length': 200,
                'max_income': 50000,
                'valid_statuses': ['active', 'inactive', 'suspended'],
            }
        }
        df = pd.DataFrame({
            'customer_id': [1],
            'first_name': ['John'],
            'last_name': ['Doe'],
            'email': ['test@example.com'],
            'phone': ['555-123-4567'],
            'date_of_birth': ['1985-03-15'],
            'address': ['123 Main Street New York NY 10001'],
            'income': [75000.0],
            'account_status': ['active'],
            'created_date': ['2024-01-10']
        })
        validator = DataValidator(df, config)
        passed, failed, failures = validator.validate()
        
        assert failed > 0
        failure_cols = [f.column for f in failures]
        assert 'income' in failure_cols
