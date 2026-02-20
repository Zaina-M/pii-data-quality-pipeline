"""
Tests for the cleaner module.
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from cleaner import DataCleaner, clean_data


class TestPhoneNormalization:
    
    def test_normalize_standard_format(self):
        df = pd.DataFrame({
            'phone': ['555-123-4567', '555-234-5678']
        })
        cleaner = DataCleaner(df)
        affected = cleaner.normalize_phone()
        
        assert affected == 0
        assert df['phone'].iloc[0] == '555-123-4567'
    
    def test_normalize_no_dashes(self):
        df = pd.DataFrame({
            'phone': ['5551234567']
        })
        cleaner = DataCleaner(df)
        affected = cleaner.normalize_phone()
        
        assert affected == 1
        assert cleaner.df['phone'].iloc[0] == '555-123-4567'
    
    def test_normalize_parentheses(self):
        df = pd.DataFrame({
            'phone': ['(555) 234-5678']
        })
        cleaner = DataCleaner(df)
        affected = cleaner.normalize_phone()
        
        assert affected == 1
        assert cleaner.df['phone'].iloc[0] == '555-234-5678'
    
    def test_normalize_dots(self):
        df = pd.DataFrame({
            'phone': ['555.123.4567']
        })
        cleaner = DataCleaner(df)
        affected = cleaner.normalize_phone()
        
        assert affected == 1
        assert cleaner.df['phone'].iloc[0] == '555-123-4567'


class TestDateNormalization:
    
    def test_normalize_correct_format(self):
        df = pd.DataFrame({
            'date_of_birth': ['1985-03-15'],
            'created_date': ['2024-01-10']
        })
        cleaner = DataCleaner(df)
        affected = cleaner.normalize_dates()
        
        assert affected == 0
    
    def test_normalize_slash_format(self):
        df = pd.DataFrame({
            'date_of_birth': ['1985/03/15'],
            'created_date': ['2024-01-10']
        })
        cleaner = DataCleaner(df)
        affected = cleaner.normalize_dates()
        
        assert affected == 1
        assert cleaner.df['date_of_birth'].iloc[0] == '1985-03-15'
    
    def test_normalize_us_format(self):
        df = pd.DataFrame({
            'date_of_birth': ['03/15/1985'],
            'created_date': ['2024-01-10']
        })
        cleaner = DataCleaner(df)
        affected = cleaner.normalize_dates()
        
        assert affected == 1
        assert cleaner.df['date_of_birth'].iloc[0] == '1985-03-15'
    
    def test_handle_invalid_date(self):
        df = pd.DataFrame({
            'date_of_birth': ['invalid_date'],
            'created_date': ['2024-01-10']
        })
        cleaner = DataCleaner(df)
        affected = cleaner.normalize_dates()
        
        assert affected == 1
        assert cleaner.df['date_of_birth'].iloc[0] == '[INVALID_DATE]'


class TestNameNormalization:
    
    def test_normalize_uppercase(self):
        df = pd.DataFrame({
            'first_name': ['JOHN'],
            'last_name': ['DOE']
        })
        cleaner = DataCleaner(df)
        affected = cleaner.normalize_names()
        
        assert affected == 2
        assert cleaner.df['first_name'].iloc[0] == 'John'
        assert cleaner.df['last_name'].iloc[0] == 'Doe'
    
    def test_normalize_lowercase(self):
        df = pd.DataFrame({
            'first_name': ['john'],
            'last_name': ['doe']
        })
        cleaner = DataCleaner(df)
        affected = cleaner.normalize_names()
        
        assert affected == 2
        assert cleaner.df['first_name'].iloc[0] == 'John'
        assert cleaner.df['last_name'].iloc[0] == 'Doe'
    
    def test_preserve_title_case(self):
        df = pd.DataFrame({
            'first_name': ['John'],
            'last_name': ['Doe']
        })
        cleaner = DataCleaner(df)
        affected = cleaner.normalize_names()
        
        assert affected == 0


class TestEmailNormalization:
    
    def test_normalize_uppercase_email(self):
        df = pd.DataFrame({
            'email': ['JOHN@EXAMPLE.COM']
        })
        cleaner = DataCleaner(df)
        affected = cleaner.normalize_emails()
        
        assert affected == 1
        assert cleaner.df['email'].iloc[0] == 'john@example.com'
    
    def test_preserve_lowercase_email(self):
        df = pd.DataFrame({
            'email': ['john@example.com']
        })
        cleaner = DataCleaner(df)
        affected = cleaner.normalize_emails()
        
        assert affected == 0


class TestMissingValueHandling:
    
    def test_fill_missing_first_name(self):
        df = pd.DataFrame({
            'first_name': ['John', ''],
            'last_name': ['Doe', 'Smith'],
            'address': ['123 Main St NYC NY 10001', '456 Oak Ave LA CA 90001'],
            'income': [75000.0, 95000.0],
            'account_status': ['active', 'active']
        })
        cleaner = DataCleaner(df)
        missing_counts = cleaner.handle_missing_values()
        
        assert 'first_name' in missing_counts
        assert cleaner.df['first_name'].iloc[1] == '[UNKNOWN]'
    
    def test_fill_missing_income(self):
        df = pd.DataFrame({
            'first_name': ['John', 'Jane'],
            'last_name': ['Doe', 'Smith'],
            'address': ['123 Main St NYC NY 10001', '456 Oak Ave LA CA 90001'],
            'income': [75000.0, None],
            'account_status': ['active', 'active']
        })
        cleaner = DataCleaner(df)
        missing_counts = cleaner.handle_missing_values()
        
        assert 'income' in missing_counts
        assert cleaner.df['income'].iloc[1] == 0
    
    def test_fill_missing_status(self):
        df = pd.DataFrame({
            'first_name': ['John', 'Jane'],
            'last_name': ['Doe', 'Smith'],
            'address': ['123 Main St NYC NY 10001', '456 Oak Ave LA CA 90001'],
            'income': [75000.0, 95000.0],
            'account_status': ['active', '']
        })
        cleaner = DataCleaner(df)
        missing_counts = cleaner.handle_missing_values()
        
        assert 'account_status' in missing_counts
        assert cleaner.df['account_status'].iloc[1] == 'unknown'


class TestCleanDataFunction:
    
    def test_clean_data_returns_dataframe_and_cleaner(self, sample_raw_df):
        cleaned_df, cleaner = clean_data(sample_raw_df)
        
        assert isinstance(cleaned_df, pd.DataFrame)
        assert isinstance(cleaner, DataCleaner)
        assert len(cleaner.actions) > 0
    
    def test_generate_log(self, sample_raw_df):
        cleaned_df, cleaner = clean_data(sample_raw_df)
        log = cleaner.generate_log(validation_before=3, validation_after=1)
        
        assert 'DATA CLEANING LOG' in log
        assert 'ACTIONS TAKEN:' in log
