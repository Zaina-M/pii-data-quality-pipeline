"""
Tests for the masker module.
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from masker import PIIMasker, mask_pii


class TestNameMasking:
    
    def test_mask_simple_name(self):
        df = pd.DataFrame({'first_name': ['John', 'Jane', 'Bob']})
        masker = PIIMasker(df)
        
        assert masker.mask_name('John') == 'J***'
        assert masker.mask_name('Jane') == 'J***'
        assert masker.mask_name('Bob') == 'B***'
    
    def test_mask_empty_name(self):
        df = pd.DataFrame({'first_name': ['']})
        masker = PIIMasker(df)
        
        assert masker.mask_name('') == ''
    
    def test_mask_unknown_placeholder(self):
        df = pd.DataFrame({'first_name': ['[UNKNOWN]']})
        masker = PIIMasker(df)
        
        assert masker.mask_name('[UNKNOWN]') == '[UNKNOWN]'
    
    def test_mask_none_name(self):
        df = pd.DataFrame({'first_name': [None]})
        masker = PIIMasker(df)
        
        result = masker.mask_name(None)
        assert 'None' in result or result == 'None'


class TestEmailMasking:
    
    def test_mask_standard_email(self):
        df = pd.DataFrame({'email': ['john.doe@gmail.com']})
        masker = PIIMasker(df)
        
        result = masker.mask_email('john.doe@gmail.com')
        assert result == 'j***@gmail.com'
    
    def test_mask_short_local_part(self):
        df = pd.DataFrame({'email': ['a@example.com']})
        masker = PIIMasker(df)
        
        result = masker.mask_email('a@example.com')
        assert result == 'a***@example.com'
    
    def test_mask_preserves_domain(self):
        df = pd.DataFrame({'email': ['test@company.org']})
        masker = PIIMasker(df)
        
        result = masker.mask_email('test@company.org')
        assert '@company.org' in result
    
    def test_mask_empty_email(self):
        df = pd.DataFrame({'email': ['']})
        masker = PIIMasker(df)
        
        assert masker.mask_email('') == ''


class TestPhoneMasking:
    
    def test_mask_standard_phone(self):
        df = pd.DataFrame({'phone': ['555-123-4567']})
        masker = PIIMasker(df)
        
        result = masker.mask_phone('555-123-4567')
        assert result == '***-***-4567'
    
    def test_mask_preserves_last_four(self):
        df = pd.DataFrame({'phone': ['555-987-6543']})
        masker = PIIMasker(df)
        
        result = masker.mask_phone('555-987-6543')
        assert '6543' in result
    
    def test_mask_unformatted_phone(self):
        df = pd.DataFrame({'phone': ['5551234567']})
        masker = PIIMasker(df)
        
        result = masker.mask_phone('5551234567')
        assert '4567' in result


class TestAddressMasking:
    
    def test_mask_full_address(self):
        df = pd.DataFrame({'address': ['123 Main St New York NY 10001']})
        masker = PIIMasker(df)
        
        result = masker.mask_address('123 Main St New York NY 10001')
        assert result == '[MASKED ADDRESS]'
    
    def test_mask_empty_address(self):
        df = pd.DataFrame({'address': ['']})
        masker = PIIMasker(df)
        
        result = masker.mask_address('')
        assert result == ''
    
    def test_mask_unknown_placeholder(self):
        df = pd.DataFrame({'address': ['[UNKNOWN]']})
        masker = PIIMasker(df)
        
        result = masker.mask_address('[UNKNOWN]')
        assert result == '[UNKNOWN]'


class TestDOBMasking:
    
    def test_mask_standard_dob(self):
        df = pd.DataFrame({'date_of_birth': ['1985-03-15']})
        masker = PIIMasker(df)
        
        result = masker.mask_dob('1985-03-15')
        assert result == '1985-**-**'
    
    def test_mask_preserves_year(self):
        df = pd.DataFrame({'date_of_birth': ['1990-07-22']})
        masker = PIIMasker(df)
        
        result = masker.mask_dob('1990-07-22')
        assert result.startswith('1990')
    
    def test_mask_empty_dob(self):
        df = pd.DataFrame({'date_of_birth': ['']})
        masker = PIIMasker(df)
        
        result = masker.mask_dob('')
        assert result == ''


class TestMaskAll:
    
    def test_mask_all_columns(self, sample_valid_df):
        masker = PIIMasker(sample_valid_df)
        masked_df = masker.mask_all()
        
        assert masked_df['first_name'].iloc[0] == 'J***'
        assert '***@' in masked_df['email'].iloc[0]
        assert '***-***' in masked_df['phone'].iloc[0]
        assert masked_df['address'].iloc[0] == '[MASKED ADDRESS]'
        assert '**-**' in masked_df['date_of_birth'].iloc[0]
    
    def test_mask_stats_populated(self, sample_valid_df):
        masker = PIIMasker(sample_valid_df)
        masker.mask_all()
        
        assert 'first_name' in masker.mask_stats
        assert 'email' in masker.mask_stats
        assert 'phone' in masker.mask_stats
    
    def test_preserves_non_pii_columns(self, sample_valid_df):
        original_income = sample_valid_df['income'].copy()
        original_status = sample_valid_df['account_status'].copy()
        
        masker = PIIMasker(sample_valid_df)
        masked_df = masker.mask_all()
        
        assert list(masked_df['income']) == list(original_income)
        assert list(masked_df['account_status']) == list(original_status)


class TestMaskPIIFunction:
    
    def test_returns_masked_df_and_masker(self, sample_valid_df):
        masked_df, masker = mask_pii(sample_valid_df)
        
        assert isinstance(masked_df, pd.DataFrame)
        assert isinstance(masker, PIIMasker)
    
    def test_generate_sample_report(self, sample_valid_df):
        masked_df, masker = mask_pii(sample_valid_df)
        report = masker.generate_sample_report()
        
        assert 'BEFORE MASKING' in report
        assert 'AFTER MASKING' in report
        assert 'ANALYSIS:' in report
