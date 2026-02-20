"""
Tests for the profiler module.
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from profiler import DataProfiler, profile_data


class TestDataProfiler:
    
    def test_calculate_completeness_all_present(self, sample_valid_df):
        profiler = DataProfiler(sample_valid_df)
        completeness = profiler.calculate_completeness()
        
        for col, (pct, missing) in completeness.items():
            assert pct == 100.0
            assert missing == 0
    
    def test_calculate_completeness_with_missing(self, sample_invalid_df):
        profiler = DataProfiler(sample_invalid_df)
        completeness = profiler.calculate_completeness()
        
        assert completeness['first_name'][1] == 1
        assert completeness['last_name'][1] == 1
        assert completeness['address'][1] == 1
    
    def test_detect_types(self, sample_valid_df):
        profiler = DataProfiler(sample_valid_df)
        types = profiler.detect_types()
        
        assert types['customer_id'][0] == 'INT'
        assert types['income'][0] == 'NUMERIC'
        assert types['first_name'][0] == 'STRING'
    
    def test_check_uniqueness_unique_ids(self, sample_valid_df):
        profiler = DataProfiler(sample_valid_df)
        uniqueness = profiler.check_uniqueness()
        
        is_unique, duplicates = uniqueness['customer_id']
        assert is_unique is True
        assert duplicates == 0
    
    def test_check_uniqueness_duplicate_ids(self):
        df = pd.DataFrame({
            'customer_id': [1, 1, 2],
            'first_name': ['John', 'Jane', 'Bob'],
            'last_name': ['Doe', 'Smith', 'Johnson'],
            'email': ['a@b.com', 'c@d.com', 'e@f.com'],
            'phone': ['555-123-4567', '555-234-5678', '555-345-6789'],
            'date_of_birth': ['1985-03-15', '1990-07-22', '1988-11-08'],
            'address': ['123 Main St NYC', '456 Oak Ave LA', '789 Pine Rd CHI'],
            'income': [75000.0, 95000.0, 85000.0],
            'account_status': ['active', 'active', 'inactive'],
            'created_date': ['2024-01-10', '2024-01-11', '2024-01-12']
        })
        profiler = DataProfiler(df)
        uniqueness = profiler.check_uniqueness()
        
        is_unique, duplicates = uniqueness['customer_id']
        assert is_unique is False
        assert duplicates == 1
    
    def test_analyze_phone_formats(self, sample_invalid_df):
        profiler = DataProfiler(sample_invalid_df)
        formats = profiler.analyze_phone_formats()
        
        assert 'XXX-XXX-XXXX' in formats
        assert 'XXXXXXXXXX' in formats or 'XXX.XXX.XXXX' in formats
    
    def test_find_quality_issues(self, sample_invalid_df):
        profiler = DataProfiler(sample_invalid_df)
        issues = profiler.find_quality_issues()
        
        assert len(issues) > 0
        issue_types = [i.issue_type for i in issues]
        assert 'MISSING_VALUE' in issue_types or 'INVALID_VALUE' in issue_types
    
    def test_generate_report(self, sample_valid_df):
        profiler = DataProfiler(sample_valid_df)
        report = profiler.generate_report()
        
        assert 'DATA QUALITY PROFILE REPORT' in report
        assert 'COMPLETENESS:' in report
        assert 'DATA TYPES:' in report
        assert 'UNIQUENESS:' in report
    
    def test_profile_data_function(self, temp_csv_file):
        df, report = profile_data(temp_csv_file)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert isinstance(report, str)
        assert len(report) > 0
