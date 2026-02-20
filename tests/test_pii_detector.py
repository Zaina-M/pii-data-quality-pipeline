"""
Tests for the PII detector module.
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from pii_detector import PIIDetector, detect_pii


class TestPIIDetector:
    
    def test_detect_by_column_name(self, sample_valid_df):
        detector = PIIDetector(sample_valid_df)
        pii_cols = detector.detect_by_column_name()
        
        pii_names = [p.name for p in pii_cols]
        assert 'first_name' in pii_names
        assert 'last_name' in pii_names
        assert 'email' in pii_names
        assert 'phone' in pii_names
        assert 'date_of_birth' in pii_names
        assert 'address' in pii_names
    
    def test_detect_emails(self, sample_valid_df):
        detector = PIIDetector(sample_valid_df)
        result = detector.detect_emails()
        
        assert result['found'] is True
        assert result['count'] == 3
    
    def test_detect_emails_no_email_column(self):
        df = pd.DataFrame({'name': ['John', 'Jane'], 'age': [30, 25]})
        detector = PIIDetector(df)
        result = detector.detect_emails()
        
        assert result['found'] is False
        assert result['count'] == 0
    
    def test_detect_phones(self, sample_valid_df):
        detector = PIIDetector(sample_valid_df)
        result = detector.detect_phones()
        
        assert result['found'] is True
        assert result['count'] == 3
    
    def test_detect_addresses(self, sample_valid_df):
        detector = PIIDetector(sample_valid_df)
        result = detector.detect_addresses()
        
        assert result['found'] is True
        assert result['count'] == 3
    
    def test_detect_addresses_with_missing(self, sample_raw_df):
        detector = PIIDetector(sample_raw_df)
        result = detector.detect_addresses()
        
        assert result['found'] is True
        assert result['count'] == 2
    
    def test_detect_dob(self, sample_valid_df):
        detector = PIIDetector(sample_valid_df)
        result = detector.detect_dob()
        
        assert result['found'] is True
        assert result['count'] == 3
    
    def test_assess_risk(self, sample_valid_df):
        detector = PIIDetector(sample_valid_df)
        risk = detector.assess_risk()
        
        assert 'HIGH' in risk
        assert 'MEDIUM' in risk
        assert len(risk['HIGH']) > 0
    
    def test_detect_by_pattern(self, sample_valid_df):
        detector = PIIDetector(sample_valid_df)
        patterns = detector.detect_by_pattern()
        
        assert 'email' in patterns
        assert 'phone' in patterns
    
    def test_generate_report(self, sample_valid_df):
        detector = PIIDetector(sample_valid_df)
        report = detector.generate_report()
        
        assert 'PII DETECTION REPORT' in report
        assert 'RISK ASSESSMENT:' in report
        assert 'DETECTED PII:' in report
        assert 'EXPOSURE RISK:' in report
    
    def test_detect_pii_function(self, sample_valid_df):
        report = detect_pii(sample_valid_df)
        
        assert isinstance(report, str)
        assert 'PII DETECTION REPORT' in report


class TestPIIRiskLevels:
    
    def test_high_risk_columns_identified(self, sample_valid_df):
        detector = PIIDetector(sample_valid_df)
        pii_cols = detector.detect_by_column_name()
        
        high_risk = [p for p in pii_cols if p.risk_level in ['HIGH', 'CRITICAL']]
        assert len(high_risk) >= 5
    
    def test_medium_risk_for_income(self, sample_valid_df):
        detector = PIIDetector(sample_valid_df)
        pii_cols = detector.detect_by_column_name()
        
        income_pii = [p for p in pii_cols if p.name == 'income']
        assert len(income_pii) == 1
        assert income_pii[0].risk_level == 'MEDIUM'
