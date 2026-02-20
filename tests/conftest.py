"""
Pytest fixtures and configuration for data quality validation tests.
"""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from config import reset_config


@pytest.fixture
def sample_valid_df():
    """DataFrame with valid data."""
    return pd.DataFrame({
        'customer_id': [1, 2, 3],
        'first_name': ['John', 'Jane', 'Bob'],
        'last_name': ['Doe', 'Smith', 'Johnson'],
        'email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
        'phone': ['555-123-4567', '555-234-5678', '555-345-6789'],
        'date_of_birth': ['1985-03-15', '1990-07-22', '1988-11-08'],
        'address': ['123 Main St New York NY 10001', '456 Oak Ave Los Angeles CA 90001', '789 Pine Rd Chicago IL 60601'],
        'income': [75000.0, 95000.0, 85000.0],
        'account_status': ['active', 'active', 'inactive'],
        'created_date': ['2024-01-10', '2024-01-11', '2024-01-12']
    })


@pytest.fixture
def sample_invalid_df():
    """DataFrame with various data quality issues."""
    return pd.DataFrame({
        'customer_id': [1, 2, 3, 4],
        'first_name': ['John', '', 'PATRICIA', 'Mary'],
        'last_name': ['Doe', 'Smith', 'Davis', ''],
        'email': ['john@example.com', 'invalid-email', 'PATRICIA@GMAIL.COM', 'mary@test.com'],
        'phone': ['555-123-4567', '5551234567', '555.234.5678', '(555) 345-6789'],
        'date_of_birth': ['1985-03-15', 'invalid_date', '1990/07/22', '01/15/1975'],
        'address': ['123 Main St New York NY 10001', '', '456 Oak Ave', '789 Pine Rd Chicago IL 60601'],
        'income': [75000.0, None, -5000.0, 95000.0],
        'account_status': ['active', 'unknown', '', 'suspended'],
        'created_date': ['2024-01-10', '2024-01-11', 'invalid_date', '01/13/2024']
    })


@pytest.fixture
def sample_raw_df():
    """DataFrame matching the raw CSV format used in the project."""
    return pd.DataFrame({
        'customer_id': [1, 2, 3],
        'first_name': ['John', 'Jane', ''],
        'last_name': ['Doe', 'Smith', 'Johnson'],
        'email': ['john.doe@gmail.com', 'jane.smith@company.com', 'bob.johnson@email.com'],
        'phone': ['555-123-4567', '555-987-6543', '(555) 234-5678'],
        'date_of_birth': ['1985-03-15', '1990-07-22', '1988-11-08'],
        'address': ['123 Main St New York NY 10001', '', '456 Oak Ave Los Angeles CA 90001'],
        'income': [75000.0, 95000.0, None],
        'account_status': ['active', 'active', 'suspended'],
        'created_date': ['2024-01-10', '2024-01-11', '2024-01-12']
    })


@pytest.fixture
def temp_csv_file(sample_valid_df):
    """Create a temporary CSV file with valid data."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        sample_valid_df.to_csv(f, index=False)
        filepath = f.name
    yield filepath
    os.unlink(filepath)


@pytest.fixture
def temp_output_dir():
    """Create a temporary output directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def test_config():
    """Test configuration dictionary."""
    return {
        'pipeline': {
            'input_file': 'data/customers_raw.csv',
            'output_dir': 'output',
            'log_file': 'output/pipeline.log',
            'log_level': 'DEBUG',
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
    }


@pytest.fixture(autouse=True)
def reset_config_cache():
    """Reset config cache before each test."""
    reset_config()
    yield
    reset_config()
