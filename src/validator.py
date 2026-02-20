"""
Data Validator Module (Pandera Implementation)
Validates data against schema rules using Pandera library.
"""

import pandas as pd
import pandera as pa
from pandera import Column, Check, DataFrameSchema
from pandera.errors import SchemaErrors
import re
from typing import Dict, List, Tuple
from dataclasses import dataclass

from config import get_config


@dataclass
class ValidationFailure:
    row: int
    column: str
    rule: str
    value: any
    message: str


def is_valid_name(s: pd.Series) -> pd.Series:
    """Check if name contains only alphabetic characters."""
    pattern = re.compile(r'^[a-zA-Z\s\-\']+$')
    return s.apply(lambda x: bool(pattern.match(str(x))) if pd.notna(x) and str(x).strip() else False)


def is_valid_email(s: pd.Series) -> pd.Series:
    """Check if email format is valid."""
    pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    return s.apply(lambda x: bool(pattern.match(str(x).strip().lower())) if pd.notna(x) else False)


def is_valid_date(s: pd.Series) -> pd.Series:
    """Check if date is in YYYY-MM-DD format."""
    pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    return s.apply(lambda x: bool(pattern.match(str(x).strip())) if pd.notna(x) and 'invalid' not in str(x).lower() else False)


def is_valid_status(valid_statuses: set):
    """Create status validator with configurable valid values."""
    def validator(s: pd.Series) -> pd.Series:
        return s.apply(lambda x: str(x).strip().lower() in valid_statuses if pd.notna(x) else False)
    return validator


def is_valid_phone(s: pd.Series) -> pd.Series:
    """Check if phone has at least 10 digits."""
    return s.apply(lambda x: len(re.sub(r'\D', '', str(x))) >= 10 if pd.notna(x) else False)


def create_customer_schema(config: dict) -> DataFrameSchema:
    """Create Pandera schema from configuration."""
    validation_cfg = config.get('validation', {})
    name_min = validation_cfg.get('name_min_length', 2)
    name_max = validation_cfg.get('name_max_length', 50)
    address_min = validation_cfg.get('address_min_length', 10)
    address_max = validation_cfg.get('address_max_length', 200)
    max_income = validation_cfg.get('max_income', 10_000_000)
    valid_statuses = set(validation_cfg.get('valid_statuses', ['active', 'inactive', 'suspended']))
    
    return DataFrameSchema(
        {
            "customer_id": Column(
                nullable=False,
                checks=[
                    Check(lambda s: s > 0, error="customer_id must be positive"),
                    Check(lambda s: s.is_unique, error="customer_id must be unique"),
                ],
                coerce=True,
            ),
            "first_name": Column(
                str,
                nullable=False,
                checks=[
                    Check(lambda s: s.str.strip().str.len() >= name_min, error=f"first_name must be at least {name_min} chars"),
                    Check(lambda s: s.str.strip().str.len() <= name_max, error=f"first_name must be at most {name_max} chars"),
                    Check(is_valid_name, error="first_name must be alphabetic"),
                ],
            ),
            "last_name": Column(
                str,
                nullable=False,
                checks=[
                    Check(lambda s: s.str.strip().str.len() >= name_min, error=f"last_name must be at least {name_min} chars"),
                    Check(lambda s: s.str.strip().str.len() <= name_max, error=f"last_name must be at most {name_max} chars"),
                    Check(is_valid_name, error="last_name must be alphabetic"),
                ],
            ),
            "email": Column(
                str,
                nullable=False,
                checks=[
                    Check(is_valid_email, error="email must be valid format"),
                ],
            ),
            "phone": Column(
                str,
                nullable=False,
                checks=[
                    Check(is_valid_phone, error="phone must have at least 10 digits"),
                ],
            ),
            "date_of_birth": Column(
                str,
                nullable=False,
                checks=[
                    Check(is_valid_date, error="date_of_birth must be YYYY-MM-DD format"),
                ],
            ),
            "address": Column(
                str,
                nullable=False,
                checks=[
                    Check(lambda s: s.str.strip().str.len() >= address_min, error=f"address must be at least {address_min} chars"),
                    Check(lambda s: s.str.strip().str.len() <= address_max, error=f"address must be at most {address_max} chars"),
                ],
            ),
            "income": Column(
                float,
                nullable=False,
                checks=[
                    Check(lambda s: s >= 0, error="income must be non-negative"),
                    Check(lambda s: s <= max_income, error=f"income must be <= ${max_income:,}"),
                ],
                coerce=True,
            ),
            "account_status": Column(
                str,
                nullable=False,
                checks=[
                    Check(is_valid_status(valid_statuses), error=f"account_status must be one of: {', '.join(valid_statuses)}"),
                ],
            ),
            "created_date": Column(
                str,
                nullable=False,
                checks=[
                    Check(is_valid_date, error="created_date must be YYYY-MM-DD format"),
                ],
            ),
        },
        strict=False,
        coerce=True,
    )


class DataValidator:
    """Validates DataFrame against Pandera schema."""
    
    def __init__(self, df: pd.DataFrame, config: dict = None):
        self.df = df.copy()
        self.failures: List[ValidationFailure] = []
        self.config = config or get_config()
        self.schema = create_customer_schema(self.config)
    
    def validate(self) -> Tuple[int, int, List[ValidationFailure]]:
        """Run Pandera validation and return results."""
        self.failures = []
        failed_rows = set()
        
        try:
            self.schema.validate(self.df, lazy=True)
        except SchemaErrors as err:
            failure_df = err.failure_cases
            for _, row in failure_df.iterrows():
                row_idx = row.get('index', 0)
                col = row.get('column', 'unknown')
                check = row.get('check', 'schema')
                failure_case = row.get('failure_case', None)
                
                self.failures.append(ValidationFailure(
                    row=int(row_idx) + 1 if pd.notna(row_idx) else 0,
                    column=str(col),
                    rule=str(check),
                    value=failure_case,
                    message=str(check)
                ))
                if pd.notna(row_idx):
                    failed_rows.add(int(row_idx) + 1)
        
        passed = len(self.df) - len(failed_rows)
        failed = len(failed_rows)
        
        return passed, failed, self.failures
    
    def generate_report(self) -> str:
        """Generate validation results report."""
        passed, failed, failures = self.validate()
        
        report = []
        report.append("VALIDATION RESULTS (Pandera)")
        report.append("=" * 28)
        report.append("")
        report.append(f"PASS: {passed} rows passed all checks")
        report.append(f"FAIL: {failed} rows failed")
        report.append("")
        
        failures_by_column: Dict[str, List[ValidationFailure]] = {}
        for f in failures:
            if f.column not in failures_by_column:
                failures_by_column[f.column] = []
            failures_by_column[f.column].append(f)
        
        report.append("FAILURES BY COLUMN:")
        report.append("-" * 19)
        
        for col, col_failures in sorted(failures_by_column.items()):
            report.append(f"\n{col}:")
            for f in col_failures[:5]:
                val_display = f"'{f.value}'" if isinstance(f.value, str) else f.value
                if pd.isna(f.value) or str(f.value).strip() == '':
                    val_display = 'Empty'
                report.append(f"- Row {f.row}: {val_display} ({f.message})")
            if len(col_failures) > 5:
                report.append(f"  ... and {len(col_failures) - 5} more")
        
        report.append("")
        report.append("SUMMARY BY RULE:")
        report.append("-" * 16)
        
        rule_counts: Dict[str, int] = {}
        for f in failures:
            rule_counts[f.rule] = rule_counts.get(f.rule, 0) + 1
        
        for rule, count in sorted(rule_counts.items(), key=lambda x: -x[1]):
            report.append(f"- {rule}: {count} failures")
        
        return "\n".join(report)


def validate_data(df: pd.DataFrame, config: dict = None) -> Tuple[str, bool]:
    """Main entry point for validation."""
    validator = DataValidator(df, config)
    report = validator.generate_report()
    passed, failed, _ = validator.validate()
    return report, failed == 0
