"""
Data Profiler Module
Analyzes raw data for completeness, types, formats, and quality issues.
"""

import pandas as pd
import re
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass


@dataclass
class QualityIssue:
    issue_type: str
    column: str
    severity: str
    description: str
    affected_rows: List[int]
    examples: List[Any]


class DataProfiler:
    
    EXPECTED_TYPES = {
        'customer_id': 'INT',
        'first_name': 'STRING',
        'last_name': 'STRING',
        'email': 'STRING',
        'phone': 'STRING',
        'date_of_birth': 'DATE',
        'address': 'STRING',
        'income': 'NUMERIC',
        'account_status': 'STRING',
        'created_date': 'DATE'
    }
    
    VALID_STATUSES = {'active', 'inactive', 'suspended'}
    DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    ALT_DATE_PATTERN = re.compile(r'^\d{2}/\d{2}/\d{4}$')
    SLASH_DATE_PATTERN = re.compile(r'^\d{4}/\d{2}/\d{2}$')
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.issues: List[QualityIssue] = []
        
    def calculate_completeness(self) -> Dict[str, Tuple[float, int]]:
        """Calculate completeness percentage for each column."""
        completeness = {}
        for col in self.df.columns:
            total = len(self.df)
            missing = self.df[col].isna().sum() + (self.df[col].astype(str).str.strip() == '').sum()
            pct = ((total - missing) / total) * 100
            completeness[col] = (round(pct, 1), int(missing))
        return completeness
    
    def detect_types(self) -> Dict[str, Tuple[str, bool]]:
        """Detect actual vs expected data types."""
        type_analysis = {}
        for col in self.df.columns:
            expected = self.EXPECTED_TYPES.get(col, 'UNKNOWN')
            actual = self._infer_type(col)
            matches = (expected == actual) or (expected == 'DATE' and actual == 'STRING')
            type_analysis[col] = (actual, matches if expected != 'DATE' else actual == 'DATE')
        return type_analysis
    
    def _infer_type(self, col: str) -> str:
        """Infer the actual type of a column."""
        sample = self.df[col].dropna()
        if sample.empty:
            return 'UNKNOWN'
        
        if pd.api.types.is_integer_dtype(self.df[col]):
            return 'INT'
        if pd.api.types.is_float_dtype(self.df[col]):
            return 'NUMERIC'
        
        sample_str = sample.astype(str)
        if all(self.DATE_PATTERN.match(str(v)) for v in sample_str if str(v).strip()):
            return 'DATE'
        
        return 'STRING'
    
    def analyze_phone_formats(self) -> Dict[str, List[int]]:
        """Identify different phone number formats."""
        formats = {
            'XXX-XXX-XXXX': [],
            '(XXX) XXX-XXXX': [],
            'XXX.XXX.XXXX': [],
            'XXXXXXXXXX': [],
            'OTHER': []
        }
        
        for idx, phone in self.df['phone'].items():
            phone_str = str(phone).strip()
            if re.match(r'^\d{3}-\d{3}-\d{4}$', phone_str):
                formats['XXX-XXX-XXXX'].append(idx)
            elif re.match(r'^\(\d{3}\)\s*\d{3}-\d{4}$', phone_str):
                formats['(XXX) XXX-XXXX'].append(idx)
            elif re.match(r'^\d{3}\.\d{3}\.\d{4}$', phone_str):
                formats['XXX.XXX.XXXX'].append(idx)
            elif re.match(r'^\d{10}$', phone_str):
                formats['XXXXXXXXXX'].append(idx)
            else:
                formats['OTHER'].append(idx)
        
        return {k: v for k, v in formats.items() if v}
    
    def analyze_date_formats(self) -> Dict[str, Dict[str, List[int]]]:
        """Analyze date formats in date columns."""
        date_cols = ['date_of_birth', 'created_date']
        analysis = {}
        
        for col in date_cols:
            analysis[col] = {
                'YYYY-MM-DD': [],
                'YYYY/MM/DD': [],
                'MM/DD/YYYY': [],
                'INVALID': []
            }
            for idx, val in self.df[col].items():
                val_str = str(val).strip()
                if self.DATE_PATTERN.match(val_str):
                    analysis[col]['YYYY-MM-DD'].append(idx)
                elif self.SLASH_DATE_PATTERN.match(val_str):
                    analysis[col]['YYYY/MM/DD'].append(idx)
                elif self.ALT_DATE_PATTERN.match(val_str):
                    analysis[col]['MM/DD/YYYY'].append(idx)
                else:
                    analysis[col]['INVALID'].append(idx)
        
        return analysis
    
    def check_uniqueness(self) -> Dict[str, Tuple[bool, int]]:
        """Check uniqueness of customer_id."""
        unique_count = self.df['customer_id'].nunique()
        total_count = len(self.df)
        duplicates = total_count - unique_count
        return {'customer_id': (duplicates == 0, duplicates)}
    
    def find_quality_issues(self) -> List[QualityIssue]:
        """Identify all quality issues in the dataset."""
        self.issues = []
        
        self._check_missing_values()
        self._check_invalid_dates()
        self._check_invalid_status()
        self._check_name_issues()
        self._check_income_issues()
        self._check_phone_format_issues()
        self._check_email_issues()
        
        return self.issues
    
    def _check_missing_values(self):
        """Check for missing values."""
        for col in ['first_name', 'last_name', 'address', 'income', 'account_status']:
            missing_mask = self.df[col].isna() | (self.df[col].astype(str).str.strip() == '')
            if missing_mask.any():
                affected = self.df[missing_mask].index.tolist()
                self.issues.append(QualityIssue(
                    issue_type='MISSING_VALUE',
                    column=col,
                    severity='High' if col in ['first_name', 'last_name'] else 'Medium',
                    description=f'Missing {col} values',
                    affected_rows=[r + 1 for r in affected],
                    examples=[f'Row {r+1}: empty' for r in affected[:3]]
                ))
    
    def _check_invalid_dates(self):
        """Check for invalid date values."""
        for col in ['date_of_birth', 'created_date']:
            invalid_mask = self.df[col].astype(str).str.contains('invalid', case=False, na=False)
            if invalid_mask.any():
                affected = self.df[invalid_mask].index.tolist()
                examples = [f"Row {r+1}: '{self.df.loc[r, col]}'" for r in affected]
                self.issues.append(QualityIssue(
                    issue_type='INVALID_VALUE',
                    column=col,
                    severity='Critical',
                    description=f'Invalid date strings in {col}',
                    affected_rows=[r + 1 for r in affected],
                    examples=examples
                ))
            
            wrong_format = []
            for idx, val in self.df[col].items():
                val_str = str(val).strip()
                if not self.DATE_PATTERN.match(val_str) and 'invalid' not in val_str.lower():
                    if val_str and val_str.lower() != 'nan':
                        wrong_format.append(idx)
            
            if wrong_format:
                examples = [f"Row {r+1}: '{self.df.loc[r, col]}'" for r in wrong_format[:3]]
                self.issues.append(QualityIssue(
                    issue_type='FORMAT_ERROR',
                    column=col,
                    severity='High',
                    description=f'Non-standard date format in {col}',
                    affected_rows=[r + 1 for r in wrong_format],
                    examples=examples
                ))
    
    def _check_invalid_status(self):
        """Check for invalid account status values."""
        invalid_mask = ~self.df['account_status'].astype(str).str.strip().str.lower().isin(
            self.VALID_STATUSES | {'', 'nan'}
        )
        if invalid_mask.any():
            affected = self.df[invalid_mask].index.tolist()
            examples = [f"Row {r+1}: '{self.df.loc[r, 'account_status']}'" for r in affected]
            self.issues.append(QualityIssue(
                issue_type='INVALID_VALUE',
                column='account_status',
                severity='High',
                description='Invalid account_status value (not in: active, inactive, suspended)',
                affected_rows=[r + 1 for r in affected],
                examples=examples
            ))
    
    def _check_name_issues(self):
        """Check for name format issues."""
        for col in ['first_name', 'last_name']:
            case_issues = []
            for idx, val in self.df[col].items():
                val_str = str(val).strip()
                if val_str and val_str.lower() != 'nan':
                    if val_str.isupper() or val_str.islower():
                        if len(val_str) > 1:
                            case_issues.append(idx)
            
            if case_issues:
                examples = [f"Row {r+1}: '{self.df.loc[r, col]}'" for r in case_issues[:3]]
                self.issues.append(QualityIssue(
                    issue_type='FORMAT_ERROR',
                    column=col,
                    severity='Low',
                    description=f'Inconsistent capitalization in {col}',
                    affected_rows=[r + 1 for r in case_issues],
                    examples=examples
                ))
    
    def _check_income_issues(self):
        """Check for income data issues."""
        try:
            income_numeric = pd.to_numeric(self.df['income'], errors='coerce')
            negative_mask = income_numeric < 0
            if negative_mask.any():
                affected = self.df[negative_mask].index.tolist()
                self.issues.append(QualityIssue(
                    issue_type='INVALID_VALUE',
                    column='income',
                    severity='Critical',
                    description='Negative income values',
                    affected_rows=[r + 1 for r in affected],
                    examples=[f"Row {r+1}: {self.df.loc[r, 'income']}" for r in affected]
                ))
            
            over_limit = income_numeric > 10_000_000
            if over_limit.any():
                affected = self.df[over_limit].index.tolist()
                self.issues.append(QualityIssue(
                    issue_type='OUTLIER',
                    column='income',
                    severity='High',
                    description='Income exceeds $10M limit',
                    affected_rows=[r + 1 for r in affected],
                    examples=[f"Row {r+1}: {self.df.loc[r, 'income']}" for r in affected]
                ))
        except Exception:
            pass
    
    def _check_phone_format_issues(self):
        """Check for non-standard phone formats."""
        non_standard = []
        for idx, phone in self.df['phone'].items():
            phone_str = str(phone).strip()
            if not re.match(r'^\d{3}-\d{3}-\d{4}$', phone_str):
                non_standard.append(idx)
        
        if non_standard:
            examples = [f"Row {r+1}: '{self.df.loc[r, 'phone']}'" for r in non_standard[:3]]
            self.issues.append(QualityIssue(
                issue_type='FORMAT_ERROR',
                column='phone',
                severity='Medium',
                description='Non-standard phone format (should be XXX-XXX-XXXX)',
                affected_rows=[r + 1 for r in non_standard],
                examples=examples
            ))
    
    def _check_email_issues(self):
        """Check for email format issues."""
        uppercase_emails = []
        for idx, email in self.df['email'].items():
            email_str = str(email).strip()
            if email_str != email_str.lower():
                uppercase_emails.append(idx)
        
        if uppercase_emails:
            examples = [f"Row {r+1}: '{self.df.loc[r, 'email']}'" for r in uppercase_emails[:3]]
            self.issues.append(QualityIssue(
                issue_type='FORMAT_ERROR',
                column='email',
                severity='Low',
                description='Email contains uppercase characters',
                affected_rows=[r + 1 for r in uppercase_emails],
                examples=examples
            ))
    
    def generate_report(self) -> str:
        """Generate the complete data quality report."""
        completeness = self.calculate_completeness()
        types = self.detect_types()
        phone_formats = self.analyze_phone_formats()
        date_formats = self.analyze_date_formats()
        uniqueness = self.check_uniqueness()
        issues = self.find_quality_issues()
        
        severity_counts = {'Critical': 0, 'High': 0, 'Medium': 0, 'Low': 0}
        for issue in issues:
            severity_counts[issue.severity] += 1
        
        report = []
        report.append("DATA QUALITY PROFILE REPORT")
        report.append("=" * 27)
        report.append("")
        
        report.append("COMPLETENESS:")
        for col, (pct, missing) in completeness.items():
            status = f"({missing} missing)" if missing > 0 else ""
            report.append(f"- {col}: {pct}% {status}")
        report.append("")
        
        report.append("DATA TYPES:")
        for col, (actual, matches) in types.items():
            expected = self.EXPECTED_TYPES.get(col, 'UNKNOWN')
            symbol = "pass" if matches else "X"
            note = "" if matches else f" (should be {expected})"
            report.append(f"- {col}: {actual} {symbol}{note}")
        report.append("")
        
        report.append("UNIQUENESS:")
        is_unique, dups = uniqueness['customer_id']
        report.append(f"- customer_id: {'UNIQUE' if is_unique else f'NOT UNIQUE ({dups} duplicates)'}")
        report.append("")
        
        report.append("PHONE FORMATS FOUND:")
        for fmt, rows in phone_formats.items():
            report.append(f"- {fmt}: {len(rows)} occurrences (rows: {[r+1 for r in rows]})")
        report.append("")
        
        report.append("DATE FORMATS ANALYSIS:")
        for col, formats in date_formats.items():
            report.append(f"  {col}:")
            for fmt, rows in formats.items():
                if rows:
                    report.append(f"    - {fmt}: {len(rows)} occurrences")
        report.append("")
        
        report.append("QUALITY ISSUES:")
        for i, issue in enumerate(issues, 1):
            report.append(f"{i}. [{issue.severity}] {issue.description} ({issue.column})")
            for ex in issue.examples:
                report.append(f"   Examples: {ex}")
        report.append("")
        
        report.append("SEVERITY SUMMARY:")
        report.append(f"- Critical (blocks processing): {severity_counts['Critical']}")
        report.append(f"- High (data incorrect): {severity_counts['High']}")
        report.append(f"- Medium (needs cleaning): {severity_counts['Medium']}")
        report.append(f"- Low (cosmetic): {severity_counts['Low']}")
        report.append("")
        
        report.append("ESTIMATED IMPACT:")
        total_rows = len(self.df)
        affected_rows = set()
        for issue in issues:
            affected_rows.update(issue.affected_rows)
        report.append(f"- {len(affected_rows)} of {total_rows} rows ({len(affected_rows)*100//total_rows}%) have at least one quality issue")
        
        return "\n".join(report)


def profile_data(filepath: str) -> Tuple[pd.DataFrame, str]:
    """Main entry point for data profiling."""
    df = pd.read_csv(filepath)
    profiler = DataProfiler(df)
    report = profiler.generate_report()
    return df, report
