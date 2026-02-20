"""
Data Cleaner Module
Normalizes formats, handles missing values, and fixes data quality issues.
"""

import pandas as pd
import re
from typing import Dict, List, Tuple
from dataclasses import dataclass
from datetime import datetime


@dataclass
class CleaningAction:
    action_type: str
    column: str
    rows_affected: int
    description: str
    details: List[str]


class DataCleaner:
    
    VALID_STATUSES = {'active', 'inactive', 'suspended'}
    DATE_FORMATS = [
        ('%Y-%m-%d', r'^\d{4}-\d{2}-\d{2}$'),
        ('%Y/%m/%d', r'^\d{4}/\d{2}/\d{2}$'),
        ('%m/%d/%Y', r'^\d{2}/\d{2}/\d{4}$'),
    ]
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.actions: List[CleaningAction] = []
        self.original_df = df.copy()
    
    def normalize_phone(self) -> int:
        """Normalize all phone numbers to XXX-XXX-XXXX format."""
        affected = 0
        details = []
        
        for idx, phone in self.df['phone'].items():
            phone_str = str(phone).strip()
            digits = re.sub(r'\D', '', phone_str)
            
            if len(digits) >= 10:
                normalized = f"{digits[:3]}-{digits[3:6]}-{digits[6:10]}"
                if normalized != phone_str:
                    details.append(f"Row {idx+1}: '{phone_str}' -> '{normalized}'")
                    self.df.at[idx, 'phone'] = normalized
                    affected += 1
        
        if affected > 0:
            self.actions.append(CleaningAction(
                action_type='NORMALIZATION',
                column='phone',
                rows_affected=affected,
                description='Phone format converted to XXX-XXX-XXXX',
                details=details[:5]
            ))
        
        return affected
    
    def normalize_dates(self) -> int:
        """Convert all dates to YYYY-MM-DD format."""
        affected = 0
        details = []
        
        for col in ['date_of_birth', 'created_date']:
            for idx, date_val in self.df[col].items():
                date_str = str(date_val).strip()
                
                if 'invalid' in date_str.lower():
                    details.append(f"Row {idx+1} ({col}): '{date_str}' -> '[INVALID_DATE]'")
                    self.df.at[idx, col] = '[INVALID_DATE]'
                    affected += 1
                    continue
                
                if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                    continue
                
                for fmt, pattern in self.DATE_FORMATS:
                    if re.match(pattern, date_str):
                        try:
                            parsed = datetime.strptime(date_str, fmt)
                            normalized = parsed.strftime('%Y-%m-%d')
                            details.append(f"Row {idx+1} ({col}): '{date_str}' -> '{normalized}'")
                            self.df.at[idx, col] = normalized
                            affected += 1
                            break
                        except ValueError:
                            pass
        
        if affected > 0:
            self.actions.append(CleaningAction(
                action_type='NORMALIZATION',
                column='date_of_birth, created_date',
                rows_affected=affected,
                description='Date format converted to YYYY-MM-DD',
                details=details[:5]
            ))
        
        return affected
    
    def normalize_names(self) -> int:
        """Apply title case to names."""
        affected = 0
        details = []
        
        for col in ['first_name', 'last_name']:
            for idx, name in self.df[col].items():
                name_str = str(name).strip()
                if name_str and name_str.lower() != 'nan' and name_str != '[UNKNOWN]':
                    title_case = name_str.title()
                    if title_case != name_str:
                        details.append(f"Row {idx+1} ({col}): '{name_str}' -> '{title_case}'")
                        self.df.at[idx, col] = title_case
                        affected += 1
        
        if affected > 0:
            self.actions.append(CleaningAction(
                action_type='NORMALIZATION',
                column='first_name, last_name',
                rows_affected=affected,
                description='Name case normalized to title case',
                details=details[:5]
            ))
        
        return affected
    
    def normalize_emails(self) -> int:
        """Lowercase all email addresses."""
        affected = 0
        details = []
        
        for idx, email in self.df['email'].items():
            email_str = str(email).strip()
            if email_str != email_str.lower():
                details.append(f"Row {idx+1}: '{email_str}' -> '{email_str.lower()}'")
                self.df.at[idx, 'email'] = email_str.lower()
                affected += 1
        
        if affected > 0:
            self.actions.append(CleaningAction(
                action_type='NORMALIZATION',
                column='email',
                rows_affected=affected,
                description='Email addresses lowercased',
                details=details[:5]
            ))
        
        return affected
    
    def handle_missing_values(self) -> Dict[str, int]:
        """Handle missing values with appropriate strategies."""
        missing_counts = {}
        
        strategies = {
            'first_name': ('[UNKNOWN]', 'STRING'),
            'last_name': ('[UNKNOWN]', 'STRING'),
            'address': ('[UNKNOWN]', 'STRING'),
            'income': (0, 'NUMERIC'),
            'account_status': ('unknown', 'STRING'),
        }
        
        for col, (fill_value, _) in strategies.items():
            details = []
            affected = 0
            
            for idx, val in self.df[col].items():
                if pd.isna(val) or str(val).strip() == '' or str(val).strip().lower() == 'nan':
                    details.append(f"Row {idx+1}: empty -> '{fill_value}'")
                    self.df.at[idx, col] = fill_value
                    affected += 1
            
            if affected > 0:
                missing_counts[col] = affected
                self.actions.append(CleaningAction(
                    action_type='MISSING_VALUE_FILL',
                    column=col,
                    rows_affected=affected,
                    description=f"Filled with '{fill_value}'",
                    details=details[:3]
                ))
        
        return missing_counts
    
    def clean(self) -> pd.DataFrame:
        """Execute all cleaning operations."""
        self.normalize_phone()
        self.normalize_dates()
        self.normalize_names()
        self.normalize_emails()
        self.handle_missing_values()
        
        return self.df
    
    def generate_log(self, validation_before: int, validation_after: int) -> str:
        """Generate the cleaning log."""
        log = []
        log.append("DATA CLEANING LOG")
        log.append("=" * 17)
        log.append("")
        
        log.append("ACTIONS TAKEN:")
        log.append("-" * 14)
        log.append("")
        
        normalizations = [a for a in self.actions if a.action_type == 'NORMALIZATION']
        if normalizations:
            log.append("Normalization:")
            for action in normalizations:
                log.append(f"- {action.column}: {action.description} ({action.rows_affected} rows affected)")
                for detail in action.details:
                    log.append(f"  {detail}")
            log.append("")
        
        missing_fills = [a for a in self.actions if a.action_type == 'MISSING_VALUE_FILL']
        if missing_fills:
            log.append("Missing Values:")
            for action in missing_fills:
                log.append(f"- {action.column}: {action.rows_affected} row(s) missing -> {action.description}")
            log.append("")
        
        log.append("Validation After Cleaning:")
        log.append(f"- Before: {validation_before} rows failed")
        log.append(f"- After: {validation_after} rows failed")
        log.append(f"- Status: {'PASS' if validation_after == 0 else 'PARTIAL - some issues remain'}")
        log.append("")
        
        log.append(f"Output: customers_cleaned.csv ({len(self.df)} rows, {len(self.df.columns)} columns)")
        
        return "\n".join(log)


def clean_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, DataCleaner]:
    """Main entry point for data cleaning."""
    cleaner = DataCleaner(df)
    cleaned_df = cleaner.clean()
    return cleaned_df, cleaner
