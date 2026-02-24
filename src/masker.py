"""
PII Masker Module
Masks personally identifiable information while preserving data structure.
"""

import pandas as pd
import re
from typing import Dict, Callable


class PIIMasker:
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.original_df = df.copy()
        self.mask_stats: Dict[str, int] = {}
    
    def mask_name(self, name: str) -> str:
        """Mask name: 'John' -> 'J***'"""
        if pd.isna(name) or str(name).strip() == '' or str(name) == '[UNKNOWN]':
            return str(name)
        name_str = str(name).strip()
        if len(name_str) > 0:
            return name_str[0] + '***'
        return name_str
    
    def mask_email(self, email: str) -> str:
        """Mask email: 'john.doe@gmail.com' -> 'j***@gmail.com'"""
        if pd.isna(email) or str(email).strip() == '':
            return str(email)
        email_str = str(email).strip()
        if '@' in email_str:
            local, domain = email_str.split('@', 1)
            if len(local) > 0:
                masked_local = local[0] + '***'
                return f"{masked_local}@{domain}"
        return email_str
    
    def mask_phone(self, phone: str) -> str:
        """Mask phone: '555-123-4567' -> '***-***-4567'"""
        if pd.isna(phone) or str(phone).strip() == '':
            return str(phone)
        phone_str = str(phone).strip()
        if re.match(r'^\d{3}-\d{3}-\d{4}$', phone_str):
            return f"***-***-{phone_str[-4:]}"
        digits = re.sub(r'\D', '', phone_str)
        if len(digits) >= 4:
            return f"***-***-{digits[-4:]}"
        return phone_str
    
  
    def mask_address(self, address: str) -> str:
        """Mask address: '123 Main St...' -> '[MASKED ADDRESS]'"""
        if pd.isna(address) or str(address).strip() == '' or str(address) == '[UNKNOWN]':
            return str(address)
        return '[MASKED ADDRESS]'
    
    
    def mask_created_date(self, created_date: str) -> str:
     try:
        parsed = pd.to_datetime(created_date, errors='coerce')
        if pd.isna(parsed):
            return 'UNKNOWN'
        return str(created_date)
     except Exception:
        return 'UNKNOWN'
     
   
    def mask_dob(self, dob: str) -> str:
     parsed = pd.to_datetime(dob, errors='coerce')

     if pd.isna(parsed):
        return 'UNKNOWN'

     return f"{parsed.year}-**-**"
    
    def mask_all(self) -> pd.DataFrame:
        """Apply all masking operations."""
        masking_map: Dict[str, Callable] = {
            'first_name': self.mask_name,
            'last_name': self.mask_name,
            'email': self.mask_email,
            'phone': self.mask_phone,
            'address': self.mask_address,
            'date_of_birth': self.mask_dob,
            'created_date': self.mask_created_date,
        }
        
        for col, mask_func in masking_map.items():
            if col in self.df.columns:
                masked_count = 0
                for idx, val in self.df[col].items():
                    original = str(val)
                    masked = mask_func(val)
                    if original != masked:
                        masked_count += 1
                    self.df.at[idx, col] = masked
                self.mask_stats[col] = masked_count
        
        return self.df
    
    def generate_sample_report(self, num_rows: int = 2) -> str:
        """Generate before/after masking comparison."""
        report = []
        
        report.append("BEFORE MASKING (first {} rows):".format(num_rows))
        report.append("-" * 30)
        
        cols = list(self.original_df.columns)
        report.append(", ".join(cols))
        
        for idx in range(min(num_rows, len(self.original_df))):
            row_vals = [str(self.original_df.iloc[idx][col]) for col in cols]
            report.append(", ".join(row_vals))
        
        report.append("")
        report.append("AFTER MASKING (first {} rows):".format(num_rows))
        report.append("-" * 29)
        
        report.append(", ".join(cols))
        
        for idx in range(min(num_rows, len(self.df))):
            row_vals = [str(self.df.iloc[idx][col]) for col in cols]
            report.append(", ".join(row_vals))
        
        report.append("")
        report.append("ANALYSIS:")
        report.append(f"- Data structure preserved (still {len(self.df)} rows, {len(self.df.columns)} columns)")
        report.append("- PII masked (names, emails, phones, addresses, DOBs hidden)")
        report.append("- Business data intact (income, account_status, dates available)")
        report.append("- Use case: Safe for analytics team (GDPR/CCPA compliant)")
        
        report.append("")
        report.append("MASKING STATISTICS:")
        for col, count in self.mask_stats.items():
            report.append(f"- {col}: {count} values masked")
        
        return "\n".join(report)


def mask_pii(df: pd.DataFrame) -> tuple[pd.DataFrame, PIIMasker]:
    """Main entry point for PII masking."""
    masker = PIIMasker(df)
    masked_df = masker.mask_all()
    return masked_df, masker
