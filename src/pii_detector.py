"""
PII Detection Module
Identifies personally identifiable information in datasets.
"""

import pandas as pd
import re
from typing import Dict, List, Set
from dataclasses import dataclass


@dataclass
class PIIColumn:
    name: str
    pii_type: str
    risk_level: str
    count: int
    coverage_pct: float


class PIIDetector:
    
    PII_PATTERNS = {
        'email': re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'),
        'phone': re.compile(r'[\d\(\)\-\.\s]{10,}'),
        'ssn': re.compile(r'\d{3}-\d{2}-\d{4}'),
        'credit_card': re.compile(r'\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}'),
        'date': re.compile(r'\d{4}[-/]\d{2}[-/]\d{2}|\d{2}[-/]\d{2}[-/]\d{4}'),
        'address': re.compile(r'\d+\s+[\w\s]+\s+(St|Ave|Rd|Dr|Ln|way|Blvd)', re.IGNORECASE),
    }
    
    KNOWN_PII_COLUMNS = {
        'first_name': ('NAME', 'HIGH'),
        'last_name': ('NAME', 'HIGH'),
        'email': ('CONTACT', 'HIGH'),
        'phone': ('CONTACT', 'HIGH'),
        'date_of_birth': ('PERSONAL', 'HIGH'),
        'address': ('LOCATION', 'HIGH'),
        'ssn': ('IDENTIFIER', 'CRITICAL'),
        'social_security': ('IDENTIFIER', 'CRITICAL'),
    }
    
    SENSITIVE_COLUMNS = {
        'income': ('FINANCIAL', 'MEDIUM'),
        'salary': ('FINANCIAL', 'MEDIUM'),
        'account_balance': ('FINANCIAL', 'MEDIUM'),
    }
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.detected_pii: List[PIIColumn] = []
        
    def detect_by_column_name(self) -> List[PIIColumn]:
        """Detect PII based on column names."""
        detected = []
        for col in self.df.columns:
            col_lower = col.lower()
            if col_lower in self.KNOWN_PII_COLUMNS:
                pii_type, risk = self.KNOWN_PII_COLUMNS[col_lower]
                non_null = self.df[col].notna().sum()
                pct = (non_null / len(self.df)) * 100
                detected.append(PIIColumn(col, pii_type, risk, non_null, round(pct, 1)))
            elif col_lower in self.SENSITIVE_COLUMNS:
                pii_type, risk = self.SENSITIVE_COLUMNS[col_lower]
                non_null = self.df[col].notna().sum()
                pct = (non_null / len(self.df)) * 100
                detected.append(PIIColumn(col, pii_type, risk, non_null, round(pct, 1)))
        return detected
    
    def detect_by_pattern(self) -> Dict[str, List[int]]:
        """Detect PII patterns in data content."""
        pattern_matches = {ptype: [] for ptype in self.PII_PATTERNS}
        
        for idx, row in self.df.iterrows():
            for col in self.df.columns:
                val = str(row[col])
                for ptype, pattern in self.PII_PATTERNS.items():
                    if pattern.search(val):
                        if idx not in pattern_matches[ptype]:
                            pattern_matches[ptype].append(idx)
        
        return {k: v for k, v in pattern_matches.items() if v}
    
    def detect_emails(self) -> Dict[str, any]:
        """Specifically detect and analyze email addresses."""
        email_col = 'email' if 'email' in self.df.columns else None
        if not email_col:
            for col in self.df.columns:
                if self.df[col].astype(str).str.contains('@').any():
                    email_col = col
                    break
        
        if not email_col:
            return {'found': False, 'count': 0, 'rows': []}
        
        valid_emails = []
        for idx, val in self.df[email_col].items():
            if self.PII_PATTERNS['email'].search(str(val)):
                valid_emails.append(idx)
        
        return {
            'found': len(valid_emails) > 0,
            'count': len(valid_emails),
            'coverage': f"{len(valid_emails)*100//len(self.df)}%",
            'rows': [r + 1 for r in valid_emails]
        }
    
    def detect_phones(self) -> Dict[str, any]:
        """Specifically detect and analyze phone numbers."""
        phone_col = 'phone' if 'phone' in self.df.columns else None
        if not phone_col:
            return {'found': False, 'count': 0, 'rows': []}
        
        valid_phones = []
        for idx, val in self.df[phone_col].items():
            digits = re.sub(r'\D', '', str(val))
            if len(digits) >= 10:
                valid_phones.append(idx)
        
        return {
            'found': len(valid_phones) > 0,
            'count': len(valid_phones),
            'coverage': f"{len(valid_phones)*100//len(self.df)}%",
            'rows': [r + 1 for r in valid_phones]
        }
    
    def detect_addresses(self) -> Dict[str, any]:
        """Detect physical addresses."""
        address_col = 'address' if 'address' in self.df.columns else None
        if not address_col:
            return {'found': False, 'count': 0, 'rows': []}
        
        valid_addresses = []
        for idx, val in self.df[address_col].items():
            val_str = str(val).strip()
            if val_str and val_str.lower() not in ['nan', '']:
                valid_addresses.append(idx)
        
        return {
            'found': len(valid_addresses) > 0,
            'count': len(valid_addresses),
            'coverage': f"{len(valid_addresses)*100//len(self.df)}%",
            'rows': [r + 1 for r in valid_addresses]
        }
    
    def detect_dob(self) -> Dict[str, any]:
        """Detect dates of birth."""
        dob_col = 'date_of_birth' if 'date_of_birth' in self.df.columns else None
        if not dob_col:
            return {'found': False, 'count': 0, 'rows': []}
        
        valid_dob = []
        for idx, val in self.df[dob_col].items():
            val_str = str(val).strip()
            if self.PII_PATTERNS['date'].search(val_str):
                valid_dob.append(idx)
        
        return {
            'found': len(valid_dob) > 0,
            'count': len(valid_dob),
            'coverage': f"{len(valid_dob)*100//len(self.df)}%",
            'rows': [r + 1 for r in valid_dob]
        }
    
    def assess_risk(self) -> Dict[str, str]:
        """Assess overall PII risk."""
        risk_assessment = {
            'HIGH': [],
            'MEDIUM': [],
            'LOW': []
        }
        
        pii_cols = self.detect_by_column_name()
        for pii in pii_cols:
            if pii.risk_level in ['HIGH', 'CRITICAL']:
                risk_assessment['HIGH'].append(pii.name)
            elif pii.risk_level == 'MEDIUM':
                risk_assessment['MEDIUM'].append(pii.name)
            else:
                risk_assessment['LOW'].append(pii.name)
        
        return risk_assessment
    
    def generate_report(self) -> str:
        """Generate the PII detection report."""
        pii_columns = self.detect_by_column_name()
        patterns = self.detect_by_pattern()
        risk = self.assess_risk()
        
        emails = self.detect_emails()
        phones = self.detect_phones()
        addresses = self.detect_addresses()
        dob = self.detect_dob()
        
        report = []
        report.append("PII DETECTION REPORT")
        report.append("=" * 22)
        report.append("")
        
        report.append("RISK ASSESSMENT:")
        if risk['HIGH']:
            report.append(f"- HIGH: {', '.join(risk['HIGH'])}")
        if risk['MEDIUM']:
            report.append(f"- MEDIUM: {', '.join(risk['MEDIUM'])}")
        if risk['LOW']:
            report.append(f"- LOW: {', '.join(risk['LOW'])}")
        report.append("")
        
        report.append("DETECTED PII:")
        report.append(f"- Emails found: {emails['count']} ({emails['coverage']})")
        report.append(f"- Phone numbers found: {phones['count']} ({phones['coverage']})")
        report.append(f"- Addresses found: {addresses['count']} ({addresses['coverage']})")
        report.append(f"- Dates of birth found: {dob['count']} ({dob['coverage']})")
        report.append("")
        
        report.append("PII COLUMN ANALYSIS:")
        for pii in pii_columns:
            report.append(f"- {pii.name}: {pii.pii_type} ({pii.risk_level}) - {pii.coverage_pct}% populated")
        report.append("")
        
        report.append("PATTERN-BASED DETECTION:")
        for ptype, rows in patterns.items():
            report.append(f"- {ptype.upper()} pattern: {len(rows)} rows affected")
        report.append("")
        
        report.append("EXPOSURE RISK:")
        report.append("If this dataset were breached, attackers could:")
        if emails['found']:
            report.append("- Phish customers (have emails)")
        if phones['found'] or dob['found'] or addresses['found']:
            report.append("- Spoof identities (have names + DOB + address)")
        if phones['found']:
            report.append("- Social engineer (have phone numbers)")
        report.append("")
        
        report.append("MITIGATION RECOMMENDATIONS:")
        report.append("- Mask all PII before sharing with analytics teams")
        report.append("- Implement role-based access control")
        report.append("- Encrypt PII columns at rest")
        report.append("- Audit access to PII data")
        report.append("- Consider tokenization for sensitive identifiers")
        
        return "\n".join(report)


def detect_pii(df: pd.DataFrame) -> str:
    """Main entry point for PII detection."""
    detector = PIIDetector(df)
    return detector.generate_report()
