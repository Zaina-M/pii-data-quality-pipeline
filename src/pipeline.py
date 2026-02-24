"""
Data Quality Validation Pipeline
Main orchestrator that runs all pipeline stages.
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

from profiler import DataProfiler, profile_data
from pii_detector import PIIDetector, detect_pii
from validator import DataValidator, validate_data
from cleaner import DataCleaner, clean_data
from masker import PIIMasker, mask_pii
from config import load_config, Config
from logger import create_pipeline_logger, LogContext


def setup_logging(config: Config) -> logging.Logger:
    """Configure logging based on config settings."""
    config_dict = {
        'pipeline': {
            'output_dir': config.output_dir,
            'log_file': config.log_file,
            'log_level': config.log_level
        },
        'logging': config.raw.get('logging', {})
    }
    return create_pipeline_logger(config_dict)


class PipelineStage:
    """Represents a pipeline execution stage."""
    
    def __init__(self, name: str):
        self.name = name
        self.status = 'PENDING'
        self.message = ''
        self.details = []
    
    def success(self, message: str = '', details: list = None):
        self.status = 'SUCCESS'
        self.message = message
        self.details = details or []
    
    def fail(self, message: str = '', details: list = None):
        self.status = 'FAILED'
        self.message = message
        self.details = details or []


class DataQualityPipeline:
    """
    End-to-end data quality validation pipeline.
    
    Stages:
    1. LOAD - Load raw data
    2. PROFILE - Analyze data quality
    3. DETECT_PII - Identify PII columns
    4. CLEAN - Normalize and fix issues
    5. VALIDATE - Check against schema
    6. MASK - Protect PII
    7. SAVE - Output results
    """
    
    def __init__(self, input_path: str, output_dir: str, config: dict = None, logger: logging.Logger = None):
        self.input_path = Path(input_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.config = config or {}
        
        # Create structured output directories relative to output_dir
        self.csv_dir = self.output_dir / 'csv'
        self.reports_dir = self.output_dir / 'reports'
        self.logs_dir = self.output_dir / 'logs'
        
        self.csv_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        self.df_raw = None
        self.df_cleaned = None
        self.df_masked = None
        
        self.stages = []
        self.reports = {}
        
        self.start_time = None
        self.end_time = None
        
        # Use provided logger or get the configured 'pipeline' logger
        self.logger = logger if logger else logging.getLogger('pipeline')
    
    def run(self) -> bool:
        """Execute all pipeline stages."""
        self.start_time = datetime.now()
        self.logger.info("Starting data quality pipeline")
        
        try:
            self._stage_load()
            self._stage_profile()
            self._stage_detect_pii()
            self._stage_clean()
            self._stage_validate()
            self._stage_mask()
            self._stage_save()
            
            self.end_time = datetime.now()
            self._generate_execution_report()
            
            self.logger.info("Pipeline completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            self.end_time = datetime.now()
            self._generate_execution_report()
            return False
    
    def _stage_load(self):
        """Stage 1: Load raw data."""
        stage = PipelineStage('LOAD')
        self.logger.info("Stage 1: Loading raw data")
        
        try:
            self.df_raw = pd.read_csv(self.input_path)
            rows, cols = self.df_raw.shape
            stage.success(
                f"Loaded {self.input_path.name}",
                [f"{rows} rows, {cols} columns"]
            )
            self.logger.info(f"Loaded {rows} rows, {cols} columns")
        except Exception as e:
            stage.fail(f"Failed to load data: {str(e)}")
            raise
        
        self.stages.append(stage)
    
    def _stage_profile(self):
        """Stage 2: Profile data quality."""
        stage = PipelineStage('PROFILE')
        self.logger.info("Stage 2: Profiling data quality")
        
        try:
            profiler = DataProfiler(self.df_raw)
            report = profiler.generate_report()
            self.reports['data_quality_report'] = report
            
            issues = profiler.find_quality_issues()
            stage.success(
                "Generated data quality profile",
                [f"Found {len(issues)} quality issues"]
            )
            self.logger.info(f"Found {len(issues)} quality issues")
        except Exception as e:
            stage.fail(f"Profiling failed: {str(e)}")
            raise
        
        self.stages.append(stage)
    
    def _stage_detect_pii(self):
        """Stage 3: Detect PII."""
        stage = PipelineStage('DETECT_PII')
        self.logger.info("Stage 3: Detecting PII")
        
        try:
            detector = PIIDetector(self.df_raw)
            report = detector.generate_report()
            self.reports['pii_detection_report'] = report
            
            pii_cols = detector.detect_by_column_name()
            emails = detector.detect_emails()
            phones = detector.detect_phones()
            addresses = detector.detect_addresses()
            dob = detector.detect_dob()
            
            stage.success(
                "Detected PII columns",
                [
                    f"{len(pii_cols)} PII columns identified",
                    f"{emails['count']} email addresses",
                    f"{phones['count']} phone numbers",
                    f"{addresses['count']} addresses",
                    f"{dob['count']} dates of birth"
                ]
            )
            self.logger.info(f"Detected {len(pii_cols)} PII columns")
        except Exception as e:
            stage.fail(f"PII detection failed: {str(e)}")
            raise
        
        self.stages.append(stage)
    
    def _stage_clean(self):
        """Stage 4: Clean and normalize data."""
        stage = PipelineStage('CLEAN')
        self.logger.info("Stage 4: Cleaning data")
        
        try:
            self.df_cleaned, cleaner = clean_data(self.df_raw)
            
            details = []
            for action in cleaner.actions:
                if action.action_type == 'NORMALIZATION':
                    details.append(f"Normalized {action.column} ({action.rows_affected} rows)")
                elif action.action_type == 'MISSING_VALUE_FILL':
                    details.append(f"Filled missing {action.column} ({action.rows_affected} rows)")
            
            validator_before = DataValidator(self.df_raw)
            _, failed_before, _ = validator_before.validate()
            
            validator_after = DataValidator(self.df_cleaned)
            _, failed_after, _ = validator_after.validate()
            
            cleaning_log = cleaner.generate_log(failed_before, failed_after)
            self.reports['cleaning_log'] = cleaning_log
            
            stage.success(
                "Data cleaned and normalized",
                details
            )
            self.logger.info(f"Cleaning complete: {len(cleaner.actions)} actions taken")
        except Exception as e:
            stage.fail(f"Cleaning failed: {str(e)}")
            raise
        
        self.stages.append(stage)
    
    def _stage_validate(self):
        """Stage 5: Validate cleaned data."""
        stage = PipelineStage('VALIDATE')
        self.logger.info("Stage 5: Validating data")
        
        try:
            validator = DataValidator(self.df_cleaned)
            report = validator.generate_report()
            self.reports['validation_results'] = report
            
            passed, failed, failures = validator.validate()
            
            if failed == 0:
                stage.success(
                    "All validation checks passed",
                    [f"{passed}/{passed+failed} rows valid"]
                )
            else:
                failures_by_col = {}
                for f in failures:
                    failures_by_col[f.column] = failures_by_col.get(f.column, 0) + 1
                
                details = [f"{passed}/{passed+failed} rows valid"]
                for col, count in failures_by_col.items():
                    details.append(f"{col}: {count} failures")
                
                stage.success(
                    "Validation complete (some issues remain)",
                    details
                )
            
            self.logger.info(f"Validation: {passed} passed, {failed} failed")
        except Exception as e:
            stage.fail(f"Validation failed: {str(e)}")
            raise
        
        self.stages.append(stage)
    
    def _stage_mask(self):
        """Stage 6: Mask PII."""
        stage = PipelineStage('MASK')
        self.logger.info("Stage 6: Masking PII")
        
        try:
            self.df_masked, masker = mask_pii(self.df_cleaned)
            sample_report = masker.generate_sample_report()
            self.reports['masked_sample'] = sample_report
            
            details = [f"Masked {col}" for col in masker.mask_stats.keys()]
            
            stage.success(
                "PII masked successfully",
                details
            )
            self.logger.info(f"Masked {len(masker.mask_stats)} columns")
        except Exception as e:
            stage.fail(f"Masking failed: {str(e)}")
            raise
        
        self.stages.append(stage)
    
    def _stage_save(self):
        """Stage 7: Save outputs."""
        stage = PipelineStage('SAVE')
        self.logger.info("Stage 7: Saving outputs")
        
        try:
            # Save CSV files to csv/ directory
            output_csv = self.csv_dir / 'customers_cleaned.csv'
            self.df_cleaned.to_csv(output_csv, index=False)
            
            output_masked_csv = self.csv_dir / 'customers_cleaned_masked.csv'
            self.df_masked.to_csv(output_masked_csv, index=False)
            
            # Save report files to reports/ directory
            report_files = []
            for report_name, content in self.reports.items():
                filename = f"{report_name}.txt"
                if report_name == 'cleaning_log':
                    # cleaning_log goes to logs/ directory
                    filepath = self.logs_dir / filename
                else:
                    # other reports go to reports/ directory
                    filepath = self.reports_dir / filename
                with open(filepath, 'w') as f:
                    f.write(content)
                report_files.append(filename)
            
            stage.success(
                "Outputs saved",
                ["customers_cleaned.csv", "customers_cleaned_masked.csv"] + report_files
            )
            self.logger.info(f"Saved {len(report_files) + 2} files to structured output directories")
        except Exception as e:
            stage.fail(f"Save failed: {str(e)}")
            raise
        
        self.stages.append(stage)
    
    def _generate_execution_report(self):
        """Generate final pipeline execution report."""
        report = []
        report.append("PIPELINE EXECUTION REPORT")
        report.append("=" * 25)
        report.append(f"Timestamp: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        for i, stage in enumerate(self.stages, 1):
            status_icon = "[OK]" if stage.status == 'SUCCESS' else "[X]"
            report.append(f"Stage {i}: {stage.name}")
            report.append(f"{status_icon} {stage.message}")
            for detail in stage.details:
                report.append(f"- {detail}")
            report.append("")
        
        passed_stages = sum(1 for s in self.stages if s.status == 'SUCCESS')
        failed_stages = sum(1 for s in self.stages if s.status == 'FAILED')
        
        report.append("SUMMARY:")
        report.append(f"- Input: {self.df_raw.shape[0] if self.df_raw is not None else 0} rows (raw)")
        report.append(f"- Output: {self.df_masked.shape[0] if self.df_masked is not None else 0} rows (clean, masked, validated)")
        report.append(f"- Quality: {'PASS' if failed_stages == 0 else 'PARTIAL'}")
        report.append(f"- PII Risk: MITIGATED (all masked)")
        
        duration = (self.end_time - self.start_time).total_seconds()
        report.append(f"- Duration: {duration:.2f} seconds")
        report.append("")
        
        overall_status = "SUCCESS" if failed_stages == 0 else "FAILED"
        report.append(f"Status: {overall_status}")
        
        execution_report = "\n".join(report)
        
        # Save pipeline execution report to reports/ directory
        filepath = self.reports_dir / 'pipeline_execution_report.txt'
        with open(filepath, 'w') as f:
            f.write(execution_report)
        
        for line in report:
            self.logger.info(line)


def main():
    parser = argparse.ArgumentParser(description='Data Quality Validation Pipeline')
    parser.add_argument(
        '--input',
        default=None,
        help='Path to input CSV file (overrides config)'
    )
    parser.add_argument(
        '--output',
        default=None,
        help='Output directory for results (overrides config)'
    )
    parser.add_argument(
        '--config',
        default=None,
        help='Path to config YAML file'
    )
    
    args = parser.parse_args()
    
    config_dict = load_config(args.config)
    config = Config(config_dict)
    
    script_dir = Path(__file__).parent.parent
    
    # Set up absolute paths for output and logging
    if args.output:
        output_dir = Path(args.output)
    else:
        output_dir = script_dir / config.output_dir
    
    # Create logs directory and set log file path
    logs_dir = output_dir / 'logs'
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file_path = logs_dir / 'pipeline.log'
    
    # Update config with absolute log file path
    config_dict['pipeline']['log_file'] = str(log_file_path)
    config = Config(config_dict)
    
    logger = setup_logging(config)
    
    if args.input:
        input_path = Path(args.input)
    else:
        input_path = script_dir / config.input_file
    
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)
    
    pipeline = DataQualityPipeline(str(input_path), str(output_dir), config_dict, logger)
    success = pipeline.run()
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()

