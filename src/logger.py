"""
Enhanced logging configuration for the Data Quality Validation Pipeline.
Supports file rotation, console output, and structured logging.
"""

import logging
import json
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path
from typing import Optional, Dict, Any


class StructuredFormatter(logging.Formatter):
    """Formatter that outputs JSON-structured logs."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        if hasattr(record, 'extra_data'):
            log_entry['data'] = record.extra_data
        
        return json.dumps(log_entry)


class ColoredFormatter(logging.Formatter):
    """Formatter with color-coded log levels for console output."""
    
    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname_colored = f'{color}{record.levelname}{self.RESET}'
        return super().format(record)


class PipelineLogger:
    """Enhanced logger with structured logging and rotation support."""
    
    def __init__(
        self,
        name: str = 'pipeline',
        log_dir: str = 'output',
        log_file: str = 'pipeline.log',
        level: str = 'INFO',
        console_output: bool = True,
        structured: bool = False,
        max_bytes: int = 10 * 1024 * 1024,  # 10MB
        backup_count: int = 5,
        rotation_type: str = 'size',  # 'size' or 'time'
        rotation_interval: str = 'midnight'
    ):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        self.logger.handlers.clear()
        
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        log_file_path = log_path / log_file
        
        if rotation_type == 'size':
            file_handler = RotatingFileHandler(
                log_file_path,
                maxBytes=max_bytes,
                backupCount=backup_count
            )
        else:
            file_handler = TimedRotatingFileHandler(
                log_file_path,
                when=rotation_interval,
                backupCount=backup_count
            )
        
        if structured:
            file_handler.setFormatter(StructuredFormatter())
        else:
            file_handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            ))
        
        self.logger.addHandler(file_handler)
        
        if console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
            
            if sys.stdout.isatty():
                console_handler.setFormatter(ColoredFormatter(
                    '%(asctime)s - %(levelname_colored)s - %(message)s',
                    datefmt='%H:%M:%S'
                ))
            else:
                console_handler.setFormatter(logging.Formatter(
                    '%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%H:%M:%S'
                ))
            
            self.logger.addHandler(console_handler)
    
    def get_logger(self) -> logging.Logger:
        """Return the configured logger instance."""
        return self.logger
    
    def log_with_data(
        self,
        level: int,
        message: str,
        data: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log a message with additional structured data."""
        record = self.logger.makeRecord(
            self.logger.name,
            level,
            '',
            0,
            message,
            (),
            None
        )
        if data:
            record.extra_data = data
        self.logger.handle(record)


def create_pipeline_logger(config: Dict[str, Any]) -> logging.Logger:
    """
    Create a pipeline logger from configuration.
    
    Args:
        config: Configuration dictionary with logging settings
        
    Returns:
        Configured logging.Logger instance
    """
    pipeline_config = config.get('pipeline', {})
    logging_config = config.get('logging', {})
    
    # Get log file path from config
    log_file_path = Path(pipeline_config.get('log_file', 'output/logs/pipeline.log'))
    log_dir = str(log_file_path.parent)
    log_file = log_file_path.name
    
    pipeline_logger = PipelineLogger(
        name='pipeline',
        log_dir=log_dir,
        log_file=log_file,
        level=pipeline_config.get('log_level', 'INFO'),
        console_output=logging_config.get('console_output', False),
        structured=logging_config.get('structured', False),
        max_bytes=logging_config.get('max_bytes', 10 * 1024 * 1024),
        backup_count=logging_config.get('backup_count', 5),
        rotation_type=logging_config.get('rotation_type', 'size'),
        rotation_interval=logging_config.get('rotation_interval', 'midnight')
    )
    
    return pipeline_logger.get_logger()


class LogContext:
    """Context manager for logging with timing and automatic status."""
    
    def __init__(self, logger: logging.Logger, operation: str):
        self.logger = logger
        self.operation = operation
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.info(f'Starting: {self.operation}')
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        if exc_type:
            self.logger.error(
                f'Failed: {self.operation} after {elapsed:.2f}s - {exc_val}'
            )
        else:
            self.logger.info(f'Completed: {self.operation} in {elapsed:.2f}s')
        return False
