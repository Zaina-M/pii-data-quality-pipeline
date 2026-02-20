# Data Quality Validation Pipeline

A production-ready Python pipeline for data profiling, PII detection, validation, cleaning, and masking.

## Project Structure

```
Data_Quality_Validation/
|-- config.yaml                 # Configuration settings
|-- pytest.ini                  # Pytest configuration
|-- requirements.txt            # Python dependencies
|-- data/
|   |-- customers_raw.csv       # Raw input data
|-- output/
|   |-- customers_cleaned.csv   # Cleaned and masked output
|   |-- data_quality_report.txt # Part 1 deliverable
|   |-- pii_detection_report.txt# Part 2 deliverable
|   |-- validation_results.txt  # Part 3 deliverable
|   |-- cleaning_log.txt        # Part 4 deliverable
|   |-- masked_sample.txt       # Part 5 deliverable
|   |-- pipeline_execution_report.txt # Part 6 deliverable
|   |-- pipeline.log            # Rotating log file
|-- src/
|   |-- __init__.py             # Package marker
|   |-- config.py               # Configuration management
|   |-- logger.py               # Enhanced logging (rotation, structured)
|   |-- pipeline.py             # Main orchestrator
|   |-- profiler.py             # Data quality profiling
|   |-- pii_detector.py         # PII detection
|   |-- validator.py            # Pandera schema validation
|   |-- cleaner.py              # Data normalization
|   |-- masker.py               # PII masking
|-- tests/
|   |-- conftest.py             # Pytest fixtures
|   |-- test_profiler.py        # Profiler tests
|   |-- test_pii_detector.py    # PII detector tests
|   |-- test_validator.py       # Validator tests
|   |-- test_cleaner.py         # Cleaner tests
|   |-- test_masker.py          # Masker tests
|   |-- test_config.py          # Config tests
|-- learning_guidance.md        # Learning priorities
|-- reflection.md               # Part 7 deliverable
```

## Installation

```bash
pip install -r requirements.txt
```

## Usage

Run the complete pipeline:

```bash
cd src
python pipeline.py
```

With custom paths:

```bash
python pipeline.py --input path/to/data.csv --output path/to/output/
```

## Configuration

Settings are managed via `config.yaml`:

```yaml
pipeline:
  input_file: "data/customers_raw.csv"
  output_dir: "output"
  log_level: "INFO"

validation:
  max_income: 10000000
  valid_statuses: [active, inactive, suspended]

logging:
  console_output: true
  rotation_type: "size"
  max_bytes: 10485760  # 10MB
  backup_count: 5
```

Environment variables override config (prefix with `DQV_`):

```bash
export DQV_LOG_LEVEL=DEBUG
export DQV_MAX_INCOME=5000000
```

## Testing

Run the test suite:

```bash
python -m pytest tests/ -v
```

With coverage:

```bash
python -m pytest tests/ --cov=src --cov-report=html
```

## Pipeline Stages

1. **LOAD**: Read raw CSV data
2. **PROFILE**: Analyze completeness, types, formats, issues
3. **DETECT_PII**: Identify personally identifiable information
4. **CLEAN**: Normalize formats, handle missing values
5. **VALIDATE**: Check against schema rules
6. **MASK**: Protect PII while preserving structure
7. **SAVE**: Output cleaned data and reports

## Validation Rules

| Column | Type | Rules |
|--------|------|-------|
| customer_id | Integer | Unique, positive |
| first_name | String | Non-empty, 2-50 chars, alphabetic |
| last_name | String | Non-empty, 2-50 chars, alphabetic |
| email | String | Valid email format |
| phone | String | Valid phone (normalized to XXX-XXX-XXXX) |
| date_of_birth | Date | Valid date, YYYY-MM-DD |
| address | String | Non-empty, 10-200 chars |
| income | Numeric | Non-negative, <= $10M |
| account_status | String | active, inactive, or suspended |
| created_date | Date | Valid date, YYYY-MM-DD |

## PII Masking Formats

| Field | Example Before | Example After |
|-------|----------------|---------------|
| first_name | John | J*** |
| email | john@gmail.com | j***@gmail.com |
| phone | 555-123-4567 | ***-***-4567 |
| address | 123 Main St NYC | [MASKED ADDRESS] |
| date_of_birth | 1985-03-15 | 1985-**-** |

## Extending the Pipeline

To add new validation rules, edit `src/validator.py` using Pandera:

```python
# Add a new check function
def is_valid_custom_field(series: pd.Series) -> pd.Series:
    return series.apply(lambda x: your_validation_logic(x))

# Add to schema
pa.Column(
    str,
    checks=pa.Check(is_valid_custom_field, error="Custom field validation failed")
)
```

To add new cleaning operations, edit `src/cleaner.py` and add a method following the pattern of `normalize_phone()`.

## Dependencies

- **pandas**: Data manipulation
- **pandera**: Schema validation
- **pyyaml**: Configuration parsing
- **pytest**: Testing framework
- **pytest-cov**: Coverage reporting

## License

Internal use only.
