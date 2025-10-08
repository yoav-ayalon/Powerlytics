# Powerlytics - Phase 1: Data Processing Foundation

A modular electricity consumption data analysis project that processes smart meter CSV files and prepares clean datasets for analysis.

## Project Overview

Powerlytics is designed to analyze household electricity consumption data from smart meters. Phase 1 establishes the foundation by implementing data loading and cleaning capabilities with a clean, modular architecture.

## Project Structure

```
Powerlytics/
│
├── data/
│   └── oct24-oct25.csv          # Raw electricity meter data
│
├── src/
│   └── powerlytics.py           # Core data processing functions
│
├── main.py                      # Main execution script
│
└── README.md                    # This documentation
```

## Features

### Phase 1 Implementation
- **Data Loading**: Intelligent CSV parsing that skips metadata rows
- **Data Cleaning**: Standardizes column names and data types
- **Data Validation**: Ensures data integrity and structure
- **Modular Design**: Reusable functions for future development phases

### Data Processing Pipeline
1. Load raw CSV data while skipping metadata (first 12 rows)
2. Clean and standardize column names (Date, Hour, KWH)
3. Convert data types (datetime, float)
4. Remove invalid or empty records
5. Validate final dataset structure

## Installation and Setup

### Prerequisites
- Python 3.7+
- pandas library

### Installation
```bash
# Clone the repository
git clone <repository-url>
cd Powerlytics

# Install required packages
pip install pandas

# Ensure your CSV file is in the data/ directory
# The file should be named: oct24-oct25.csv
```

## Usage

### Quick Start (Database-Ready)
```bash
# Initialize the system and prepare data for database
python main.py
```

### Advanced Usage with Individual Functions
```python
from src.powerlytics import process_electricity_data, get_data_summary

# Process complete pipeline
df_clean = process_electricity_data("data/oct24-oct25.csv")

# Get data summary
summary = get_data_summary(df_clean)

# Or use individual functions
from src.powerlytics import load_raw_data, clean_raw_data
df_raw = load_raw_data("data/oct24-oct25.csv")
df_clean = clean_raw_data(df_raw)
```

## Data Format

### Input CSV Structure
- **Rows 1-12**: Metadata and headers (automatically skipped)
- **Row 13+**: Actual consumption data with columns:
  - תאריך (Date): DD/MM/YYYY format
  - מועד תחילת הפעימה (Start Time): HH:MM format (15-minute intervals)
  - צריכה בקוט"ש (Consumption): Numeric value in kWh

### Output DataFrame Structure
| Column | Type | Description |
|--------|------|-------------|
| Date | datetime | Calendar date of measurement |
| Hour | time | Start time of 15-minute interval |
| KWH | float | Electricity consumption in kWh |

## API Reference

### Core Functions

#### `process_electricity_data(file_path: str, verbose: bool = True) -> pd.DataFrame`
Complete data processing pipeline that orchestrates loading, cleaning, and validation.

**Parameters:**
- `file_path` (str): Path to the CSV file
- `verbose` (bool): Whether to print progress information

**Returns:**
- `pd.DataFrame`: Clean, validated DataFrame ready for database operations

#### `get_data_summary(df: pd.DataFrame) -> dict`
Generate comprehensive summary statistics for the electricity consumption data.

**Parameters:**
- `df` (pd.DataFrame): Clean electricity data DataFrame

**Returns:**
- `dict`: Summary with consumption stats, data quality metrics, and metadata

#### `load_raw_data(file_path: str) -> pd.DataFrame`
Loads raw CSV data while skipping metadata rows.

**Parameters:**
- `file_path` (str): Path to the CSV file

**Returns:**
- `pd.DataFrame`: Raw data with original column names

**Raises:**
- `FileNotFoundError`: If file doesn't exist
- `pd.errors.EmptyDataError`: If file is empty

#### `clean_raw_data(df: pd.DataFrame) -> pd.DataFrame`
Cleans and standardizes the raw dataset.

**Parameters:**
- `df` (pd.DataFrame): Raw DataFrame from load_raw_data()

**Returns:**
- `pd.DataFrame`: Cleaned DataFrame with standard columns

**Raises:**
- `ValueError`: If DataFrame structure is unexpected

#### `validate_cleaned_data(df: pd.DataFrame) -> bool`
Validates the cleaned DataFrame structure.

**Parameters:**
- `df` (pd.DataFrame): Cleaned DataFrame to validate

**Returns:**
- `bool`: True if validation passes

**Raises:**
- `ValueError`: If validation fails

## Development Guidelines

### Code Style
- Follow PEP 8 guidelines
- Use type hints for all function parameters and returns
- Include comprehensive docstrings
- Maintain modular, single-responsibility functions

### Architecture Principles
- **Modularity**: Each function has a single, clear purpose
- **Reusability**: Functions can be used independently
- **Extensibility**: Code structure supports future enhancements
- **Error Handling**: Robust error handling with informative messages

## Future Phases

The current implementation provides the foundation for upcoming phases:
- **Phase 2**: Data aggregation and summary statistics
- **Phase 3**: Data visualization and reporting
- **Phase 4**: Advanced analytics and insights

## Troubleshooting

### Common Issues

1. **File Not Found Error**
   - Ensure CSV file is in the `data/` directory
   - Check file name matches exactly: `oct24-oct25.csv`

2. **Encoding Issues**
   - The system uses UTF-8 encoding for Hebrew text
   - Ensure your CSV file is properly encoded

3. **Data Type Conversion Errors**
   - Check that date formats match DD/MM/YYYY
   - Verify time formats are HH:MM
   - Ensure numeric values are properly formatted

### Getting Help

If you encounter issues:
1. Check the error messages for specific guidance
2. Verify your CSV file structure matches the expected format
3. Ensure all dependencies are properly installed

## License

This project is part of the Powerlytics electricity analysis suite.