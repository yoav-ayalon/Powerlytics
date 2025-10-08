# Powerlytics - Phase 2: Complete Data Processing and Aggregation System

A comprehensive electricity consumption data analysis project that processes smart meter CSV files and provides multi-level aggregated datasets for analysis and visualization.

## Project Overview

Powerlytics analyzes household electricity consumption data from smart meters with a structured, automated, and scalable approach. The system's architecture allows seamless exploration of electricity consumption patterns at different time resolutions — from 15-minute readings up to yearly and seasonal trends.

**Phase 2** implements a complete aggregation framework that automatically computes all time-based summaries when the program runs, providing immediate access to multi-level analysis datasets.

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

### Phase 2 Implementation - Complete Aggregation System
- **Automated Multi-Level Aggregation**: All time-based summaries computed automatically
- **8 Aggregation Levels**: Hourly, daily, weekly, monthly, seasonal, yearly, hour-of-day, and day-of-week
- **Data Consistency Validation**: Ensures aggregation accuracy across all levels
- **Memory-Efficient Processing**: All datasets stored in unified dictionary structure
- **Ready for Visualization**: Pre-computed summaries enable instant analysis

### Core Data Processing (Phase 1)
- **Data Loading**: Intelligent CSV parsing that skips metadata rows
- **Data Cleaning**: Standardizes column names and data types
- **Data Validation**: Ensures data integrity and structure
- **Modular Design**: Reusable functions for future development phases

### Data Processing Pipeline
1. Load raw CSV data while skipping metadata (first 12 rows)
2. Clean and standardize column names (Date, Hour, KWH)
3. Convert data types (datetime, float)
4. Remove invalid or empty records
5. **Compute all aggregation levels automatically**
6. Validate consistency across aggregation levels
7. Store unified aggregations dictionary in memory

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

### Quick Start (Complete System with Aggregation)
```bash
# Initialize the system with full aggregation processing
python main.py
```

### Advanced Usage with Aggregation Functions
```python
from src.powerlytics import process_electricity_data, aggregate_all_levels

# Process complete pipeline with all aggregations
df_clean, aggregations = process_electricity_data("data/oct24-oct25.csv")

# Access specific aggregation levels
daily_data = aggregations['daily']
monthly_data = aggregations['monthly']
hourly_patterns = aggregations['hour_of_day']
seasonal_trends = aggregations['seasonal']

# Or compute individual aggregations
from src.powerlytics import aggregate_data
daily_summary = aggregate_data(df_clean, "daily")
```

### Individual Function Usage
```python
from src.powerlytics import load_raw_data, clean_raw_data

# Load and clean data only
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

### Aggregation Levels and Output Structure

| Aggregation | Description | Output Columns | Records |
|-------------|-------------|----------------|---------|
| **Hourly** | Sum of 15-minute readings per hour | `Date`, `Hour`, `KWH` | ~8,760 |
| **Daily** | Total daily consumption | `Date`, `KWH` | ~365 |
| **Weekly** | Weekly consumption (Sunday-Saturday) | `Week`, `Year`, `KWH` | ~52 |
| **Monthly** | Monthly consumption totals | `Month`, `Year`, `KWH` | ~12 |
| **Seasonal** | Seasonal consumption (Israel climate) | `Season`, `Year`, `KWH` | ~8 |
| **Yearly** | Annual consumption totals | `Year`, `KWH` | ~2 |
| **Hour of Day** | Average consumption per clock hour | `Hour`, `Avg_KWH` | 24 |
| **Day of Week** | Average consumption per weekday | `DayOfWeek`, `Avg_KWH` | 7 |

#### Season Definitions (Israel - Coastal Climate)
- **Winter:** December to February
- **Spring:** March to May  
- **Summer:** June to September
- **Autumn:** October to November

## API Reference

### Core Functions

## API Reference

### Core Functions

#### `process_electricity_data(file_path: str, verbose: bool = True) -> tuple[pd.DataFrame, Dict[str, pd.DataFrame]]`
Complete data processing pipeline that includes automatic multi-level aggregation.

**Parameters:**
- `file_path` (str): Path to the CSV file
- `verbose` (bool): Whether to print progress information

**Returns:**
- `tuple`: (df_clean, aggregations) where df_clean is the base dataset and aggregations contains all computed levels

#### `aggregate_all_levels(df: pd.DataFrame) -> Dict[str, pd.DataFrame]`
Automatically compute all aggregation levels during runtime.

**Parameters:**
- `df` (pd.DataFrame): Clean electricity data DataFrame

**Returns:**
- `Dict[str, pd.DataFrame]`: Dictionary with keys: hourly, daily, weekly, monthly, seasonal, yearly, hour_of_day, day_of_week

#### `aggregate_data(df: pd.DataFrame, level: str = "daily") -> pd.DataFrame`
Perform aggregation for a specific time level.

**Parameters:**
- `df` (pd.DataFrame): Clean electricity data DataFrame
- `level` (str): Aggregation level (hourly, daily, weekly, monthly, seasonal, yearly, hour_of_day, day_of_week)

**Returns:**
- `pd.DataFrame`: Aggregated DataFrame for the specified level

#### `get_aggregation_summary(aggregations: Dict[str, pd.DataFrame]) -> Dict[str, Dict]`
Generate summary statistics for all aggregation levels.

**Parameters:**
- `aggregations` (Dict[str, pd.DataFrame]): All computed aggregations

**Returns:**
- `Dict[str, Dict]`: Summary with record counts and consumption statistics for each level

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

The current implementation provides comprehensive data processing and aggregation:
- **Phase 1**: ✅ Data loading and cleaning foundation
- **Phase 2**: ✅ Complete multi-level aggregation system
- **Phase 3**: 🔄 Data visualization and interactive reporting
- **Phase 4**: 📋 Advanced analytics and predictive insights
- **Phase 5**: 🌐 Web dashboard and real-time monitoring

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