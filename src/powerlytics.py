"""
Powerlytics - Phase 1: Data Loading and Cleaning Module

This module provides core functionality for loading and cleaning electricity
consumption data from smart meter CSV files.

Functions:
    load_raw_data: Load raw CSV data while skipping metadata rows
    clean_raw_data: Clean and standardize the raw dataset
"""

import pandas as pd
from typing import Optional


def load_raw_data(file_path: str) -> pd.DataFrame:
    """
    Load the raw CSV file while skipping irrelevant metadata rows.
    
    The CSV file contains metadata in the first 12 rows which need to be skipped.
    The actual data begins from row 13 (0-indexed row 12).
    
    Args:
        file_path (str): Path to the source CSV file
        
    Returns:
        pd.DataFrame: Raw data DataFrame containing all valid data rows
        
    Raises:
        FileNotFoundError: If the specified file doesn't exist
        pd.errors.EmptyDataError: If the file is empty or contains no valid data
    """
    try:
        # Skip first 12 rows (metadata) and load the remaining data
        df_raw = pd.read_csv(file_path, skiprows=12, encoding='utf-8')
        
        # Remove any completely empty rows
        df_raw = df_raw.dropna(how='all')
        
        # print(f"Successfully loaded {len(df_raw)} rows of raw data from {file_path}")
        return df_raw
        
    except FileNotFoundError:
        raise FileNotFoundError(f"Could not find the file: {file_path}")
    except pd.errors.EmptyDataError:
        raise pd.errors.EmptyDataError(f"The file {file_path} is empty or contains no valid data")
    except Exception as e:
        raise Exception(f"Error loading data from {file_path}: {str(e)}")


def clean_raw_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize the raw dataset.
    
    This function performs the following cleaning operations:
    1. Renames columns to standard English names: Date, Hour, KWH
    2. Converts Date and Hour to appropriate datetime formats
    3. Converts KWH to numeric (float) type
    4. Removes any invalid or empty rows
    
    Args:
        df (pd.DataFrame): Raw DataFrame returned from load_raw_data()
        
    Returns:
        pd.DataFrame: Cleaned DataFrame with standardized columns and data types
        
    Raises:
        ValueError: If the DataFrame doesn't have the expected structure
    """
    try:
        # Make a copy to avoid modifying the original DataFrame
        df_clean = df.copy()
        
        # Get the original column names (should be Hebrew)
        original_columns = df_clean.columns.tolist()
        
        # Verify we have the expected number of columns
        if len(original_columns) < 3:
            raise ValueError(f"Expected at least 3 columns, but found {len(original_columns)}")
        
        # Rename columns to standard English names
        # The columns should be: תאריך, מועד תחילת הפעימה, צריכה בקוט"ש
        column_mapping = {
            original_columns[0]: 'Date',
            original_columns[1]: 'Hour', 
            original_columns[2]: 'KWH'
        }
        df_clean = df_clean.rename(columns=column_mapping)
        
        # Keep only the three main columns
        df_clean = df_clean[['Date', 'Hour', 'KWH']]
        
        # Remove rows where any of the main columns are completely empty
        df_clean = df_clean.dropna(subset=['Date', 'Hour', 'KWH'])
        
        # Convert Date column to datetime
        df_clean['Date'] = pd.to_datetime(df_clean['Date'], format='%d/%m/%Y', errors='coerce')
        
        # Convert Hour column to datetime (time format)
        df_clean['Hour'] = pd.to_datetime(df_clean['Hour'], format='%H:%M', errors='coerce').dt.time
        
        # Convert KWH to numeric (float)
        df_clean['KWH'] = pd.to_numeric(df_clean['KWH'], errors='coerce')
        
        # Remove any rows where conversion failed (NaN values)
        initial_rows = len(df_clean)
        df_clean = df_clean.dropna()
        final_rows = len(df_clean)
        
        if initial_rows != final_rows:
            print(f"Removed {initial_rows - final_rows} rows with invalid data during cleaning")
        
        # Verify we still have data after cleaning
        if len(df_clean) == 0:
            raise ValueError("No valid data remaining after cleaning process")
        
        # print(f"Successfully cleaned data: {len(df_clean)} rows with columns {list(df_clean.columns)}")
        return df_clean
        
    except Exception as e:
        raise Exception(f"Error cleaning data: {str(e)}")


def validate_cleaned_data(df: pd.DataFrame) -> bool:
    """
    Validate that the cleaned DataFrame has the expected structure.
    
    Args:
        df (pd.DataFrame): Cleaned DataFrame to validate
        
    Returns:
        bool: True if validation passes, raises exception otherwise
        
    Raises:
        ValueError: If validation fails
    """
    expected_columns = ['Date', 'Hour', 'KWH']
    
    # Check column names
    if list(df.columns) != expected_columns:
        raise ValueError(f"Expected columns {expected_columns}, but found {list(df.columns)}")
    
    # Check data types
    if not pd.api.types.is_datetime64_any_dtype(df['Date']):
        raise ValueError("Date column should be datetime type")
    
    if not pd.api.types.is_numeric_dtype(df['KWH']):
        raise ValueError("KWH column should be numeric type")
    
    # Check for empty data
    if len(df) == 0:
        raise ValueError("DataFrame is empty")
    
    # print("Data validation passed successfully")
    return True


def process_electricity_data(file_path: str, verbose: bool = True) -> pd.DataFrame:
    """
    Complete data processing pipeline for electricity consumption data.
    
    This function orchestrates the entire data processing workflow:
    1. Loads raw data from CSV
    2. Cleans and standardizes the data
    3. Validates the final dataset
    4. Returns the clean DataFrame ready for database storage
    
    Args:
        file_path (str): Path to the CSV file containing electricity data
        verbose (bool): Whether to print progress information
        
    Returns:
        pd.DataFrame: Clean, validated DataFrame ready for database operations
        
    Raises:
        Exception: If any step in the processing pipeline fails
    """
    try:
        if verbose:
            print("=== Powerlytics Data Processing Pipeline ===")
        
        # Step 1: Load raw data
        if verbose:
            print("Loading raw data...")
        df_raw = load_raw_data(file_path)
        
        # Step 2: Clean and standardize
        if verbose:
            print("Cleaning and standardizing data...")
        df_clean = clean_raw_data(df_raw)
        
        # Step 3: Validate the final dataset
        if verbose:
            print("Validating processed data...")
        validate_cleaned_data(df_clean)
        
        if verbose:
            print(f"✅ Successfully processed {len(df_clean)} records")
            print(f"Date range: {df_clean['Date'].min().date()} to {df_clean['Date'].max().date()}")
            print(f"Total consumption: {df_clean['KWH'].sum():.3f} kWh")
        
        return df_clean
        
    except Exception as e:
        if verbose:
            print(f"❌ Error in data processing pipeline: {str(e)}")
        raise


def get_data_summary(df: pd.DataFrame) -> dict:
    """
    Generate a summary of the electricity consumption data.
    
    Args:
        df (pd.DataFrame): Clean electricity data DataFrame
        
    Returns:
        dict: Summary statistics and metadata about the dataset
    """
    summary = {
        'total_records': len(df),
        'date_range': {
            'start': df['Date'].min(),
            'end': df['Date'].max()
        },
        'consumption_stats': {
            'total_kwh': round(df['KWH'].sum(), 3),
            'average_kwh': round(df['KWH'].mean(), 3),
            'max_kwh': round(df['KWH'].max(), 3),
            'min_kwh': round(df['KWH'].min(), 3)
        },
        'data_quality': {
            'missing_values': df.isnull().sum().sum(),
            'duplicate_rows': df.duplicated().sum()
        }
    }
    return summary