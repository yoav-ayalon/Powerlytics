"""
Powerlytics - Data Processing, Aggregation, and Visualization Module

Core functionality for processing electricity consumption data:
- Load and clean CSV data
- Generate multi-level aggregations  
- Prepare data structure for analysis
- Visualize consumption patterns and trends
"""

import pandas as pd
from typing import Dict, Optional, Union
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, date
import seaborn as sns


def _load_raw_data(file_path: str) -> pd.DataFrame:
    """Load raw CSV data while skipping metadata rows."""
    try:
        df_raw = pd.read_csv(file_path, skiprows=12, encoding='utf-8')
        df_raw = df_raw.dropna(how='all')
        return df_raw
    except FileNotFoundError:
        raise FileNotFoundError(f"Could not find the file: {file_path}")
    except Exception as e:
        raise Exception(f"Error loading data from {file_path}: {str(e)}")


def _clean_raw_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize the raw dataset."""
    try:
        df_clean = df.copy()
        original_columns = df_clean.columns.tolist()
        
        if len(original_columns) < 3:
            raise ValueError(f"Expected at least 3 columns, but found {len(original_columns)}")
        
        # Rename columns to standard English names
        column_mapping = {
            original_columns[0]: 'Date',
            original_columns[1]: 'Hour', 
            original_columns[2]: 'KWH'
        }
        df_clean = df_clean.rename(columns=column_mapping)
        df_clean = df_clean[['Date', 'Hour', 'KWH']]
        df_clean = df_clean.dropna(subset=['Date', 'Hour', 'KWH'])
        
        # Convert data types
        df_clean['Date'] = pd.to_datetime(df_clean['Date'], format='%d/%m/%Y', errors='coerce')
        df_clean['Hour'] = pd.to_datetime(df_clean['Hour'], format='%H:%M', errors='coerce').dt.time
        df_clean['KWH'] = pd.to_numeric(df_clean['KWH'], errors='coerce')
        
        df_clean = df_clean.dropna()
        
        if len(df_clean) == 0:
            raise ValueError("No valid data remaining after cleaning process")
        
        return df_clean
        
    except Exception as e:
        raise Exception(f"Error cleaning data: {str(e)}")


def _validate_cleaned_data(df: pd.DataFrame) -> bool:
    """Validate that the cleaned DataFrame has the expected structure."""
    expected_columns = ['Date', 'Hour', 'KWH']
    
    if list(df.columns) != expected_columns:
        raise ValueError(f"Expected columns {expected_columns}, but found {list(df.columns)}")
    
    if not pd.api.types.is_datetime64_any_dtype(df['Date']):
        raise ValueError("Date column should be datetime type")
    
    if not pd.api.types.is_numeric_dtype(df['KWH']):
        raise ValueError("KWH column should be numeric type")
    
    if len(df) == 0:
        raise ValueError("DataFrame is empty")
    
    return True


def process_electricity_data(file_path: str) -> tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """Complete data processing pipeline with aggregation."""
    df_raw = _load_raw_data(file_path)
    df_clean = _clean_raw_data(df_raw)
    _validate_cleaned_data(df_clean)
    aggregations = _aggregate_all_levels(df_clean)
    return df_clean, aggregations


def _aggregate_data(df: pd.DataFrame, level: str = "daily") -> pd.DataFrame:
    """Perform aggregation of the cleaned dataset for a specific time level."""
    if level not in ["hourly", "daily", "weekly", "monthly", "seasonal", "yearly", 
                     "hour_of_day", "day_of_week"]:
        raise ValueError(f"Unsupported aggregation level: {level}")
    
    df_work = df.copy()
    df_work['DateTime'] = pd.to_datetime(
        df_work['Date'].astype(str) + ' ' + df_work['Hour'].astype(str)
    )
    df_work['KWH'] = df_work['KWH'].round(3)
    
    if level == "hourly":
        df_agg = df_work.groupby([
            df_work['Date'],
            df_work['DateTime'].dt.hour
        ])['KWH'].sum().reset_index()
        df_agg.columns = ['Date', 'Hour', 'KWH']
        df_agg['KWH'] = df_agg['KWH'].round(3)
        
    elif level == "daily":
        df_agg = df_work.groupby('Date')['KWH'].sum().reset_index()
        df_agg['KWH'] = df_agg['KWH'].round(3)
        
    elif level == "weekly":
        df_work['Week'] = df_work['Date'].dt.isocalendar().week
        df_work['Year'] = df_work['Date'].dt.year
        df_agg = df_work.groupby(['Year', 'Week'])['KWH'].sum().reset_index()
        df_agg['KWH'] = df_agg['KWH'].round(3)
        
    elif level == "monthly":
        df_work['Month'] = df_work['Date'].dt.month
        df_work['Year'] = df_work['Date'].dt.year
        df_agg = df_work.groupby(['Year', 'Month'])['KWH'].sum().reset_index()
        df_agg['KWH'] = df_agg['KWH'].round(3)
        
    elif level == "seasonal":
        def get_season(month):
            if month in [12, 1, 2]:
                return 'Winter'
            elif month in [3, 4, 5]:
                return 'Spring'
            elif month in [6, 7, 8, 9]:
                return 'Summer'
            else:
                return 'Autumn'
        
        df_work['Season'] = df_work['Date'].dt.month.apply(get_season)
        df_work['Year'] = df_work['Date'].dt.year
        df_agg = df_work.groupby(['Year', 'Season'])['KWH'].sum().reset_index()
        df_agg['KWH'] = df_agg['KWH'].round(3)
        
    elif level == "yearly":
        df_work['Year'] = df_work['Date'].dt.year
        df_agg = df_work.groupby('Year')['KWH'].sum().reset_index()
        df_agg['KWH'] = df_agg['KWH'].round(3)
        
    elif level == "hour_of_day":
        df_work['HourOfDay'] = df_work['DateTime'].dt.hour
        df_agg = df_work.groupby('HourOfDay')['KWH'].mean().reset_index()
        df_agg.columns = ['Hour', 'Avg_KWH']
        df_agg['Avg_KWH'] = df_agg['Avg_KWH'].round(3)
        
    elif level == "day_of_week":
        df_work['DayOfWeek'] = df_work['Date'].dt.dayofweek
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        df_agg = df_work.groupby('DayOfWeek')['KWH'].mean().reset_index()
        df_agg['DayOfWeek'] = df_agg['DayOfWeek'].apply(lambda x: day_names[x])
        df_agg.columns = ['DayOfWeek', 'Avg_KWH']
        df_agg['Avg_KWH'] = df_agg['Avg_KWH'].round(3)
    
    return df_agg


def _aggregate_all_levels(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Automatically compute all aggregation levels."""
    aggregation_levels = [
        "hourly", "daily", "weekly", "monthly", 
        "seasonal", "yearly", "hour_of_day", "day_of_week"
    ]
    
    aggregations = {}
    
    try:
        for level in aggregation_levels:
            aggregations[level] = _aggregate_data(df, level)
        
        _validate_aggregations(df, aggregations)
        return aggregations
        
    except Exception as e:
        raise Exception(f"Error in aggregate_all_levels: {str(e)}")


def _validate_aggregations(df_original: pd.DataFrame, aggregations: Dict[str, pd.DataFrame]) -> bool:
    """Internal validation function to ensure aggregation consistency."""
    original_total = df_original['KWH'].sum()
    tolerance = 0.001
    
    daily_total = aggregations['daily']['KWH'].sum()
    yearly_total = aggregations['yearly']['KWH'].sum()
    
    if abs(original_total - daily_total) > tolerance:
        raise ValueError(f"Daily aggregation total ({daily_total:.3f}) doesn't match original ({original_total:.3f})")
    
    if abs(original_total - yearly_total) > tolerance:
        raise ValueError(f"Yearly aggregation total ({yearly_total:.3f}) doesn't match original ({original_total:.3f})")
    
    days_in_data = (df_original['Date'].max() - df_original['Date'].min()).days + 1
    daily_records = len(aggregations['daily'])
    
    if daily_records > days_in_data:
        raise ValueError(f"Too many daily records: {daily_records} > {days_in_data}")
    
    return True


def get_aggregation_summary(aggregations: Dict[str, pd.DataFrame]) -> Dict[str, Dict]:
    """Generate summary information for all aggregation levels."""
    summary = {}
    
    for level, df_agg in aggregations.items():
        if level in ['hour_of_day', 'day_of_week']:
            kwh_col = 'Avg_KWH'
            total_key = 'average_consumption'
        else:
            kwh_col = 'KWH'
            total_key = 'total_consumption'
        
        summary[level] = {
            'record_count': len(df_agg),
            total_key: round(df_agg[kwh_col].sum(), 3),
            'max_consumption': round(df_agg[kwh_col].max(), 3),
            'min_consumption': round(df_agg[kwh_col].min(), 3),
            'average_consumption': round(df_agg[kwh_col].mean(), 3)
        }
    
    return summary
