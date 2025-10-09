"""
Powerlytics - Data Processing, Aggregation, and Visualization Module

Core functionality for processing electricity consumption data:
- Load and clean CSV data
- Generate multi-level aggregations with multi-year support
- Prepare data structure for analysis
- Dynamic secondary groupings with date range filtering
- Visualize consumption patterns and trends

Phase 2.2 Features:
- Multi-year dataset support with year-specific aggregations
- Dynamic on-demand secondary groupings (hour of day, day of week)
- Israeli coastal climate seasonal definitions
- Date range filtering for flexible analytical exploration
"""

import pandas as pd
from typing import Dict, Optional, Union, Tuple
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


def _aggregate_data(df: pd.DataFrame, level: str = "daily") -> pd.DataFrame:
    """
    Perform aggregation of the cleaned dataset for a specific time level.
    
    Multi-year support: All aggregations above daily level include Year column
    to maintain separation between different years.
    
    Args:
        df: Cleaned electricity consumption data
        level: Aggregation level - one of: hourly, daily, weekly, monthly, seasonal, yearly
    
    Returns:
        DataFrame with aggregated data including Year column for multi-year datasets
    """
    if level not in ["hourly", "daily", "weekly", "monthly", "seasonal", "yearly"]:
        raise ValueError(f"Unsupported aggregation level: {level}")
    
    df_work = df.copy()
    df_work['DateTime'] = pd.to_datetime(
        df_work['Date'].astype(str) + ' ' + df_work['Hour'].astype(str)
    )
    df_work['KWH'] = df_work['KWH'].round(3)
    df_work['Year'] = df_work['Date'].dt.year
    
    if level == "hourly":
        df_agg = df_work.groupby([
            df_work['Year'],
            df_work['Date'],
            df_work['DateTime'].dt.hour
        ])['KWH'].sum().reset_index()
        df_agg.columns = ['Year', 'Date', 'Hour', 'KWH']
        df_agg['KWH'] = df_agg['KWH'].round(3)
        
    elif level == "daily":
        df_agg = df_work.groupby([df_work['Year'], df_work['Date']])['KWH'].sum().reset_index()
        df_agg.columns = ['Year', 'Date', 'KWH']
        df_agg['KWH'] = df_agg['KWH'].round(3)
        
    elif level == "weekly":
        df_work['Week'] = df_work['Date'].dt.isocalendar().week
        df_agg = df_work.groupby(['Year', 'Week'])['KWH'].sum().reset_index()
        df_agg['KWH'] = df_agg['KWH'].round(3)
        
    elif level == "monthly":
        df_work['Month'] = df_work['Date'].dt.month
        df_agg = df_work.groupby(['Year', 'Month'])['KWH'].sum().reset_index()
        df_agg['KWH'] = df_agg['KWH'].round(3)
        
    elif level == "seasonal":
        def get_season_israeli(month):
            """Israeli coastal climate seasonal definitions."""
            if month in [12, 1, 2]:
                return 'Winter'
            elif month in [3, 4, 5]:
                return 'Spring'
            elif month in [6, 7, 8, 9]:
                return 'Summer'
            else:  # October, November
                return 'Autumn'
        
        df_work['Season'] = df_work['Date'].dt.month.apply(get_season_israeli)
        df_agg = df_work.groupby(['Year', 'Season'])['KWH'].sum().reset_index()
        df_agg['KWH'] = df_agg['KWH'].round(3)
        
    elif level == "yearly":
        df_agg = df_work.groupby('Year')['KWH'].sum().reset_index()
        df_agg['KWH'] = df_agg['KWH'].round(3)
    
    return df_agg


def _aggregate_all_levels(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Automatically compute all primary aggregation levels with multi-year support.
    
    Primary aggregations computed:
    - hourly: Hour-by-hour consumption (with Year and Date)
    - daily: Daily consumption totals (with Year)
    - weekly: Weekly consumption totals (with Year)
    - monthly: Monthly consumption totals (with Year)
    - seasonal: Seasonal consumption totals (with Year, Israeli climate)
    - yearly: Annual consumption totals
    
    Returns:
        Dict containing all primary aggregation DataFrames.
        Secondary groupings are generated separately using dynamic functions.
    """
    primary_aggregation_levels = [
        "hourly", "daily", "weekly", "monthly", "seasonal", "yearly"
    ]
    
    aggregations = {}
    
    try:
        for level in primary_aggregation_levels:
            aggregations[level] = _aggregate_data(df, level)
        
        _validate_aggregations(df, aggregations)
        return aggregations
        
    except Exception as e:
        raise Exception(f"Error in aggregate_all_levels: {str(e)}")


def _validate_aggregations(df_original: pd.DataFrame, aggregations: Dict[str, pd.DataFrame]) -> bool:
    """
    Internal validation function to ensure aggregation consistency with multi-year support.
    
    Validates:
    - Total consumption consistency across aggregation levels
    - Year column presence in multi-year aggregations
    - Data integrity and no cross-year merging
    """
    original_total = df_original['KWH'].sum()
    tolerance = 0.001
    
    daily_total = aggregations['daily']['KWH'].sum()
    yearly_total = aggregations['yearly']['KWH'].sum()
    
    if abs(original_total - daily_total) > tolerance:
        raise ValueError(f"Daily aggregation total ({daily_total:.3f}) doesn't match original ({original_total:.3f})")
    
    if abs(original_total - yearly_total) > tolerance:
        raise ValueError(f"Yearly aggregation total ({yearly_total:.3f}) doesn't match original ({original_total:.3f})")
    
    # Validate multi-year structure
    for level in ['hourly', 'daily', 'weekly', 'monthly', 'seasonal']:
        if 'Year' not in aggregations[level].columns:
            raise ValueError(f"{level} aggregation missing required Year column for multi-year support")
    
    # Validate year separation - ensure no data mixing across years
    original_years = set(df_original['Date'].dt.year)
    for level in ['daily', 'weekly', 'monthly', 'seasonal', 'yearly']:
        agg_years = set(aggregations[level]['Year'])
        if not agg_years.issubset(original_years):
            raise ValueError(f"{level} aggregation contains years not present in original data")
    
    days_in_data = (df_original['Date'].max() - df_original['Date'].min()).days + 1
    daily_records = len(aggregations['daily'])
    
    if daily_records > days_in_data:
        raise ValueError(f"Too many daily records: {daily_records} > {days_in_data}")
    
    return True


def process_electricity_data(file_path: str) -> tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """
    Complete data processing pipeline with enhanced multi-year aggregation support.
    
    Returns:
        tuple: (cleaned_data, primary_aggregations)
            - cleaned_data: Original cleaned dataset
            - primary_aggregations: Dictionary containing six primary aggregation levels
                                  (hourly, daily, weekly, monthly, seasonal, yearly)
    
    Note: Secondary groupings (hour_of_day, day_of_week) are generated dynamically
          using separate functions with date range filtering capabilities.
    """
    df_raw = _load_raw_data(file_path)
    df_clean = _clean_raw_data(df_raw)
    _validate_cleaned_data(df_clean)
    aggregations = _aggregate_all_levels(df_clean)
    return df_clean, aggregations


def get_aggregation_summary(aggregations: Dict[str, pd.DataFrame]) -> Dict[str, Dict]:
    """
    Generate summary information for all primary aggregation levels.
    
    Note: This function only processes primary aggregations. Secondary groupings
          are generated dynamically and should be summarized separately if needed.
    """
    summary = {}
    
    for level, df_agg in aggregations.items():
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


def group_by_hour(
    aggregations: Dict[str, pd.DataFrame], 
    start_date: Optional[Union[str, datetime, date]] = None,
    end_date: Optional[Union[str, datetime, date]] = None,
    years: Optional[list] = None
) -> pd.DataFrame:
    """
    Generate dynamic grouping of average consumption by hour of day.
    
    Args:
        aggregations: Dictionary containing primary aggregation DataFrames
        start_date: Optional start date for filtering (inclusive)
        end_date: Optional end date for filtering (inclusive)
        years: Optional list of years to include (e.g., [2024, 2025])
    
    Returns:
        DataFrame with columns: ['Hour', 'Avg_KWH']
        - Hour: 0-23 representing hour of day
        - Avg_KWH: Average consumption for that hour across the date range
    """
    if 'hourly' not in aggregations:
        raise ValueError("Hourly aggregation required for hour-of-day analysis")
    
    df_hourly = aggregations['hourly'].copy()
    
    # Apply date range filtering
    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        df_hourly = df_hourly[df_hourly['Date'] >= start_date]
    
    if end_date is not None:
        end_date = pd.to_datetime(end_date)
        df_hourly = df_hourly[df_hourly['Date'] <= end_date]
    
    # Apply year filtering
    if years is not None:
        df_hourly = df_hourly[df_hourly['Year'].isin(years)]
    
    if len(df_hourly) == 0:
        raise ValueError("No data available for the specified date range and years")
    
    # Group by hour of day and calculate average
    df_result = df_hourly.groupby('Hour')['KWH'].mean().reset_index()
    df_result.columns = ['Hour', 'Avg_KWH']
    df_result['Avg_KWH'] = df_result['Avg_KWH'].round(3)
    
    return df_result


def group_by_day_of_week(
    aggregations: Dict[str, pd.DataFrame],
    start_date: Optional[Union[str, datetime, date]] = None,
    end_date: Optional[Union[str, datetime, date]] = None,
    years: Optional[list] = None
) -> pd.DataFrame:
    """
    Generate dynamic grouping of average consumption by day of week.
    
    Args:
        aggregations: Dictionary containing primary aggregation DataFrames
        start_date: Optional start date for filtering (inclusive)
        end_date: Optional end date for filtering (inclusive)
        years: Optional list of years to include (e.g., [2024, 2025])
    
    Returns:
        DataFrame with columns: ['DayOfWeek', 'Avg_KWH']
        - DayOfWeek: Monday through Sunday
        - Avg_KWH: Average consumption for that day across the date range
    """
    if 'daily' not in aggregations:
        raise ValueError("Daily aggregation required for day-of-week analysis")
    
    df_daily = aggregations['daily'].copy()
    
    # Apply date range filtering
    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        df_daily = df_daily[df_daily['Date'] >= start_date]
    
    if end_date is not None:
        end_date = pd.to_datetime(end_date)
        df_daily = df_daily[df_daily['Date'] <= end_date]
    
    # Apply year filtering
    if years is not None:
        df_daily = df_daily[df_daily['Year'].isin(years)]
    
    if len(df_daily) == 0:
        raise ValueError("No data available for the specified date range and years")
    
    # Add day of week and group
    df_daily['DayOfWeekNum'] = df_daily['Date'].dt.dayofweek
    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    df_result = df_daily.groupby('DayOfWeekNum')['KWH'].mean().reset_index()
    df_result['DayOfWeek'] = df_result['DayOfWeekNum'].apply(lambda x: day_names[x])
    df_result = df_result[['DayOfWeek', 'KWH']]
    df_result.columns = ['DayOfWeek', 'Avg_KWH']
    df_result['Avg_KWH'] = df_result['Avg_KWH'].round(3)
    
    return df_result


