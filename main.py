"""
Powerlytics - Main Application Entry Point
This script initializes the electricity consumption data processing system
and prepares the dataset with full aggregation for analysis.
"""

import src.preprocess as preprocess
import src.visualization as visualization

import os


def initialize_powerlytics_database():
    """Initialize the Powerlytics system and prepare data with enhanced multi-year aggregation."""
    DATA_FILE = "data/electric_data.csv"
    
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Data file not found: {DATA_FILE}")
    
    df_electricity, aggregations = preprocess.process_electricity_data(DATA_FILE)
    return df_electricity, aggregations

def print_dfs(df_clean, agg):
    """Print sample data and aggregation structure for verification."""
    print("\n📋 Data Structure Overview:")
    print(f"   Raw data: {len(df_clean):,} records")
    print(f"   Date range: {df_clean['Date'].min().date()} to {df_clean['Date'].max().date()}")
    print(f"   Available aggregations: {list(agg.keys())}")
    
    print("\n📊 Aggregation Summary:")
    for level, df_agg in agg.items():
        print(f"   {level:10}: {len(df_agg):4} records, columns: {list(df_agg.columns)}")
        
    print("\n📈 Sample Hourly Data:")
    print(agg['hourly'].head())


def visualize(aggregations):

    # (1) Daily consumption – full dataset
    visualization.visualize(
        aggregations['daily'],
        level='daily',
        kind='line',
        title='Daily Electricity Consumption - Full Dataset')

    # (2) Daily consumption – specific date range
    start_date = '2025-06-01'
    end_date = '2025-09-30'
    daily_filtered = aggregations['daily'][
        (aggregations['daily']['Date'] >= start_date) &
        (aggregations['daily']['Date'] <= end_date)]
    
    visualization.visualize(
        daily_filtered,
        level='daily',
        kind='line',
        title=f'Daily Electricity Consumption ({start_date} to {end_date})')
    
    # (3) Hourly consumption – full dataset
    visualization.visualize(
        aggregations['hourly'],
        level='hourly',
        kind='line',
        title='Hourly Electricity Consumption - Full Dataset')

    # (4) Hourly consumption – specific date range
    start_date = '2025-06-01'
    end_date = '2025-09-30'
    hourly_filtered = aggregations['hourly'][
        (aggregations['hourly']['Date'] >= start_date) &
        (aggregations['hourly']['Date'] <= end_date)]
    
    visualization.visualize(
        hourly_filtered,
        level='hourly',
        kind='line',
        title=f'Hourly Electricity Consumption ({start_date} to {end_date})')

    # (5) Average hourly consumption – all data
    hourly_pattern_all = preprocess.group_by_hour(aggregations)
    visualization.visualize(
        hourly_pattern_all,
        level='hour_of_day',
        kind='line',
        title='Average Hourly Electricity Consumption (All Data)')

    # (6) Average hourly consumption – specific date range
    start_date = '2025-06-01'
    end_date = '2025-09-30'
    hourly_pattern_filtered = preprocess.group_by_hour(
        aggregations,
        start_date=start_date,
        end_date=end_date)
    
    visualization.visualize(
        hourly_pattern_filtered,
        level='hour_of_day',
        kind='line',
        title=f'Average Hourly Electricity Consumption ({start_date} to {end_date})')
    
    # # (5) Monthly consumption – bar chart by month and year
    # visualization.visualize(
    #     aggregations['monthly'],
    #     level='monthly',
    #     kind='bar',
    #     title='Monthly Electricity Consumption by Month and Year')

    # # (6) Seasonal comparison – bar chart (Winter, Spring, Summer, Autumn)
    # visualization.visualize(
    #     aggregations['seasonal'],
    #     level='seasonal',
    #     kind='bar',
    #     title='Seasonal Electricity Consumption Comparison (All Years)')

    # # (7) Daily consumption distribution – boxplot
    # visualization.visualize(
    #     aggregations['daily'],
    #     level='daily',
    #     kind='box',
    #     title='Daily Electricity Consumption Distribution (All Data)')


if __name__ == "__main__":
    try:
        df_clean, agg = initialize_powerlytics_database()
        print("✅ Data loaded and aggregated successfully")
        
        # print_dfs(df_clean, agg)
        visualize(agg)


    except Exception as e:
        print(f"❌ System initialization failed: {e}")