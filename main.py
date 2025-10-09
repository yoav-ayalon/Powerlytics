"""
Powerlytics - Main Application Entry Point
This script initializes the electricity consumption data processing system
and prepares the dataset with full aggregation for analysis.
"""

import src.preprocess as preprocess
import src.visualization as visualization

import os


def initialize_powerlytics_database():
    """Initialize the Powerlytics system and prepare data with full aggregation."""
    DATA_FILE = "data/oct24-oct25.csv"
    
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Data file not found: {DATA_FILE}")
    
    df_electricity, aggregations = preprocess.process_electricity_data(DATA_FILE)
    return df_electricity, aggregations

def visualize(df_plot):
    print("📊 Generating diverse electricity consumption visualizations...")
    
    # 1. Hour of Day Analysis - Line charts with and without filtering
    print("\n1. Hour of Day Patterns:")
    visualization.visualize(df_plot['hour_of_day'], level='hour_of_day', kind='line', 
                          title='Daily Consumption Pattern - All Hours')
    visualization.visualize(df_plot['hour_of_day'], level='hour_of_day', kind='line', 
                          start='2025-06-01', end='2025-09-30', 
                          title='Daily Consumption Pattern - Peak Hours (6AM-9:30PM)')
    
    # 2. Day of Week Analysis - Bar chart
    print("\n2. Weekly Consumption Patterns:")
    visualization.visualize(df_plot['day_of_week'], level='day_of_week', kind='bar',
                          title='Average Consumption by Day of Week')
    
    # 3. Daily Trends - Line chart with time filtering
    print("\n3. Daily Consumption Trends:")
    visualization.visualize(df_plot['daily'], level='daily', kind='line',
                          title='Daily Consumption Over Time')
    
    # 4. Monthly Analysis - Bar chart
    print("\n4. Monthly Consumption Comparison:")
    visualization.visualize(df_plot['monthly'], level='monthly', kind='bar',
                          title='Monthly Consumption Totals')
    
    # 5. Seasonal Patterns - Bar chart
    print("\n5. Seasonal Consumption Analysis:")
    visualization.visualize(df_plot['seasonal'], level='seasonal', kind='bar',
                          title='Consumption by Season')
    
    # 6. Hour of Day - Bar chart (different visualization of same data)
    print("\n6. Hourly Consumption Distribution:")
    visualization.visualize(df_plot['hour_of_day'], level='hour_of_day', kind='bar',
                          title='Average Consumption Distribution by Hour')
    
    # 7. Daily Data - Scatter plot
    print("\n7. Daily Consumption Scatter Analysis:")
    visualization.visualize(df_plot['daily'], level='daily', kind='scatter',
                          title='Daily Consumption Scatter Plot')
    
    # 8. Box plot analysis
    print("\n8. Consumption Distribution Analysis:")
    visualization.visualize(df_plot['daily'], level='daily', kind='box',
                          title='Daily Consumption Distribution (Box Plot)')
    
    print("\n✅ All visualizations generated successfully!")

    




if __name__ == "__main__":
    try:
        df_clean, agg = initialize_powerlytics_database()
        print("✅ Data loaded and aggregated successfully")
        visualize(agg)


    except Exception as e:
        print(f"❌ System initialization failed: {e}")