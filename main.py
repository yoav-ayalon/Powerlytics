"""
Powerlytics - Main Application Entry Point

This script initializes the electricity consumption data processing system
and prepares the dataset with full aggregation for analysis.
"""

from src.powerlytics import process_electricity_data
import os


def initialize_powerlytics_database():
    """Initialize the Powerlytics system and prepare data with full aggregation."""
    DATA_FILE = "data/oct24-oct25.csv"
    
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Data file not found: {DATA_FILE}")
    
    df_electricity, aggregations = process_electricity_data(DATA_FILE)
    return df_electricity, aggregations


if __name__ == "__main__":
    try:
        df_clean, aggregations = initialize_powerlytics_database()
        print("✅ Data loaded and aggregated successfully")
        
    except Exception as e:
        print(f"❌ System initialization failed: {e}")