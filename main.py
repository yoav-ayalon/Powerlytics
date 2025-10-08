"""
Powerlytics - Main Application Entry Point

This script initializes the electricity consumption data processing system
and prepares the dataset for database operations and future analysis.
"""

from src.powerlytics import process_electricity_data, get_data_summary
import os


def initialize_powerlytics_database():
    """
    Initialize the Powerlytics system and prepare data for database storage.
    
    Returns:
        pd.DataFrame: Clean electricity consumption data ready for database operations
    """
    # Configuration
    DATA_FILE = "data/oct24-oct25.csv"
    
    # Verify data file exists
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Data file not found: {DATA_FILE}")
    
    # Process electricity consumption data
    df_electricity = process_electricity_data(DATA_FILE, verbose=False)
    return df_electricity


if __name__ == "__main__":
    try:
        # Initialize system and prepare database
        df_clean = initialize_powerlytics_database()
        
        # Future database integration point
        # TODO: Implement database storage (Phase 2)
        # save_to_database(df_clean)
        
        print("\n✅ Powerlytics system initialized successfully")
        print("💾 Data ready for database operations")

        
    except Exception as e:
        print(f"❌ System initialization failed: {e}")