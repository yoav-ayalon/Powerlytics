
import pandas as pd
from typing import Dict, Optional, Union
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, date
import seaborn as sns



def _auto_detect_level(df: pd.DataFrame, level: str) -> str:
    """Auto-detect or validate the aggregation level based on DataFrame columns."""
    columns = df.columns.tolist()
    
    # Check if the provided level matches the DataFrame structure
    if level == "hour_of_day" and "Hour" in columns and "Avg_KWH" in columns:
        return level
    elif level == "day_of_week" and "DayOfWeek" in columns and "Avg_KWH" in columns:
        return level
    elif level in ["hourly", "daily"] and "Date" in columns and "KWH" in columns:
        return level
    elif level == "weekly" and "Week" in columns and "KWH" in columns:
        return level
    elif level == "monthly" and "Month" in columns and "KWH" in columns:
        return level
    elif level == "seasonal" and "Season" in columns and "KWH" in columns:
        return level
    elif level == "yearly" and "Year" in columns and "KWH" in columns:
        return level
    
    # Auto-detect based on columns if the provided level doesn't match
    if "Hour" in columns and "Avg_KWH" in columns:
        return "hour_of_day"
    elif "DayOfWeek" in columns and "Avg_KWH" in columns:
        return "day_of_week"
    elif "Date" in columns and "KWH" in columns and "Year" in columns:
        # Distinguish between hourly and daily based on presence of Hour column
        if "Hour" in columns:
            return "hourly"
        else:
            return "daily"
    elif "Week" in columns and "KWH" in columns:
        return "weekly"
    elif "Month" in columns and "KWH" in columns:
        return "monthly"
    elif "Season" in columns and "KWH" in columns:
        return "seasonal"
    elif "Year" in columns and "KWH" in columns and len(columns) == 2:
        return "yearly"
    
    # If no match found, return the original level (let other functions handle the error)
    return level


def visualize(
    df: pd.DataFrame,
    level: str = "daily",
    kind: str = "line",
    start: Optional[Union[str, date, datetime]] = None,
    end: Optional[Union[str, date, datetime]] = None,
    group_by: Optional[str] = None,
    title: Optional[str] = None,
    show: bool = True
) -> Optional[plt.Figure]:
    """
    Display electricity consumption data for a given aggregation level and time range.

    Parameters:
        df (DataFrame): Data to visualize (e.g., aggregations["daily"]).
        level (str): Aggregation level ("hourly", "daily", "weekly", "monthly", "seasonal", "yearly").
        kind (str): Chart type ("line", "bar", "scatter", "box").
        start (str/date): Start date or period filter (optional).
        end (str/date): End date or period filter (optional).
        group_by (str): For grouped visualizations ("month", "season", "day_of_week", etc.).
        title (str): Custom plot title (optional).
        show (bool): Whether to display the plot immediately or just return the figure object.
    
    Returns:
        matplotlib.figure.Figure: Figure object if show=False, otherwise None.
    """
    
    # Set up matplotlib style for consistent plots
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Make a copy of the dataframe to avoid modifying original
    df_plot = df.copy()
    
    # Auto-detect level if not specified correctly or if columns don't match
    level = _auto_detect_level(df_plot, level)
    
    # Filter data by date range if provided
    df_plot = _filter_by_date_range(df_plot, level, start, end)
    
    # Determine x and y columns based on aggregation level
    x_col, y_col, x_label, y_label = _get_plot_columns(df_plot, level)
    
    # Generate default title if not provided
    if title is None:
        title = f"Electricity Consumption - {level.replace('_', ' ').title()}"
    
    # Handle different chart types
    if kind == "line":
        _plot_line_chart(ax, df_plot, x_col, y_col, level)
    elif kind == "bar":
        _plot_bar_chart(ax, df_plot, x_col, y_col, level)
    elif kind == "scatter":
        _plot_scatter_chart(ax, df_plot, x_col, y_col)
    elif kind == "box":
        _plot_box_chart(ax, df_plot, level, group_by)
    else:
        raise ValueError(f"Unsupported chart type: {kind}")
    
    # Set labels and title
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_title(title, fontsize=14, fontweight='bold')
    
    # Format the plot
    _format_plot(ax, level, kind)
    
    # Adjust layout
    plt.tight_layout()
    
    # Show or return figure
    if show:
        plt.show()
        return None
    else:
        return fig


def _filter_by_date_range(
    df: pd.DataFrame, 
    level: str, 
    start: Optional[Union[str, date, datetime]], 
    end: Optional[Union[str, date, datetime]]
) -> pd.DataFrame:
    """Filter dataframe by date range with multi-year support."""
    if start is None and end is None:
        return df
    
    df_filtered = df.copy()
    
    # Convert string dates to datetime
    if start is not None and isinstance(start, str):
        start = pd.to_datetime(start)
    if end is not None and isinstance(end, str):
        end = pd.to_datetime(end)
    
    # For aggregated data like hour_of_day and day_of_week, 
    # date filtering doesn't make sense since they don't contain date information
    # Return the original dataframe for these cases
    if level in ["hour_of_day", "day_of_week"]:
        # These aggregations don't have date columns, so return original data
        # but we can still apply hour filtering for hour_of_day if needed
        if level == "hour_of_day" and (start is not None or end is not None):
            # Extract hour from start/end times for filtering
            if start is not None:
                start_hour = start.hour
                df_filtered = df_filtered[df_filtered["Hour"] >= start_hour]
            if end is not None:
                end_hour = end.hour
                df_filtered = df_filtered[df_filtered["Hour"] <= end_hour]
        return df_filtered
    
    # Apply filtering based on aggregation level - now with Year column support
    if level in ["hourly", "daily"] and "Date" in df.columns:
        if start is not None:
            df_filtered = df_filtered[df_filtered["Date"] >= start]
        if end is not None:
            df_filtered = df_filtered[df_filtered["Date"] <= end]
    elif level in ["monthly", "yearly", "weekly", "seasonal"] and "Year" in df.columns:
        if start is not None:
            df_filtered = df_filtered[df_filtered["Year"] >= start.year]
        if end is not None:
            df_filtered = df_filtered[df_filtered["Year"] <= end.year]
        # Additional month filtering for monthly data
        if level == "monthly" and "Month" in df.columns:
            if start is not None:
                df_filtered = df_filtered[
                    (df_filtered["Year"] > start.year) | 
                    ((df_filtered["Year"] == start.year) & (df_filtered["Month"] >= start.month))
                ]
            if end is not None:
                df_filtered = df_filtered[
                    (df_filtered["Year"] < end.year) | 
                    ((df_filtered["Year"] == end.year) & (df_filtered["Month"] <= end.month))
                ]
    
    return df_filtered


def _get_plot_columns(df: pd.DataFrame, level: str) -> tuple:
    """Determine x and y columns and labels based on aggregation level."""
    
    if level in ["hourly", "daily"]:
        x_col = "Date"
        y_col = "KWH"
        x_label = "Date"
        y_label = "Consumption (kWh)"
    elif level == "weekly":
        x_col = "Week"
        y_col = "KWH"
        x_label = "Week"
        y_label = "Consumption (kWh)"
    elif level == "monthly":
        x_col = "Month"
        y_col = "KWH"
        x_label = "Month"
        y_label = "Consumption (kWh)"
    elif level == "seasonal":
        x_col = "Season"
        y_col = "KWH"
        x_label = "Season"
        y_label = "Consumption (kWh)"
    elif level == "yearly":
        x_col = "Year"
        y_col = "KWH"
        x_label = "Year"
        y_label = "Consumption (kWh)"
    elif level == "hour_of_day":
        x_col = "Hour"
        y_col = "Avg_KWH"
        x_label = "Hour of Day"
        y_label = "Average Consumption (kWh)"
    elif level == "day_of_week":
        x_col = "DayOfWeek"
        y_col = "Avg_KWH"
        x_label = "Day of Week"
        y_label = "Average Consumption (kWh)"
    else:
        # Default fallback
        x_col = df.columns[0]
        y_col = df.columns[-1]
        x_label = x_col
        y_label = y_col
    
    return x_col, y_col, x_label, y_label


def _plot_line_chart(ax: plt.Axes, df: pd.DataFrame, x_col: str, y_col: str, level: str):
    """Create a line chart."""
    if level in ["hourly", "daily"] and x_col == "Date":
        # Time series plot with proper date handling
        ax.plot(df[x_col], df[y_col], linewidth=1.5, marker='o', markersize=3)
    else:
        # Regular line plot
        ax.plot(df[x_col], df[y_col], linewidth=2, marker='o', markersize=4)


def _plot_bar_chart(ax: plt.Axes, df: pd.DataFrame, x_col: str, y_col: str, level: str):
    """Create a bar chart."""
    if level == "seasonal":
        # Custom order for seasons - handle potential duplicates
        season_order = ['Winter', 'Spring', 'Summer', 'Autumn']
        # Create a categorical type to maintain order without reindexing
        df_plot = df.copy()
        df_plot[x_col] = pd.Categorical(df_plot[x_col], categories=season_order, ordered=True)
        df_plot = df_plot.sort_values(x_col)
        ax.bar(df_plot[x_col], df_plot[y_col], alpha=0.8)
    elif level == "day_of_week":
        # Custom order for days of week
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        df_plot = df.copy()
        df_plot[x_col] = pd.Categorical(df_plot[x_col], categories=day_order, ordered=True)
        df_plot = df_plot.sort_values(x_col)
        ax.bar(df_plot[x_col], df_plot[y_col], alpha=0.8)
        # Rotate x-axis labels for better readability
        plt.xticks(rotation=45)
    else:
        ax.bar(df[x_col], df[y_col], alpha=0.8)


def _plot_scatter_chart(ax: plt.Axes, df: pd.DataFrame, x_col: str, y_col: str):
    """Create a scatter plot."""
    ax.scatter(df[x_col], df[y_col], alpha=0.6, s=50)


def _plot_box_chart(ax: plt.Axes, df: pd.DataFrame, level: str, group_by: Optional[str]):
    """Create a box plot."""
    if group_by is None:
        # Simple box plot of the consumption values
        if level in ['hour_of_day', 'day_of_week']:
            ax.boxplot(df['Avg_KWH'], labels=['Consumption'])
        else:
            ax.boxplot(df['KWH'], labels=['Consumption'])
    else:
        # Grouped box plot (this would require additional data processing)
        # For now, create a simple box plot
        if level in ['hour_of_day', 'day_of_week']:
            ax.boxplot(df['Avg_KWH'], labels=[f'Consumption by {group_by}'])
        else:
            ax.boxplot(df['KWH'], labels=[f'Consumption by {group_by}'])


def _format_plot(ax: plt.Axes, level: str, kind: str):
    """Apply consistent formatting to the plot."""
    # Add grid for better readability
    ax.grid(True, alpha=0.3)
    
    # Format x-axis for date-based plots
    if level in ["hourly", "daily"]:
        # Format dates on x-axis
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        plt.xticks(rotation=45)
    
    # Format y-axis
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.2f}'))
    
    # Set consistent colors and styling
    if kind == "line":
        ax.lines[0].set_color('#2E86AB')
    elif kind == "bar":
        for patch in ax.patches:
            patch.set_facecolor('#F24236')
            patch.set_alpha(0.8)
    elif kind == "scatter":
        ax.collections[0].set_facecolor('#A23B72')
    
    # Remove top and right spines for cleaner look
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)