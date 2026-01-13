#!/usr/bin/env python3
"""
Convert Nechako watershed CSV data to JSON format for the visualization tool
"""

import pandas as pd
import json
import os
from datetime import datetime
import glob

def load_stations():
    """Load existing stations.json file"""
    stations_file = os.path.join(os.path.dirname(__file__), "public", "data", "stations.json")
    
    with open(stations_file, 'r') as f:
        stations = json.load(f)
    
    return stations

def process_csv_file(csv_file, station_code):
    """Process a single CSV file and convert to daily averages"""
    print(f"Processing {csv_file}")
    
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Ensure wtmp is numeric, coercing errors to NaN
    df['wtmp'] = pd.to_numeric(df['wtmp'], errors='coerce')
    
    # Extract date
    df['date'] = df['timestamp'].dt.date
    
    # Group by date and calculate daily statistics
    daily_stats = df.groupby('date').agg({
        'wtmp': ['mean', 'min', 'max', 'count']
    }).round(2)
    
    # Handle NaN values by replacing them with None later
    
    # Flatten column names
    daily_stats.columns = ['temp_c', 'min_temp_c', 'max_temp_c', 'n_values']
    
    # Reset index to make date a column
    daily_stats = daily_stats.reset_index()
    
    # Convert date to string
    daily_stats['date'] = daily_stats['date'].astype(str)
    
    # Load air temperature data if available
    airtemp_file = os.path.join(os.path.dirname(__file__), "daymet_data", f"{station_code}_airtemp.json")
    if os.path.exists(airtemp_file):
        with open(airtemp_file, 'r') as f:
            airtemp_data = json.load(f)
        # Add air temperature to daily stats
        daily_stats['airtemp_c'] = daily_stats['date'].apply(lambda d: airtemp_data.get(d, None))
    else:
        daily_stats['airtemp_c'] = None
    
    # Convert to dict and handle NaN values
    records = daily_stats.to_dict('records')
    
    # Replace NaN values with None for valid JSON
    import math
    for record in records:
        for key, value in record.items():
            if isinstance(value, float) and math.isnan(value):
                record[key] = None
    
    return records

def update_stations_data(stations, data_dir):
    """Update existing stations with new CSV data"""
    # Look for CSV files in the correct directory
    csv_dir = os.path.join(os.path.dirname(__file__), "data", "01_Data")
    
    # Create public CSV directory if it doesn't exist
    public_csv_dir = os.path.join(data_dir, "csv")
    os.makedirs(public_csv_dir, exist_ok=True)
    
    # Find all CSV files
    csv_files = glob.glob(f"{csv_dir}/*.csv")
    print(f"Found {len(csv_files)} CSV files to process")
    
    updated_stations = []
    
    for station in stations:
        station_code = station['station_code']
        
        # Find corresponding CSV file
        matching_csv = None
        for csv_file in csv_files:
            if station_code in os.path.basename(csv_file):
                matching_csv = csv_file
                break
        
        if not matching_csv:
            print(f"No CSV file found for station {station_code}, keeping existing data")
            updated_stations.append(station)
            continue
        
        print(f"Found CSV file for {station_code}: {os.path.basename(matching_csv)}")
        
        # Copy the CSV file to public directory
        import shutil
        csv_filename = os.path.basename(matching_csv)
        shutil.copy2(matching_csv, os.path.join(public_csv_dir, csv_filename))
        station['csv_filename'] = csv_filename
        
        # Process the CSV data
        try:
            data = process_csv_file(matching_csv, station_code)
            station['n'] = len(data)
            
            # Update start and end dates based on actual data
            if data:
                station['start'] = data[0]['date']
                station['end'] = data[-1]['date']
            
            # Save individual station data file
            data_output_dir = os.path.join(data_dir, "data")
            os.makedirs(data_output_dir, exist_ok=True)
            
            filename = station['filename']
            with open(os.path.join(data_output_dir, filename), 'w') as f:
                json.dump(data, f, indent=2)
                
            updated_stations.append(station)
            print(f"Updated data file for {station_code}: {len(data)} daily records")
            
        except Exception as e:
            print(f"Error processing {station_code}: {e}")
            # Keep the original station data if processing fails
            updated_stations.append(station)
            continue
    
    return updated_stations

def main():
    # Paths
    data_dir = os.path.join(os.path.dirname(__file__), "public", "data")
    
    # Load existing stations
    print("Loading existing stations...")
    stations = load_stations()
    print(f"Found {len(stations)} existing stations")
    
    # Update stations with new CSV data
    print("Processing station data...")
    updated_stations = update_stations_data(stations, data_dir)
    
    # Save updated stations.json
    with open(f"{data_dir}/stations.json", 'w') as f:
        json.dump(updated_stations, f, indent=2)
    
    print(f"Updated stations.json with {len(updated_stations)} stations")
    
    # Update config.json
    config = {
        "daymet_last_year": 2023,
        "last_updated": datetime.now().isoformat()
    }
    
    with open(f"{data_dir}/config.json", 'w') as f:
        json.dump(config, f, indent=2)
    
    print("Updated config.json")
    print("Conversion complete!")

if __name__ == "__main__":
    main()