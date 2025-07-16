#!/usr/bin/env python3
"""
Convert Nechako watershed CSV data to JSON format for the visualization tool
"""

import pandas as pd
import json
import os
from datetime import datetime
import glob

def load_metadata():
    """Load station metadata from the text file"""
    metadata_file = "/Users/ahzs645/Github/watertemp/unbcwatertemp/Nechako watershed data/02_SupplementaryMaterials/Site_Metadata.txt"
    
    # Read the metadata file with proper encoding
    metadata = pd.read_csv(metadata_file, sep='\t', encoding='latin-1')
    
    # Clean up column names
    metadata.columns = ['station_code', 'site_name', 'latitude', 'longitude', 
                       'elevation_m', 'record_start', 'record_end']
    
    return metadata

def process_csv_file(csv_file, metadata_df):
    """Process a single CSV file and convert to daily averages"""
    print(f"Processing {csv_file}")
    
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Convert timestamp to datetime
    df['timestamp (UTC)'] = pd.to_datetime(df['timestamp (UTC)'])
    
    # Extract date
    df['date'] = df['timestamp (UTC)'].dt.date
    
    # Group by date and calculate daily statistics
    daily_stats = df.groupby('date').agg({
        'wtmp (°C)': ['mean', 'min', 'max', 'count']
    }).round(2)
    
    # Handle NaN values by replacing them with None later
    
    # Flatten column names
    daily_stats.columns = ['temp_c', 'min_temp_c', 'max_temp_c', 'n_values']
    
    # Reset index to make date a column
    daily_stats = daily_stats.reset_index()
    
    # Convert date to string
    daily_stats['date'] = daily_stats['date'].astype(str)
    
    # Add placeholder air temperature (would come from Daymet in real pipeline)
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

def create_stations_json(metadata_df, data_dir):
    """Create the stations.json file"""
    stations = []
    
    for _, row in metadata_df.iterrows():
        station_code = row['station_code']
        
        # Find corresponding CSV file
        csv_files = glob.glob(f"{data_dir}/{station_code}_*.csv")
        if not csv_files:
            print(f"No CSV file found for station {station_code}")
            continue
            
        csv_file = csv_files[0]
        filename = os.path.basename(csv_file).replace('.csv', '.json')
        
        # Extract waterbody name from site name
        site_name = row['site_name'].strip('*').strip('�').strip('\u00a0').strip()  # Remove special characters
        
        station = {
            "station_id": station_code,
            "provider_station_code": f"UNBC:{station_code}",
            "station_code": station_code,
            "station_description": site_name,
            "waterbody_name": site_name.split(' above ')[0].split(' below ')[0].split(' at ')[0],
            "latitude": float(row['latitude']),
            "longitude": float(row['longitude']),
            "provider_code": "UNBC",
            "provider_name": "University of Northern British Columbia",
            "dataset": "UNBC",
            "url": f"https://watertemp.unbc.ca/#/explorer/stations/{station_code}",
            "filename": filename,
            "start": str(row['record_start']),
            "end": str(row['record_end']),
            "n": 0  # Will be calculated when processing data
        }
        
        # Process the CSV data
        try:
            data = process_csv_file(csv_file, metadata_df)
            station['n'] = len(data)
            
            # Save individual station data file
            data_output_dir = f"{data_dir}/data"
            os.makedirs(data_output_dir, exist_ok=True)
            
            with open(f"{data_output_dir}/{filename}", 'w') as f:
                json.dump(data, f, indent=2)
                
            stations.append(station)
            print(f"Created data file for {station_code}: {len(data)} daily records")
            
        except Exception as e:
            print(f"Error processing {station_code}: {e}")
            continue
    
    return stations

def main():
    # Paths
    data_dir = "/Users/ahzs645/Github/watertemp/unbcwatertemp-viz/public/data"
    
    # Load metadata
    print("Loading station metadata...")
    metadata_df = load_metadata()
    print(f"Found {len(metadata_df)} stations")
    
    # Create stations.json
    print("Processing station data...")
    stations = create_stations_json(metadata_df, data_dir)
    
    # Save stations.json
    with open(f"{data_dir}/stations.json", 'w') as f:
        json.dump(stations, f, indent=2)
    
    print(f"Created stations.json with {len(stations)} stations")
    
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