#!/usr/bin/env python3
"""
Download air temperature data from Daymet API for all stations
"""

import pandas as pd
import requests
import json
import os
import time
from datetime import datetime

def load_metadata():
    """Load station metadata from the text file"""
    metadata_file = "/Users/ahzs645/Github/watertemp/unbcwatertemp/Nechako watershed data/02_SupplementaryMaterials/Site_Metadata.txt"
    
    # Read the metadata file with proper encoding
    metadata = pd.read_csv(metadata_file, sep='\t', encoding='latin-1')
    
    # Clean up column names
    metadata.columns = ['station_code', 'site_name', 'latitude', 'longitude', 
                       'elevation_m', 'record_start', 'record_end']
    
    return metadata

def download_daymet_data(lat, lon, start_year, end_year, station_code):
    """Download Daymet data for a specific location and time period"""
    
    # Daymet API URL
    base_url = "https://daymet.ornl.gov/single-pixel/api/data"
    
    # Parameters for the API request
    params = {
        'lat': lat,
        'lon': lon,
        'vars': 'tmax,tmin',  # Get both max and min temperature
        'start': f"{start_year}-01-01",
        'end': f"{end_year}-12-31"
    }
    
    print(f"Downloading data for {station_code} ({lat}, {lon}) from {start_year} to {end_year}")
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        
        # Save the raw CSV data
        output_dir = "/Users/ahzs645/Github/watertemp/unbcwatertemp-viz/daymet_data"
        os.makedirs(output_dir, exist_ok=True)
        
        output_file = f"{output_dir}/{station_code}_daymet.csv"
        with open(output_file, 'w') as f:
            f.write(response.text)
        
        print(f"Saved data to {output_file}")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading data for {station_code}: {e}")
        return False

def process_daymet_csv(csv_file):
    """Process Daymet CSV to extract daily mean temperatures"""
    # Read the CSV, skipping the header rows
    df = pd.read_csv(csv_file, skiprows=6)
    
    # Calculate daily mean temperature from tmax and tmin
    df['tmean'] = (df['tmax (deg c)'] + df['tmin (deg c)']) / 2
    
    # Create date column from year and yday
    df['date'] = pd.to_datetime(df['year'].astype(str) + df['yday'].astype(str), format='%Y%j')
    
    # Return date and mean temperature
    return df[['date', 'tmean']]

def main():
    # Load metadata
    print("Loading station metadata...")
    metadata_df = load_metadata()
    print(f"Found {len(metadata_df)} stations")
    
    # Download data for each station
    for _, row in metadata_df.iterrows():
        station_code = row['station_code']
        lat = row['latitude']
        lon = row['longitude']
        
        # Parse start and end dates
        start_date = pd.to_datetime(row['record_start'])
        end_date = pd.to_datetime(row['record_end'])
        
        start_year = start_date.year
        end_year = min(end_date.year, 2023)  # Daymet data available up to 2023
        
        # Download the data
        success = download_daymet_data(lat, lon, start_year, end_year, station_code)
        
        if success:
            # Process the downloaded data
            csv_file = f"/Users/ahzs645/Github/watertemp/unbcwatertemp-viz/daymet_data/{station_code}_daymet.csv"
            air_temp_df = process_daymet_csv(csv_file)
            
            # Save processed data
            processed_file = f"/Users/ahzs645/Github/watertemp/unbcwatertemp-viz/daymet_data/{station_code}_airtemp.json"
            air_temp_dict = {}
            for _, row in air_temp_df.iterrows():
                date_str = row['date'].strftime('%Y-%m-%d')
                air_temp_dict[date_str] = round(row['tmean'], 2)
            
            with open(processed_file, 'w') as f:
                json.dump(air_temp_dict, f, indent=2)
            
            print(f"Processed air temperature data saved to {processed_file}")
        
        # Be nice to the API - wait between requests
        time.sleep(1)
    
    print("Air temperature download complete!")

if __name__ == "__main__":
    main()