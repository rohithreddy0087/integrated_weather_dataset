import h5py
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

from config.config import *

def process_hdf5_file(file_path, output_csv):
    # Extract the date, start time, and end time from the filename
    file_name = os.path.basename(file_path)
    parts = file_name.split('.')
    date_part = parts[4].split('-')
    
    date = date_part[0]
    start_time = date_part[1][1:]
    end_time = date_part[2][1:]
    date = datetime.strptime(date, "%Y%m%d").date()

    start_time = datetime.strptime(start_time, "%H%M%S")
    end_time = datetime.strptime(end_time, "%H%M%S")

    # Calculate the average time between start and end
    avg_time = start_time + (end_time - start_time) / 2
    avg_timestamp = datetime.combine(date, avg_time.time()).strftime("%Y-%m-%d %H:%M:%S")

    
    # Open the HDF5 file
    with h5py.File(file_path, 'r') as hdf:
        print(f"Date {avg_timestamp}")
        lon = hdf['Grid']['lon'][:]
        lat = hdf['Grid']['lat'][:]
        precipitation = hdf['Grid']['precipitation'][0]

        # Find the indices of lon and lat that are within the desired range
        lon_indices = np.where((lon >= MIN_LON) & (lon <= MAX_LON))[0]
        lat_indices = np.where((lat >= MIN_LAT) & (lat <= MAX_LAT))[0]
        
        # Filter the precipitation data
        precipitation_filtered = precipitation[np.ix_(lon_indices, lat_indices)]
        
        # Prepare the data for saving
        data_list = []
        for i, lon_idx in enumerate(lon_indices):
            for j, lat_idx in enumerate(lat_indices):
                data_list.append({
                    'Timestamp': avg_timestamp,
                    'Longitude': lon[lon_idx],
                    'Latitude': lat[lat_idx],
                    'Precipitation': precipitation_filtered[i, j]
                })
        
        # Convert to DataFrame
        df = pd.DataFrame(data_list)
        
        # Append the data to the CSV file
        df.to_csv(output_csv, mode='a', header=not os.path.exists(output_csv), index=False)

def process_all_files(folder_path, output_csv):
    # Get all HDF5 files in the folder
    files = [f for f in os.listdir(folder_path) if 'HDF5' in f]
    files = sorted(files)
    for file in files:
        file_path = os.path.join(folder_path, file)
        process_hdf5_file(file_path, output_csv)

# Usage
folder_path = '../../data/raw/Precipitation/data/'  # Replace with the actual folder path
output_csv = '../../data/processed/precipitation_data.csv'  # Output file
process_all_files(folder_path, output_csv)