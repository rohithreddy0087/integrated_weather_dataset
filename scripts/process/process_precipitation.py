import h5py
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import logging

# from config.config import *
MIN_LON = -120
MAX_LON = -115
MIN_LAT = 31.5
MAX_LAT = 38

def get_logger(name='dataset', log_file='precipitation_debug.log'):
    """helper function to get a logger object

    Args:
        name (str, optional): Logger name. Defaults to 'dataset'.
        log_file (str, optional): Debug file name. Defaults to 'debug.log'.

    Returns:
        logger object: returns a logger object
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(log_file)
    
    file_handler.setLevel(logging.DEBUG)
    console_handler.setLevel(logging.DEBUG)
    
    console_formatter = logging.Formatter('%(message)s')
    file_formatter = logging.Formatter('%(message)s')

    console_handler.setFormatter(console_formatter)
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger

def process_hdf5_file(file_path, output_path):
    file_name = file_path.split("/")[-1] 
    timestamp_str = file_name.split(".")[0]
    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d-%H%M%S")
    hdf = h5py.File(file_path, 'r')
    logger.debug(f"Processing file: {file_name}, Timestamp: {timestamp}")
    lon = hdf['lon'][:]
    lat = hdf['lat'][:]
    precipitation = hdf['precipitation'][0]

    lon_indices = np.where((lon >= MIN_LON) & (lon <= MAX_LON))[0]
    lat_indices = np.where((lat >= MIN_LAT) & (lat <= MAX_LAT))[0]

    precipitation_filtered = precipitation[np.ix_(lon_indices, lat_indices)]
    
    data_list = []
    for i, lon_idx in enumerate(lon_indices):
        for j, lat_idx in enumerate(lat_indices):
            data_list.append({
                'Timestamp': str(timestamp),
                'Longitude': lon[lon_idx],
                'Latitude': lat[lat_idx],
                'Precipitation': precipitation_filtered[i, j]
            })
    
    df = pd.DataFrame(data_list)
    
    output_csv = output_path+f'{timestamp.year}.csv'
    
    if os.path.exists(output_csv):
        print(f"Appending to {output_csv}")
    else:
        print(f"Creating new file: {output_csv}")
    
    df.to_csv(output_csv, mode='a', header=not os.path.exists(output_csv), index=False)
    print(f"Finished processing file: {file_name}")

def process_all_files(folder_path, output_path):
    files = [f for f in os.listdir(folder_path) if 'nc' in f]
    files = sorted(files)
    total_files = len(files)
    print(f"Total files to process: {total_files}")
    
    for i, file in enumerate(files):
        file_path = os.path.join(folder_path, file)
        try:
            print(f"Processing file {i+1}/{total_files}: {file}")
            process_hdf5_file(file_path, output_path)
            print(f"Successfully processed file {i+1}/{total_files}: {file}")
        except Exception as e:
            err_logger.error("Error %s, %s", file_path, e)
            print(f"Error processing file {i+1}/{total_files}: {file}, Error: {e}")

logger = get_logger()
err_logger = get_logger(log_file="err_final_precipitation.log")
folder_path = '/root/data/rrr/integrated_weather_dataset/data/raw/Precipitation/data/'  # Replace with the actual folder path
output_path = '/root/data/rrr/integrated_weather_dataset/data/processed/Precipitation/data/'  # Output file
process_all_files(folder_path, output_path)
