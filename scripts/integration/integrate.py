import pandas as pd
import dask.dataframe as dd
from datetime import datetime
import time
import json
import logging
import sys
sys.path.append('/root/data/rrr/integrated_weather_dataset/')

start_year = int(sys.argv[1])
end_year = int(sys.argv[2])

longitude_interval_ar = 0.625
latitude_interval_ar = 0.5
hours_multiple_ar = 3

longitude_interval_precip = 0.1
latitude_interval_precip = 0.1
hours_multiple_precip = 0.5  # For 30 minutes

def get_logger(name = 'integrate', log_file = 'integrate_debug.log'):
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

# Helper function to round timestamps to the nearest specified multiple of hours
def round_time_to_nearest_multiple(dt, hours_multiple):
    """Round a datetime to the nearest multiple of hours."""
    multiple_seconds = hours_multiple * 3600
    seconds_since_midnight = dt.hour * 3600 + dt.minute * 60 + dt.second
    nearest_multiple = round(seconds_since_midnight / multiple_seconds) * multiple_seconds
    total_seconds = dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() + nearest_multiple
    return datetime.fromtimestamp(total_seconds)

def apply_rounding(df, hours_multiple, grid_lons_lookup, grid_lats_lookup):
    df['Rounded Timestamp'] = df['Timestamp'].apply(lambda x: round_time_to_nearest_multiple(x, hours_multiple))
    df['Rounded Longitude'] = df['Longitude'].apply(lambda x: grid_lons_lookup.get(int(round(x * 1000)), None))
    df['Rounded Latitude'] = df['Latitude'].apply(lambda x: grid_lats_lookup.get(int(round(x * 1000)), None))
    return df

# Helper function to create lookup grids for rounding coordinates
def create_grid_lookup(grid_data, interval):
    """Create a lookup dictionary for coordinate rounding."""
    interval_threshold = interval / 2
    grid_lookup = {}
    for item in grid_data:
        lower = item - interval_threshold
        upper = item + interval_threshold
        for key in range(int(lower * 1000), int(upper * 1000) + 1):
            grid_lookup[key] = item
    return grid_lookup

def merge_zwd_with_data(zwd_df, data_df, longitude_interval, latitude_interval, hours_multiple, name = 'Rutz'):
    """
    Merges ZWD data with another dataset (e.g., AR catalogs) by creating a grid lookup
    for spatial rounding and timestamp rounding. Includes both exact match and label assignment.

    Parameters:
        zwd_df (pd.DataFrame): ZWD dataset with 'Timestamp', 'Longitude', and 'Latitude'.
        data_df (pd.DataFrame): Dataset to merge with, including 'time', 'lon', 'lat', and 'label'.
        longitude_interval (float): Interval for longitude rounding.
        latitude_interval (float): Interval for latitude rounding.
        hours_multiple (int): Interval for timestamp rounding in hours.

    Returns:
        pd.DataFrame: ZWD dataset merged with the other dataset, including exact match and label columns.
    """
    
    
    # Create grid lookups
    grid_lons_lookup = create_grid_lookup(data_df['Longitude'].unique(), longitude_interval)
    grid_lats_lookup = create_grid_lookup(data_df['Latitude'].unique(), latitude_interval)

    # Apply rounding
    zwd_df = apply_rounding(zwd_df, hours_multiple, grid_lons_lookup, grid_lats_lookup)
    
    # Rename columns for merging
    data_df = data_df.rename(columns={
        'Latitude': 'Rounded Latitude',
        'Longitude': 'Rounded Longitude',
        'Label': f'{name}_Label'  # Rename 'label' to a specific column for clarity
    })
    # Merge for exact match

    merged_df = pd.merge(
        zwd_df,
        data_df,
        on=['Timestamp', 'Rounded Longitude', 'Rounded Latitude'],  # Exact match on original coordinates and timestamp
        how='left'
    )

    # Rename columns for merging
    data_df = data_df.rename(columns={
        'Timestamp': 'Rounded Timestamp',
    })

    # Merge for approximate match (spatial and timestamp rounding)
    merged_df = pd.merge(
        merged_df,
        data_df,
        on=['Rounded Timestamp', 'Rounded Latitude', 'Rounded Longitude'],
        how='left',
        suffixes=('', '_approx')  # To differentiate between exact match and approximate
    )

    # Assign approximate match label
    # merged_df[f'{name}_Label_approx'] = merged_df[f'{name}_Label_approx'].fillna(0).astype(int)

    # Clean up intermediate columns if necessary
    merged_df.drop(
        columns=['Rounded Timestamp', 'Rounded Latitude', 'Rounded Longitude'],
        inplace=True,
        errors='ignore'
    )
    
    merged_df = merged_df.rename(columns={
        f'{name}_Label': f'{name}_Label_exact',
    })

    return merged_df

def merge_with_ffw(trop_df, ffw_data):
    trop_df['ffw'] = 0
    trop_df['Timestamp'] = pd.to_datetime(trop_df['Timestamp'])
    # Process each flash flood warning entry
    for ffw in ffw_data:
        t1 = time.time()
        begin = pd.to_datetime(ffw['begin']).tz_localize(None)
        sites = ffw['sites']
        if begin.year != yyyy or len(sites)==0:
            continue
        end = pd.to_datetime(ffw['end']).tz_localize(None)
        # Filter rows matching the time range and sites
        mask = (trop_df['Timestamp'] >= begin) & (trop_df['Timestamp'] <= end)
        if sites:
            mask &= trop_df['Site'].isin(sites)

        # Set 'ffw' column for matching rows
        trop_df.loc[mask, 'ffw'] = 1
        logger.debug(f"ffw {begin}, {end}, {time.time()-t1}")
    return trop_df

def sort_ffw_data(ffw_data, yyyy):
    for entry in ffw_data:
        entry['begin_dt'] = pd.to_datetime(entry['begin'])

    # Filter entries with year 2005
    filtered_data = [entry for entry in ffw_data if entry['begin_dt'].year == yyyy and len(entry['sites'])>0]

    # Sort the filtered entries by 'begin' timestamp
    sorted_data = sorted(filtered_data, key=lambda x: x['begin_dt'])

    # Remove the temporary 'begin_dt' key before returning the result
    for entry in sorted_data:
        del entry['begin_dt']
    
    return sorted_data
        
def get_data(yyyy):
    zwd_file_path = f"/root/data/rrr/integrated_weather_dataset/data/processed/Troposphere/final/{yyyy}.csv"
    zwd_df = pd.read_csv(zwd_file_path)
    zwd_df['Timestamp'] = zwd_df['Timestamp'].astype(str)
    zwd_df['Timestamp'] = pd.to_datetime(zwd_df['Timestamp'], errors='coerce')
    zwd_df = zwd_df.dropna()
    
    # Read AR catalog data (Rutz)
    rutz_file_path = f"/root/data/rrr/integrated_weather_dataset/data/processed/Rutz/{yyyy}.csv"
    rutz_df = pd.read_csv(rutz_file_path)
    rutz_df = rutz_df.rename(columns={"longitude": "Longitude"})
    rutz_df = rutz_df.rename(columns={"latitude": "Latitude"})
    rutz_df['Timestamp'] = pd.to_datetime(rutz_df['Timestamp'], errors='coerce')
    rutz_df = rutz_df.rename(columns={f'ARs': f'Label'})
    
    # Read AR catalog data (Guan)
    guan_file_path = f"/root/data/rrr/integrated_weather_dataset/data/processed/Guan/{yyyy}.csv"
    guan_df = pd.read_csv(guan_file_path)
    guan_df['Timestamp'] = pd.to_datetime(guan_df['Timestamp'], errors='coerce')
    guan_df = guan_df.rename(columns={f'Guan_AR_Label': f'Label'})
    
    # Read Precipitation data
    precip_file_path = f"/root/data/rrr/integrated_weather_dataset/data/processed/Precipitation/{yyyy}.csv"
    precip_df = pd.read_csv(precip_file_path)
    precip_df['Timestamp'] = pd.to_datetime(precip_df['Timestamp'], errors='coerce')
    precip_df = precip_df.rename(columns={f'Precipitation': f'Label'})
        
    return zwd_df, rutz_df, guan_df, precip_df

def process_year(yyyy):
    logger.debug(f"{yyyy} Started")
    sorted_ffw_data = sort_ffw_data(ffw_data, yyyy)
        
    zwd_df, rutz_df, guan_df, precip_df = get_data(yyyy)
    logger.debug(f"{yyyy} Loaded dataframes")
    zwd_with_rutz = merge_zwd_with_data(zwd_df, rutz_df, longitude_interval_ar, latitude_interval_ar, hours_multiple_ar, name = 'Rutz')
    logger.debug(f"{yyyy} Merged zwd with rutz")
    zwd_rutz_guan = merge_zwd_with_data(zwd_with_rutz, guan_df, longitude_interval_ar, latitude_interval_ar, hours_multiple_ar, name = 'Guan')
    logger.debug(f"{yyyy} Merged zwd_rutz with guan")
    zwd_rutz_guan_precip = merge_zwd_with_data(zwd_rutz_guan, precip_df, longitude_interval_precip, latitude_interval_precip, hours_multiple_precip, name = "Precipitation")
    logger.debug(f"{yyyy} Merged zwd_rutz_guan with precipiation")
    final_df = merge_with_ffw(zwd_rutz_guan_precip, sorted_ffw_data)
    logger.debug(f"{yyyy} Merged flash flood data")
    final_df.to_csv(f'/root/data/rrr/integrated_weather_dataset/data/integrated/{yyyy}.csv')

if __name__ == "__main__":
    logger = get_logger(log_file=f'integrate_debug_{start_year}.log')
    logger.debug(f"Integrating data for {start_year} to {end_year}")
    print(f"Starting integration for years {start_year} to {end_year}...")

    ffw_file_path = f"/root/data/rrr/integrated_weather_dataset/data/processed/Flash_Flood/data.json"
    with open(ffw_file_path, 'r') as f:
        ffw_data = json.load(f)
    
    for yyyy in range(start_year, end_year + 1):
        print(f"Processing year {yyyy}...")
        logger.debug(f"Processing year {yyyy}...")
        try:
            process_year(yyyy)
            print(f"Year {yyyy} processed successfully.")
            logger.info(f"Year {yyyy} processed successfully.")
        except Exception as e:
            print(f"Error processing year {yyyy}: {e}")
            logger.error(f"Error processing year {yyyy}: {e}", exc_info=True)

    print(f"Integration completed for years {start_year} to {end_year}.")
    logger.info(f"Integration completed for years {start_year} to {end_year}.")
