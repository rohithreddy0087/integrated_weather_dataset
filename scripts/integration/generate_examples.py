"""
Creates dataset by mapping troposphere zwd data with labels
"""
import pandas as pd
from datetime import datetime
import dask.dataframe as dd

def round_time_to_nearest_multiple(dt, hours_multiple):
    # dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    multiple_seconds = hours_multiple * 3600
    seconds_since_midnight = dt.hour * 3600 + dt.minute * 60 + dt.second
    nearest_multiple = round(seconds_since_midnight / multiple_seconds) * multiple_seconds
    total_seconds = dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() + nearest_multiple
    rounded_datetime = datetime.fromtimestamp(total_seconds)
    return rounded_datetime

def apply_rounding(df, hours_multiple):
    df['Rounded Timestamp'] = df['Timestamp'].apply(lambda x: round_time_to_nearest_multiple(x, hours_multiple))
    df['Rounded Longitude'] = df['Longitude'].apply(lambda x: grid_lons_lookup.get(int(round(x * 1000)), None))
    df['Rounded Latitude'] = df['Latitude'].apply(lambda x: grid_lats_lookup.get(int(round(x * 1000)), None))
    return df

# temporal and spatial resolution for AR catalogs
longitude_interval = 0.625
latitude_interval = 0.5
hours_multiple = 3

# loop thorough each year
for yyyy in range(2004, 2017):
    print(f"Processing year {yyyy}")
    data_file_path = f"/root/data/rrr/AR/dataset/troposphere_data/{yyyy}.csv"
    label_file_path = f"/root/data/rrr/AR/dataset/labels/{yyyy}.csv"

    # read in zwd data
    data_df = dd.read_csv(data_file_path)
    data_df['Timestamp'] = data_df['Timestamp'].astype(str)
    data_df['Timestamp'] = dd.to_datetime(data_df['Timestamp'], errors='coerce')
    data_df = data_df.dropna()

    # read in label data
    label_df = dd.read_csv(label_file_path, parse_dates=['time'])
    label_df = label_df.drop(columns=['Unnamed: 0'])

    # create a dictionary with keys as grid longitudes and values as all the longitudes with 0.001 precision 
    grid_lons = label_df['lon'].compute().unique()
    longitude_interval_threshold = longitude_interval/2
    grid_lons_lookup = {}
    for item in grid_lons:
        lower = item - longitude_interval_threshold
        upper = item + longitude_interval_threshold
        for key in range(int(lower * 1000), int(upper * 1000) + 1):
            grid_lons_lookup[key] = item
            
    grid_lats = label_df['lat'].compute().unique()
    latitude_interval_threshold = latitude_interval/2
    grid_lats_lookup = {}
    for item in grid_lats:
        lower = item - latitude_interval_threshold
        upper = item + latitude_interval_threshold
        for key in range(int(lower * 1000), int(upper * 1000) + 1):
            grid_lats_lookup[key] = item
    
    # Round the timestamp in the zwd files to the nearest 3hr timestamp
    # Round the site lat,lon to the nearest grid lat,lon available in the AR catalogs
    data_df['Timestamp'] = dd.to_datetime(data_df['Timestamp'])
    data_df = data_df.map_partitions(apply_rounding, hours_multiple)
    data_df = data_df.compute()

    label_df = label_df.rename(columns={
        'time': 'Rounded Timestamp',
        'lat': 'Rounded Latitude',
        'lon': 'Rounded Longitude'
    })

    # Left Merge the zwd and label df
    merged_df = pd.merge(
        data_df,
        label_df.compute(),
        on=['Rounded Timestamp', 'Rounded Latitude', 'Rounded Longitude'],
        how='left'
    )

    merged_df['Guan_AR_Label'] = merged_df['Guan_AR_Label'].fillna(0).astype(int)
    merged_df['Rutz_AR_Label'] = merged_df['Rutz_AR_Label'].fillna(0).astype(int)

    merged_df = merged_df.drop(columns=["Rounded Timestamp", "Rounded Longitude", "Rounded Latitude"])

    merged_df.to_parquet(f'/root/data/rrr/AR/dataset/parquet/{yyyy}.parquet')