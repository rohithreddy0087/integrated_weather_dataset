import os
import numpy as np
import xarray as xr
import pandas as pd
import warnings

warnings.filterwarnings("ignore")


def process_guan_data(year):
    filename = '/gnn/rrr/integrated_weather_dataset/data/raw/Guan_AR_Catalog/globalARcatalog_MERRA2_1980-2023_v4.0.nc'
    output_file = f'/gnn/rrr/integrated_weather_dataset/data/processed/Guan_MERRA/{year}.csv'
    label_file = f'/gnn/rrr/ES3-TACLS/AR/dataset/labels/{year}.csv'

    ds = xr.open_dataset(filename, chunks={'time': 1460}, engine='netcdf4')
    ds = ds.squeeze()
    ds = ds.reset_coords(names=['lev', 'ens'], drop=True)

    MIN_LON = -120 + 360
    MAX_LON = -115 + 360
    MIN_LAT = 31.5
    MAX_LAT = 38
    start_date = f'{year}-01-01T00:00:00.000000000'
    end_date = f'{year}-12-31T00:00:00.000000000'
    ds = ds.sel(time=slice(start_date, end_date), lat=slice(MIN_LAT, MAX_LAT), lon=slice(MIN_LON, MAX_LON))
    print(f"Data selected for year {year} -> date range: {start_date} to {end_date}, "
          f"lat: {MIN_LAT} to {MAX_LAT}, lon: {MIN_LON} to {MAX_LON}")

    df = ds.shapemap.to_dataframe(dim_order=['time', 'lat', 'lon']).dropna()
    df = df.reset_index()
    df['time'] = pd.to_datetime(df['time'])
    df['Guan_AR_Label'] = df['shapemap'].notna().astype(int)

    all_times = pd.date_range(start=start_date, end=end_date, freq='3h')
    all_combinations = pd.MultiIndex.from_product(
        [all_times, df['lat'].unique(), df['lon'].unique()],
        names=['time', 'lat', 'lon']
    ).to_frame(index=False)
    print(f"Generated all combinations for year {year}: {len(all_combinations)} rows")

    merged_df = pd.merge(
        all_combinations,
        df,
        on=['time', 'lat', 'lon'],
        how='left'
    ).fillna({'Guan_AR_Label': 0})
    print(f"Data merged with all combinations for year {year}.")

    merged_df.rename(columns={'time': 'Timestamp', 'lat': 'Latitude', 'lon': 'Longitude'}, inplace=True)
    merged_df['Longitude'] = ((merged_df['Longitude'] + 180) % 360) - 180
    df_final = merged_df[['Timestamp', 'Latitude', 'Longitude', 'Guan_AR_Label']]

    df2 = pd.read_csv(label_file, index_col=False)
    df2['time'] = pd.to_datetime(df2['time'])
    all_combinations_2 = pd.MultiIndex.from_product(
        [all_times, df2['lat'].unique(), df2['lon'].unique()],
        names=['time', 'lat', 'lon']
    ).to_frame(index=False)
    print(f"Generated all combinations for secondary labels for year {year}: {len(all_combinations_2)} rows")

    all_combinations_2['Guan_AR_Label'] = 0
    concat_df = pd.concat([all_combinations_2, df2], ignore_index=True)
    result_df = concat_df.drop_duplicates(subset=['time', 'lat', 'lon'], keep='last')
    result_df['Guan_AR_Label'] = result_df['Guan_AR_Label'].fillna(0)

    df_final.reset_index(drop=True, inplace=True)
    result_df.reset_index(drop=True, inplace=True)
    df_final['Guan_AR_Label'] = result_df['Guan_AR_Label']
    
    df_final.to_csv(output_file, index=False)
    print(f"Processed data for year {year} saved to {output_file}.")

for i in range(2017,2024):
    process_guan_data(i)
