import os, sys
import numpy as np
import xarray as xr
import pandas as pd

filename = '/gnn/rrr/integrated_weather_dataset/data/raw/Guan_AR_Catalog/globalARcatalog_MERRA2_1980-2023_v4.0.nc'

print("Opening dataset...")
ds = xr.open_dataset(filename, chunks={'time': 1460}, engine='netcdf4')
ds = ds.squeeze()
ds = ds.reset_coords(names=['lev', 'ens'], drop=True)

MIN_LON = -120 + 360
MAX_LON = -115 + 360
MIN_LAT = 31.5
MAX_LAT = 38
start_date = '2016-04-01 00:00'
end_date = '2016-05-01 00:00'
ds = ds.sel(time=slice(start_date, end_date), lat=slice(MIN_LAT, MAX_LAT), lon=slice(MIN_LON, MAX_LON))
print(f"Data selected for lat: {MIN_LAT} to {MAX_LAT} and lon: {MIN_LON} to {MAX_LON}")

df = ds.kidmap.to_dataframe(dim_order=['time', 'lat', 'lon']).dropna()
print(df)
print(df.info())
print(df.head())
df = df.reset_index()
df.to_csv('/gnn/rrr/integrated_weather_dataset/data/processed/Guan_MERRA/guan_merra.csv')
df['Label'] = df['kidmap'].apply(lambda x: 1 if x > 0 else 0)
print(df['Label'].value_counts())
all_times = pd.date_range(start=start_date, end=end_date, freq='3H')  # 3-hour intervals
all_combinations = pd.MultiIndex.from_product(
    [all_times, df['lat'].unique(), df['lon'].unique()],
    names=['time', 'lat', 'lon']
).to_frame(index=False)
print(f"Generated all combinations of time, latitude, and longitude: {len(all_combinations)} rows")
merged_df = pd.merge(
    all_combinations,
    df,
    on=['time', 'lat', 'lon'],
    how='left'
).fillna({'Label': 0})
print("Data merged with all combinations.")
print(merged_df['Label'].value_counts())
merged_df.rename(columns={'time': 'Timestamp', 'lat': 'Latitude', 'lon': 'Longitude'}, inplace=True)
print("Renamed columns.")

df_final = merged_df[['Timestamp', 'Latitude', 'Longitude', 'Label']]
df_final['Longitude'] = ((df_final['Longitude'] + 180) % 360) - 180

print("Final dataframe prepared.")

output_path = '/gnn/rrr/integrated_weather_dataset/data/processed/Guan_MERRA/guan_merra_april_16.csv'
df_final.to_csv(output_path)
print(f"Data saved as csv file at {output_path}")
