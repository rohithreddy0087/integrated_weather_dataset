""" 
Creates labels for each year by merging both the Rutz and Guan's catalog's from ARTMIP
for a given range of grid latitudes and longitudes

This code generates a csv file for each year with field names as :
Timestamp, Latitude, Longitude, AR Label
"""
import xarray as xr
import pandas as pd

min_lon = -120
max_lon = -115
min_lat = 31.5
max_lat = 38

def open_netcdf(filename, start_date, end_date):
    ds = xr.open_dataset(filename, chunks={'time': 1460}, engine='netcdf4')
    ds = ds.squeeze()
    ds = ds.sel(time=slice(start_date, end_date), lat=slice(min_lat,max_lat), lon=slice(min_lon,max_lon))
    df = ds.to_dataframe().reset_index()
    df = df[['time', 'lat', 'lon', 'ar_binary_tag']]
    return df

for year in range(2000, 2017):
    print(year)
    start_date = f'{year}-01-01 00:00'
    end_date = f'{year}-12-31 00:00'

    guan_filename = f'/root/data/rrr/AR/guan/MERRA2.ar_tag.Guan_Waliser_v2.3hourly.{year}.nc'
    rutz_filename = f'/root/data/rrr/AR/rutz/MERRA2.ar_tag.Rutz.3hourly.{year}0101-{year}1231.nc'

    guan_df = open_netcdf(guan_filename, start_date, end_date).drop_duplicates(subset=['time', 'lat', 'lon']).reset_index(drop=True)
    rutz_df = open_netcdf(rutz_filename, start_date, end_date)

    guan_df = guan_df.rename(columns={'ar_binary_tag': 'Guan_AR_Label'})
    rutz_df = rutz_df.rename(columns={'ar_binary_tag': 'Rutz_AR_Label'})

    # union of both the rutz and guan dataframes
    merged_df = pd.merge(guan_df, rutz_df, on=['time', 'lat', 'lon'], how='outer')
    final_df = merged_df[(merged_df['Guan_AR_Label']==1) | (merged_df['Rutz_AR_Label']==1)]
    final_df = final_df.reset_index(drop=True)
    final_df.to_csv(f'/root/data/rrr/AR/labels/{year}.csv')