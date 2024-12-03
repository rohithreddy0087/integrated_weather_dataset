import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime

# Load your dataset (adjust the path accordingly)
MIN_LON = -120
MAX_LON = -115
MIN_LAT = 31.5
MAX_LAT = 38

def process_rutz_catalog(file_year):
    print(f"Processing Rutz AR Catalog for the year {file_year}...")
    nc_file_path = f'/gnn/rrr/integrated_weather_dataset/data/raw/Rutz_AR_Catalog/Rutz_ARCatalog_MERRA2_{file_year}.nc'  
    output_csv = f'/gnn/rrr/integrated_weather_dataset/data/processed/Rutz/{file_year}.csv'

    try:
        print("Opening NetCDF dataset...")
        ds = xr.open_dataset(nc_file_path)
        print("Dataset loaded successfully.")
        
        print("Filtering dataset by latitude and longitude bounds...")
        ds_filtered = ds.where(
            (ds["latitude"] >= MIN_LAT) & (ds["latitude"] <= MAX_LAT) &
            (ds["longitude"] >= MIN_LON) & (ds["longitude"] <= MAX_LON),
            drop=True
        )
        print("Filtering complete.")
        
        print("Converting filtered dataset to DataFrame...")
        df = ds_filtered.to_dataframe()
        print("Conversion complete.")

        print("Creating Timestamp column...")
        df["Timestamp"] = pd.to_datetime(
            df[["cal_year", "cal_mon", "cal_day", "cal_hour"]].astype(int).rename(
                columns={"cal_year": "year", "cal_mon": "month", "cal_day": "day", "cal_hour": "hour"}
            )
        )
        print("Timestamp column created.")
        
        print("Resetting index and dropping unnecessary columns...")
        df = df.reset_index().drop(columns=["nlon", "nlat", "ntim"])
        
        print("Selecting required columns...")
        result_df = df[["Timestamp", "longitude", "latitude", "IVT", "ARs"]]

        print(f"Saving processed data to {output_csv}...")
        result_df.to_csv(output_csv, mode='w', index=False)
        print(f"Data for {file_year} saved successfully.")

    except Exception as e:
        print(f"An error occurred while processing {file_year}: {e}")
for year in range(2020, 2025):
    process_rutz_catalog(year)
