
import pandas as pd
from datetime import datetime, timedelta
from utils.utils import Trop2DataReader
import urllib.request
import gzip
import io
import sys
sys.path.append('/root/data/rrr/integrated_weather_dataset/')
from config.config import read_sites

start_year = 2003
end_year =  2005

_, sites_list = read_sites(station_list_file="/root/data/rrr/integrated_weather_dataset/config/stations.txt")
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)

# Downloads the data
url = 'http://garner.ucsd.edu/pub/measuresESESES_products/Troposphere'
local_filename = 'tmp.gz'
output_dir = '/root/data/rrr/integrated_weather_dataset/data/processed/Troposphere'

print(f"Starting data download for the 366th day in leap years from {start_year} to {end_year - 1}...")

for year in range(start_year, end_year):
    # Check for leap year
    if year % 4 == 0:
        print(f"Processing 366th day for leap year: {year}")
        day = '366'
        df_list = []
        for (site, lat, lon) in sites_list:
            url_path = f'{url}/{year}/{day}/JPS2_SES_FIN_{year}{day}0000_30H_05M_{site}_TRO.TRO.gz'
            try:
                with urllib.request.urlopen(url_path) as response:
                    compressed_file = response.read()
                with gzip.GzipFile(fileobj=io.BytesIO(compressed_file)) as decompressed_file:
                    trop_reader = Trop2DataReader(decompressed_file)
                    zwd_values, datetime_values = trop_reader.make_lst()
                    num_entries = len(zwd_values)
                    trop_data = {
                        "Timestamp": datetime_values,
                        "Site": [site] * num_entries,
                        "Latitude": [lat] * num_entries,
                        "Longitude": [lon] * num_entries,
                        "ZWD": zwd_values
                    }
                    temp = pd.DataFrame(trop_data)
                    print(temp)
                    filter_date = datetime(year, 1, 1) + timedelta(days=int(day) - 1)
                    temp = temp[temp["Timestamp"].dt.date == filter_date.date()]
                    df_list.append(temp)
            except Exception as e:
                print(f"Error processing site {site} on 366th day, year {year}: {e}")
                continue
        
        if len(df_list) > 0:
            zwd_df = pd.concat(df_list)
            print(zwd_df)
            file_path = f'{output_dir}/{year}.csv'
            if day == '366':
                zwd_df.to_csv(file_path, mode='a', index=False, header=False)
                print(f"Appended 366th day data to file for year {year}: {file_path}")

print("Processing of 366th day in leap years completed.")
