import pandas as pd
from datetime import datetime, timedelta
from utils.utils import Trop2DataReader
import urllib.request
import gzip
import io
import sys
sys.path.append('/root/data/rrr/integrated_weather_dataset/')
from config.config import read_sites

start_year = 2023
end_year = 2023  
start_day = 1 
end_day = 365  

# Read the list of sites
_, sites_list = read_sites(station_list_file="/root/data/rrr/integrated_weather_dataset/config/stations.txt")

# Downloads the data
url = 'http://garner.ucsd.edu/pub/measuresESESES_products/Troposphere'
output_dir = '/root/data/rrr/integrated_weather_dataset/data/processed/Troposphere'

print(f"Starting data download for year {start_year} from day {start_day} to day {end_day}...")

# Loop over the days in the specified range
df_list = []
for day in [str(i).zfill(3) for i in range(start_day, end_day + 1)]:
    print(f"Processing day {day} of year {start_year}...")
    for (site, lat, lon) in sites_list:
        url_path = f'{url}/{start_year}/{day}/JPS2_SES_FIN_{start_year}{day}0000_30H_05M_{site}_TRO.TRO.gz'
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

                filter_date = datetime(start_year, 1, 1) + timedelta(days=int(day) - 1)
                temp = temp[temp["Timestamp"].dt.date == filter_date.date()]
                df_list.append(temp)
        except Exception as e:
            print(f"Error processing site {site} on day {day}, year {start_year}: {e}")
            continue

    if len(df_list) > 0:
        zwd_df = pd.concat(df_list)
        file_path = f'{output_dir}/{start_year}.csv'
        if day == '001':
            zwd_df.to_csv(file_path, mode='w', index=False, header=True)
            print(f"Created new file for year {start_year}: {file_path}")
        else:
            zwd_df.to_csv(file_path, mode='a', index=False, header=False)
            print(f"Appended data to file for year {start_year}: {file_path}")


print("Data download and processing completed.")
