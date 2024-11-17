"""
This file downloads the troposphere data from the garner site
for given three years as input arguments and saves them into a csv file
for all the sites inside a given min and max latitudes, longitudes
"""
import pandas as pd
from datetime import datetime, timedelta
from utils.utils import Trop2DataReader
import urllib.request
import gzip
import io
import logging
import sys

yyyy1 = int(sys.argv[1])
yyyy2 = int(sys.argv[2])
yyyy3 = int(sys.argv[3])

min_lon = -120
max_lon = -115
min_lat = 31.5
max_lat = 38

# logs the file downloading on both console and in a log file
logger = logging.getLogger('example_logger')
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(f'debug-{yyyy1}-{yyyy2}-{yyyy3}.log')
file_handler.setLevel(logging.DEBUG)

console_formatter = logging.Formatter('%(asctime)s - %(message)s')
file_formatter = logging.Formatter('%(asctime)s - %(message)s')

console_handler.setFormatter(console_formatter)
file_handler.setFormatter(file_formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Following snippet gives unique stations with respective latitude,longitude
def convert_longitude(longitude):
    if longitude > 180:
        return longitude - 360
    else:
        return longitude
station_list_file = '/root/data/rrr/AR/data/stations.txt'
sites_df = pd.read_csv(station_list_file, delim_whitespace=True)
sites_df['Site'] = sites_df['Site'].str.upper()
sites_df['Longitude'] = sites_df['Longitude'].apply(lambda x: convert_longitude(x))
sites_df = sites_df[(sites_df['Latitude'] >= min_lat) & (sites_df['Latitude'] <= max_lat) &
                 (sites_df['Longitude'] >= min_lon) & (sites_df['Longitude'] <= max_lon)]
sites_df = sites_df.reset_index(drop=True)
sites_df = sites_df[['Site', 'Latitude', 'Longitude']].drop_duplicates()
sites_list = list(sites_df.itertuples(index=False, name=None))

# downloads the data
url = 'http://garner.ucsd.edu/pub/measuresESESES_products/Troposphere'
local_filename = 'tmp.gz'
output_dir = '/root/data/rrr/AR/troposphere_data'
for year in [yyyy1, yyyy2, yyyy3]:
    
    # number of days in a year
    days = 365
    if year%4 == 0:
        days = 366
        
    # loop through each day in the year and dowload the troposphere data
    # for each site and save it into a csv file
    for day in [str(i).zfill(3) for i in range(1, days+1)]:
        df_list = []
        previous_date = None
        for (site, lat, lon) in sites_list:
            url_path = f'{url}/{year}/{day}/JPS2_SES_FIN_{year}{day}0000_30H_05M_{site}_TRO.TRO.gz'
            try:
                with urllib.request.urlopen(url_path) as response:
                    compressed_file = response.read()
                logger.debug(url_path)
                with gzip.GzipFile(fileobj=io.BytesIO(compressed_file)) as decompressed_file:
                    trop_reader = Trop2DataReader(decompressed_file)
                    zwd_values, datetime_values = trop_reader.make_lst()
                    num_entries = len(zwd_values)
                    trop_data = {
                        "Timestamp": datetime_values,
                        "Site": [site]*num_entries,
                        "Latitude": [lat]*num_entries,
                        "Longitude": [lon]*num_entries,
                        "ZWD": zwd_values
                    }
                    temp = pd.DataFrame(trop_data)
                    
                    filter_date = datetime(year, 1, 1) + timedelta(days=int(day) - 1)
                    temp = temp[temp["Timestamp"].dt.date == filter_date.date()]
                    df_list.append(temp)
            except Exception as e:
                logger.error(e)
                continue
        if len(df_list)>0:
            zwd_df = pd.concat(df_list)
            if day == '001':
                zwd_df.to_csv(f'{output_dir}/{year}.csv', mode='w', index=False, header=True)
            else:
                zwd_df.to_csv(f'{output_dir}/{year}.csv', mode='a', index=False, header=False)