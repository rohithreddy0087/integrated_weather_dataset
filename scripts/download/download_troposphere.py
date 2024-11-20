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
from config.config import get_logger, read_sites

start_year = int(sys.argv[1])
end_year = int(sys.argv[2])

logger = get_logger()
_, sites_list = read_sites()

# downloads the data
url = 'http://garner.ucsd.edu/pub/measuresESESES_products/Troposphere'
local_filename = 'tmp.gz'
output_dir = '../data/processed/Troposphere'

for year in range(start_year, end_year):
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