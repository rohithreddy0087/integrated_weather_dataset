import numpy as np

file_year = 2000
ar_data = ds['ARs']

csv_file_path = f'/root/data/rrr/AR/rutz_csv/AR_Catalog_{file_year}.csv'  

with open(csv_file_path, 'w') as csv_file:
    csv_file.write("Timestamp,Latitude,Longitude,AR value\n")
    
    for time_index in range(ar_data.sizes['ntim']):
        ar_values_subset = ar_data.isel(ntim=time_index).values
        year = int(ds['cal_year'].values[time_index])
        month = int(ds['cal_mon'].values[time_index])
        day = int(ds['cal_day'].values[time_index])
        hour = int(ds['cal_hour'].values[time_index])

        time_str = f"{year}-{month:02d}-{day:02d} {hour:02d}:00:00"
        
        ar_indices = np.argwhere(ar_values_subset == 1)
        
        for idx in ar_indices:  
            lat_index, lon_index = idx[0], idx[1]  
            lat_value = ds['latitude'].values[lat_index]
            lon_value = ds['longitude'].values[lon_index]      
            ar_value = int(ar_values_subset[lat_index, lon_index])
            
            is_lat_in_range = min_latitude <= lat_value <= max_latitude
            is_lon_in_range = min_longitude <= lon_value <= max_longitude

            if is_lat_in_range and is_lon_in_range:
                df['Distance'] = df.apply(lambda row: calculate_distance(lat_value, lon_value, row['Latitude'], row['Longitude']), axis=1)
                closest_station = df.loc[df['Distance'].idxmin()]
                if closest_station['Distance'] > 50:
                    continue
                print(lat_value, lon_value)
                print(closest_station)
                dww
                csv_file.write(f"{time_str},{lat_value},{lon_value},{closest_station["Site"]},{ar_value}\n")

print(f"CSV file has been created at: {csv_file_path}")

ds.close()