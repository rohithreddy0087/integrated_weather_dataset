import pandas as pd

out = '/root/data/rrr/integrated_weather_dataset/scripts/validation/station_analysis.txt'

print("Starting analysis of weather station datasets from 2004 to 2024...")

with open(out, 'w') as f:
    f.write("Starting analysis of weather station datasets from 2004 to 2024...\n")
    
    for year in range(2004, 2025):
        f.write(f"\nProcessing data for year: {year} - {year + 1}\n")
        file_path_1 = f'/root/data/rrr/integrated_weather_dataset/data/processed/Troposphere/{year}.csv'
        file_path_2 = f'/root/data/rrr/integrated_weather_dataset/data/processed/Troposphere/{year + 1}.csv'

        df1 = pd.read_csv(file_path_1)
        df2 = pd.read_csv(file_path_2)        
        list1 = set(df1['Site'])
        list2 = set(df2['Site'])
        
        common_stations = list1.intersection(list2)
        unique1 = list2 - list1
        unique2 = list1 - list2
        
        len_common = len(common_stations)
        len_1 = len(unique1)
        len_2 = len(unique2)
        
        f.write(f"Common stations: {common_stations}\n")
        f.write(f"Number of common stations: {len_common}\n")
        f.write(f"Number of sites added from {year} to {year + 1}: {len_1}\n")
        f.write(f"Sites added from {year} to {year + 1}: {unique1}\n")
        f.write(f"Number of sites excluded from {year} to {year + 1}: {len_2}\n")
        f.write(f"Sites excluded from {year} to {year + 1}: {unique2}\n")
        f.write("-" * 40 + "\n")

        print(f"Done {year} to {year+1})")
    f.write("Completed analysis of all years from 2004 to 2024.\n")

print("Completed analysis and saved results to weather_analysis_summary.txt.")
