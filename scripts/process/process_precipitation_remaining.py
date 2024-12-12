import pandas as pd
import os
rem_data_dir = '/root/data/rrr/integrated_weather_dataset/data/processed/Precipitation/rem_data'
main_data_dir = '/root/data/rrr/integrated_weather_dataset/data/processed/Precipitation'
mainn_data_dir = '/root/data/rrr/integrated_weather_dataset/data/processed/Precipitation/final_data'
for year in range(2006, 2024):
    rem_data_file = os.path.join(rem_data_dir, f"{year}.csv")
    main_data_file = os.path.join(main_data_dir, f"{year}.csv")
    mainn_data_file = os.path.join(mainn_data_dir, f"{year}.csv")
    if os.path.exists(rem_data_file) and os.path.exists(main_data_file):
        rem_data = pd.read_csv(rem_data_file)
        main_data = pd.read_csv(main_data_file)
        
        merged_data = pd.concat([main_data, rem_data], axis=0)
        merged_data = merged_data.sort_values(by=['Timestamp','Longitude','Latitude'], ascending=[True, True, True])
        merged_data = merged_data.drop_duplicates(subset=['Timestamp', 'Longitude', 'Latitude'], keep='first')
        merged_data.to_csv(mainn_data_file, index=False)

        print(f"Processed {year}.csv and saved the merged data.")
    else:
        if not os.path.exists(rem_data_file):
            print(f"Missing rem_data file for year {year}. Skipping.")
        if not os.path.exists(main_data_file):
            print(f"Missing main data file for year {year}. Skipping.")
