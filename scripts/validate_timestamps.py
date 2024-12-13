import os
import pandas as pd
from datetime import datetime, timedelta

def timestamp_validate(file_path, start_date, end_date, frequency='30min'):
    start_datetime = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    expected_timestamps = pd.date_range(start=start_datetime, end=end_datetime, freq=frequency)


    import os
import pandas as pd
from datetime import datetime

def timestamp_validate(file_path, start_date, end_date, frequency='30min', output_file="validation_results.txt"):
    start_datetime = datetime.strptime(start_date, '%Y-%m-%d %H%M%S')
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d %H%M%S')
    expected_timestamps = pd.date_range(start=start_datetime, end=end_datetime, freq=frequency)

    with open(output_file, "a") as output:
        output.write(f"For {start_date} to {end_date}")
        try:
            df = pd.read_csv(file_path)
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
            df = df.dropna(subset=['Timestamp'])
            
            missing_timestamps = expected_timestamps.difference(df['Timestamp'])

            nan_rows = df[df.isna().any(axis=1)]

            if missing_timestamps.empty and nan_rows.empty:
                output.write(f"{file_path}: True\n")
            else:
                output.write(f"{file_path}: False\n")
                if not missing_timestamps.empty:
                    output.write(f"Missing timestamps: {list(missing_timestamps)}\n")
                if not nan_rows.empty:
                    output.write("Rows with NaN values:\n")
                    nan_rows.to_string(output, index=False)
                    output.write("\n")

        except Exception as e:
            output.write(f"{file_path}: Error processing file. {str(e)}\n")

if __name__ == "__main__":
    for y in range(2004,2024):
        file_path = "/data/rrr/integrated_weather_dataset/data/processed/Precipitation/{y}.csv"
        start_date = f"{y}-01-01 000000"
        end_date = f"{y}-12-31 233000"
        frequency = "30min"
        timestamp_validate(folder_path, start_date, end_date, frequency)
