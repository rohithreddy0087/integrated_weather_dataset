import pandas as pd
import os

def validate_data(folder_path, start_year, end_year, frequency):
    report = []
    for year in range(start_year, end_year + 1):
        file_name = os.path.join(folder_path, f"{year}.csv")
        if not os.path.exists(file_name):
            print(f"File {year}.csv not found. Skipping...")
            continue
        try:
            data = pd.read_csv(file_name, parse_dates=['Timestamp'])
        except Exception as error:
            print(f"Error reading {file_name}: {error}")
            continue
        rows_with_na = data[data.isnull().any(axis=1)]
        total_rows_with_na = len(rows_with_na)
        try:
            start_time = pd.Timestamp(f"{year}-01-01 00:00:00")
            end_time = pd.Timestamp(f"{year+1}-01-01 00:00:00")
            expected_timestamps = pd.date_range(start=start_time, end=end_time, freq=frequency)
            actual_timestamps = pd.to_datetime(data['Timestamp'])
            missing_timestamps = expected_timestamps.difference(actual_timestamps)
        except KeyError:
            print(f"'timestamp' column missing in {file_name}. Skipping...")
            continue
        report.append({
            'Year': year,
            'Expected Timestamps': len(expected_timestamps),
            'Available Timestamps': len(actual_timestamps),
            'Missing Timestamps': len(missing_timestamps),
            'Rows with Missing Data': total_rows_with_na
        })
        print(f"Year {year}: Missing Timestamps = {len(missing_timestamps)}, Rows with Missing Data = {total_rows_with_na}")
    report_df = pd.DataFrame(report)
    output_file = "data_validation_summary.txt"
    report_df.to_csv(output_file, index=False, sep='\t')
    print("\nValidation Completed. Report:")
    print(report_df)
    print(f"\nReport saved to {output_file}")

folder_path = "/root/data/rrr/integrated_weather_dataset/data/processed/Precipitation"
start_year = 2005
end_year = 2024
frequency = "30min"
validate_data(folder_path, start_year, end_year, frequency)

