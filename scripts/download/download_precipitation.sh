#!/bin/bash

input="/root/data/rrr/integrated_weather_dataset/data/raw/Precipitation/subset.txt"
cookies="/root/data/rrr/integrated_weather_dataset/data/raw/Precipitation/.urs_cookies"
output_folder="/root/data/rrr/integrated_weather_dataset/data/raw/Precipitation/data"

while IFS= read -r url; do
    # Extract year, month, day, and start time from the URL
    year=$(echo "$url" | grep -oP '\d{4}(?=\/\d{3}\/3B-HHR)')
    month=$(echo "$url" | grep -oP '(?<=3IMERG\.)\d{6}' | cut -c5-6)
    day=$(echo "$url" | grep -oP '(?<=3IMERG\.)\d{8}' | cut -c7-8)
    start_time=$(echo "$url" | grep -oP 'S\d{6}' | cut -c2-7)

    # Construct the filename in the format YYYY-MM-DD-STARTTIME.nc4
    filename="${year}-${month}-${day}-${start_time}.nc"

    # Construct the full path for the file
    output_file="${output_folder}/${filename}"

    # Download the file with the constructed filename
    wget --load-cookies "$cookies" --save-cookies "$cookies" --keep-session-cookies "$url" -O "$output_file"
done < "$input"