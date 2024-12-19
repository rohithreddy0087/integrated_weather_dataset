#!/bin/bash

input_file="/root/data/rrr/integrated_weather_dataset/data/raw/Precipitation/left_timestamps.txt"

# Read the input file line by line
while IFS= read -r filename; do
    # Extract year, month, day, and start time from the filename
    year=$(echo "$filename" | cut -d'-' -f1)
    month=$(echo "$filename" | cut -d'-' -f2)
    day=$(echo "$filename" | cut -d'-' -f3 | cut -d'-' -f1)
    start_time=$(echo "$filename" | grep -oP '\d{6}(?=\.nc)')

    # Convert the date to the day of the year (DOY)
    doy=$(date -d "${year}-${month}-${day}" +%j)

    # Construct the URL
    url="https://gpm1.gesdisc.eosdis.nasa.gov/opendap/GPM_L3/GPM_3IMERGHH.07/${year}/${doy}/3B-HHR.MS.MRG.3IMERG.${year}${month}${day}-S${start_time}-E122959.0720.V07B.HDF5.nc4"

    echo "$url"
done < "$input_file"
