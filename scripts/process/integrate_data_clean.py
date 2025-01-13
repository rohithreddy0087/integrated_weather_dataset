import os
import sys
import math
import numpy as np
import pandas as pd
import dask.dataframe as dd
from datetime import time
import logging
from datetime import datetime

def get_logger(name='integrate', log_file='integrate_debug.log'):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(log_file)
    
    file_handler.setLevel(logging.DEBUG)
    console_handler.setLevel(logging.DEBUG)
    
    console_formatter = logging.Formatter('%(message)s')
    file_formatter = logging.Formatter('%(message)s')

    console_handler.setFormatter(console_formatter)
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger

def process_year(year, logger):
    try:
        logger.info(f"Processing year {year}")
        
        input_path = f"/root/data/rrr/integrated_weather_dataset/data/integrated/parquet/{year}.parquet"
        if not os.path.exists(input_path):
            logger.error(f"File not found for year {year}: {input_path}")
            return None
            
        # Read the parquet file
        df = dd.read_parquet(input_path)
        
        # Convert to pandas for the site processing
        pdf = df.compute()
        last_valid_values = {}
        
        for site in pdf['Site'].unique():
            logger.info(f"Processing site {site} for year {year}")
            site_data = pdf[pdf['Site'] == site]
            
            last_valid_label1 = site_data['Rutz_Label_approx'].dropna()
            if len(last_valid_label1) > 0:
                last_valid_values[(site, 'Rutz_Label_approx')] = float(last_valid_label1.iloc[-1])
            
            last_valid_label2 = site_data['Guan_Label_approx'].dropna()
            if len(last_valid_label2) > 0:
                last_valid_values[(site, 'Guan_Label_approx')] = float(last_valid_label2.iloc[-1])

        for site in pdf['Site'].unique():
            mask = pdf['Site'] == site
            for col in ['Rutz_Label_approx', 'Guan_Label_approx']:
                last_valid = last_valid_values.get((site, col))
                if last_valid is not None:
                    pdf.loc[mask & pdf[col].isna(), col] = last_valid

        columns = pdf.columns.tolist()
        pdf = pdf[columns[1:]]
        
        result_df = dd.from_pandas(pdf, npartitions=df.npartitions)
        
        output_path = f"/root/data/rrr/integrated_weather_dataset/data/integrated/parquet_fixed/{year}.parquet"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        logger.info(result_df.head(10))
        logger.info(f"Number of rows in result_df before saving: {result_df.shape[0].compute()}")
        result_df = result_df.compute()
        result_df.to_parquet(output_path, index=False)

        
        logger.info(f"Successfully processed and saved data for year {year}")
        return result_df
        
    except Exception as e:
        logger.error(f"Error processing year {year}: {str(e)}")
        return None

def main():
    logger = get_logger(log_file='clean_icid.log')
    start_time = datetime.now()
    logger.info(f"Starting data processing at {start_time}")
    
    for year in range(2004, 2024):
        try:
            result = process_year(year, logger)
            if result is not None:
                logger.info(f"Successfully processed year {year}")
            else:
                logger.warning(f"No data processed for year {year}")
        except Exception as e:
            logger.error(f"Failed to process year {year}: {str(e)}")
            continue
    
    end_time = datetime.now()
    processing_time = end_time - start_time
    logger.info(f"Completed data processing at {end_time}")
    logger.info(f"Total processing time: {processing_time}")

if __name__ == "__main__":
    main()