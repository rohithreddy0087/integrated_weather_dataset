import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import os
import dask.dataframe as dd

import logging

logger = logging.getLogger('example_logger')
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(f'debug-gen-windows.log')
file_handler.setLevel(logging.DEBUG)

console_formatter = logging.Formatter('%(asctime)s - %(message)s')
file_formatter = logging.Formatter('%(asctime)s - %(message)s')

console_handler.setFormatter(console_formatter)
file_handler.setFormatter(file_formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

def create_window_indices(ddf, yyyy):
    if yyyy in [2004, 2008, 2012, 2016]:
        num_timestamps = 366 * 24 * 12
    else:
        num_timestamps = 365 * 24 * 12

    def process_group(group):
        if len(group) < window_size:
            return pd.DataFrame(columns=['StartIndex', 'EndIndex', 'Label'])

        logger.debug(group['Site'].iloc[0])
        group = group.reset_index()
        indices = []
        labels = []
        for i in range(0, num_timestamps - window_size, window_stride):
            start_idx = i
            end_idx = i + window_size - 1
            filtered_ddf = group.loc[start_idx:end_idx]
            if len(filtered_ddf) < window_size:
                continue
            label = int(filtered_ddf.loc[end_idx]['Label'])
            indices.append((filtered_ddf.loc[start_idx]['index'], filtered_ddf.loc[end_idx]['index']))
            labels.append(label)

        return pd.DataFrame({'StartIndex': [i[0] for i in indices],
                             'EndIndex': [i[1] for i in indices],
                             'Label': labels})

    result = ddf.groupby('Site').apply(process_group, meta={
        'StartIndex': 'int64',
        'EndIndex': 'int64',
        'Label': 'int64'
    })

    return result


window_size = 2048
window_stride = 32

for yyyy in range(2004, 2024):
    logger.debug(yyyy)
    df = dd.read_parquet(f'/root/data/rrr/integrated_weather_dataset/data/integrated/parquet_fixed/{yyyy}.parquet')
    df = df.dropna(subset=['Guan_Label_approx'])
    df['Label'] = (df['Guan_Label_approx'].astype(int) | df['Rutz_Label_approx'].astype(int))
    df = df.drop(columns=["Guan_Label_approx", "Rutz_Label_approx"])

    df = df.sort_values(by=['Site', 'Timestamp'])
    df = df.reset_index()
    
    df = df.drop(columns=["index"])
    df = df.rename(columns={'level_0': 'index'})
    res = create_window_indices(df, yyyy)
    res_df = res.compute()
    gf = res_df.reset_index()
    gf = gf.drop(columns=["level_1"])
    print("Computed")

    positive_samples = gf[gf['Label'] == 1]
    negative_samples = gf[gf['Label'] == 0]

    minority_count = min(len(positive_samples), len(negative_samples))
    negative_samples_undersampled = negative_samples.sample(minority_count, random_state=42)
    balanced_df = pd.concat([negative_samples_undersampled, positive_samples])
    balanced_df = balanced_df.sample(frac=1, random_state=42).reset_index(drop=True)
    # if yyyy==2005:
    #     balanced_df = balanced_df.sample(frac=1, random_state=42).reset_index(drop=True)
    # else:
    #     balanced_df = balanced_df.sample(frac=0.1, random_state=42).reset_index(drop=True)

    balanced_df.to_parquet(f'/root/data/rrr/integrated_weather_dataset/data/integrated/labels/{yyyy}.parquet')