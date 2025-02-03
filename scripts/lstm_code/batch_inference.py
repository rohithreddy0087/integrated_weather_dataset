import os
import csv
import time
import wandb
import math
import numpy as np
import dask.dataframe as dd

import torch
from torch.utils.data import TensorDataset, DataLoader
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.metrics import accuracy_score, precision_recall_fscore_support, roc_curve, confusion_matrix, auc
from sklearn.model_selection import train_test_split

from dataset import WindowedDataset
from model import LSTMClassifier

import logging
logger = logging.getLogger('example_logger')
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(f'batch_inf-2017-fullyear.log')
file_handler.setLevel(logging.DEBUG)

console_formatter = logging.Formatter('%(asctime)s - %(message)s')
file_formatter = logging.Formatter('%(asctime)s - %(message)s')

console_handler.setFormatter(console_formatter)
file_handler.setFormatter(file_formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

#data_dir = "/root/data/rrr/integrated_weather_dataset/data/integrated/parquet_fixed"
data_dir = "/root/data/rrr/ES3-TACLS/AR/dataset/parquet"
                       
def read_data(yyyy):
    t0 = time.time()
    parquet_file = f"{data_dir}/{yyyy}.parquet",
    
    df = dd.read_parquet(parquet_file, blocksize='32MB')  
    df['Label'] = (df['Guan_AR_Label'].astype(int) | df['Rutz_AR_Label'].astype(int))
    df = df.drop(columns=["Guan_AR_Label", "Rutz_AR_Label"])
    df = df.sort_values(by=['Site', 'Timestamp'])
    df = df.reset_index()
    df = df.drop(columns=["index"])
    df = df.rename(columns={'level_0': 'index'})
    df['Timestamp'] = df['Timestamp'].round('5T')
    df = df.compute()
    
    logger.debug(f"Data loading completed for the year {yyyy}, time taken is {time.time() - t0}")
    return df

def process_windows_stream(group, window_size=2048, batch_size=32):
    """Generates windows in batches from the given group."""
    X_batch = []
    y_batch = []
    metadata_batch = []
    
    for i in range(0, len(group) - window_size + 1):
        window = group.iloc[i:i+window_size]
        
        if len(window) == window_size:
            X_batch.append(np.array(window['ZWD'].values))
            y_batch.append(window['Label'].values[-1])
            
            metadata_batch.append({
                'Timestamp': window.iloc[-1]['Timestamp'],
                'Site': window.iloc[-1]['Site'],
                'Latitude': window.iloc[-1]['Latitude'],
                'Longitude': window.iloc[-1]['Longitude']
            })
            
            if len(X_batch) == batch_size:
                yield np.array(X_batch), np.array(y_batch), metadata_batch
                X_batch, y_batch, metadata_batch = [], [], []
    
    if X_batch:
        yield np.array(X_batch), np.array(y_batch), metadata_batch

def count_windows_in_group(group, window_size=2048):
    return max(0, len(group) - window_size + 1)

if __name__ == '__main__':
    
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    torch.manual_seed(1337)
    torch.cuda.manual_seed(1337)
    torch.set_float32_matmul_precision('high')
    
    criterion = torch.nn.BCELoss()
    
    yyyy = 2017
    logger.debug(f"Inference started for the year {yyyy}")
    data_df = read_data(yyyy)
    
    start_date = '2017-01-01'
    end_date = '2017-12-31'
    filtered_df = data_df[(data_df['Timestamp'] >= start_date) & (data_df['Timestamp'] <= end_date)]

    logger.debug(f"Filtered windows")
    total_windows = sum(count_windows_in_group(group, window_size=2048) for _, group in filtered_df.groupby('Site'))
    
    batch_size = 2048
    total_batches = (total_windows + batch_size - 1) // batch_size
    logger.debug(f"Total number of batches: {total_batches}")

    model = LSTMClassifier()
    model = model.to(device)
    checkpoint_path = os.path.join("/root/data/rrr/ES3-TACLS/AR/windowed_lstm/checkpoints/model_lr_100000.pth")
    model = torch.nn.DataParallel(model)
    model.load_state_dict(torch.load(checkpoint_path)['model'])
    logger.debug(f"Loaded model weights")
    
    csvfile = open('inf_results_2017_2.csv', 'w', newline='') 
    writer = csv.writer(csvfile)
    writer.writerow(['Timestamp', 'Site', 'Latitude', 'Longitude', 'ZWD', 'Label', 'Prediction'])
 
    model.eval()
    t0 = time.time()
    
    with torch.no_grad():
        
        for site, group in filtered_df.groupby('Site'):
            windows_grp_cnt = count_windows_in_group(group, window_size=2048)
            grp_batches = (windows_grp_cnt + batch_size - 1) // batch_size
            i=1
            for X_batch, y_batch, metadata_batch in process_windows_stream(group, window_size=2048, batch_size=batch_size):
                logger.info(f"Processing batch {i}/{grp_batches} of size {len(X_batch)} for site {site}")
                X = torch.tensor(X_batch, dtype=torch.float32)[..., None].to(device)
                y = torch.tensor(y_batch, dtype=torch.float32).to(device)
                outputs = model(X)
                
                for b in range(len(X_batch)):
                    writer.writerow([
                        str(metadata_batch[b]['Timestamp']),
                        metadata_batch[b]['Site'],
                        metadata_batch[b]['Latitude'],
                        metadata_batch[b]['Longitude'],
                        X_batch[b][-1],
                        y_batch[b],
                        outputs[b].item()
                    ])
                i+=1

        # for i in range(0, len(all_windows), batch_size):
        #     logger.info(f"Processing window {i+1}/{len(all_windows)}")
        #     X = []
        #     y = []
        #     for b in range(batch_size):
        #         X.append(np.array(all_windows[i*batch_size + b]['ZWD'].values))
        #         y.append(all_windows[i*batch_size + b]['Label'].values[-1])
            
        #     X = np.array(X)
        #     y = np.array(y)
        #     X = torch.tensor(X, dtype=torch.float32)[...,None].unsqueeze(0).to(device)
        #     y = torch.tensor(y, dtype=torch.float32).to(device)
            
        #     outputs = model(X)
        #     # with open('inf_results.csv', 'a', newline='') as csvfile:
        #     # writer = csv.writer(csvfile)
        #     # for i in range(batch_size):
            
