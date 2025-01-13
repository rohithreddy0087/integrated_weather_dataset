import os
import csv
import time
import wandb
import math
import numpy as np
import dask.dataframe as dd

import torch
from torch_geometric.loader import DataLoader
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.metrics import accuracy_score, precision_recall_fscore_support, roc_curve, confusion_matrix, auc
from sklearn.model_selection import train_test_split

from dataset import WindowedDataset
from model import LSTMClassifier
import logging
import traceback
def get_logger(name = 'integrate', log_file = 'integrate_debug.log'):
    """helper function to get a logger object

    Args:
        name (str, optional): Logger name. Defaults to 'dataset'.
        log_file (str, optional): Debug file name. Defaults to 'debug.log'.

    Returns:
        logger object: returns a logger object
    """
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

def log_training_config(logger, config):
    logger.info("=== Training Configuration ===")
    for key, value in config.items():
        logger.info(f"{key}: {value}")
    logger.info("============================")

def log_model_summary(logger, model):
    logger.info("=== Model Architecture ===")
    logger.info(str(model))
    total_params = sum(p.numel() for p in model.parameters())
    trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    logger.info(f"Total parameters: {total_params:,}")
    logger.info(f"Trainable parameters: {trainable_params:,}")
    logger.info("========================")

def log_error_with_trace(logger, error):
    logger.error(f"Error occurred: {str(error)}")
    logger.error("Traceback:")
    logger.error(traceback.format_exc())


data_dir = "/root/data/rrr/integrated_weather_dataset/data/integrated/parquet_fixed/"
                       
def read_data(yyyy):
    t0 = time.time()
    parquet_file = f"{data_dir}/{yyyy}.parquet",
    
    df = dd.read_parquet(parquet_file, blocksize='32MB')  
    df['Timestamp'] = 
    df['Label'] = (df['Rutz_Label_approx'].astype(int) | df['Guan_Label_approx'].astype(int))
    df = df.drop(columns=["Rutz_Label_approx", "Guan_Label_approx"])
    df = df.sort_values(by=['Site', 'Timestamp'])
    df = df.reset_index().reset_index()
    df = df.drop(columns=["index"])
    df = df.rename(columns={'level_0': 'index'})
    df['Timestamp'] = df['Timestamp'].round('5T')
    df = df.compute()
    
    print(f"Data loading completed for the year {yyyy}, time taken is {time.time() - t0}")
    return df

def plot_confusion_matrix_and_roc(epoch_cm, fpr, tpr, roc_auc):
    plt.figure(figsize=(10, 7))
    sns.heatmap(epoch_cm, annot=True, fmt='g', cmap='Blues', xticklabels=['Negative', 'Positive'], yticklabels=['Negative', 'Positive'])
    plt.xlabel('Predicted labels')
    plt.ylabel('True labels')
    plt.title('Confusion Matrix')
    plt.savefig('inf_confusion_matrix.png', bbox_inches='tight')
    plt.close()

    plt.figure()
    plt.plot(fpr, tpr, color='darkorange', lw=2, label='ROC curve (area = %0.2f)' % roc_auc)
    plt.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver Operating Characteristic')
    plt.legend(loc="lower right")
    plt.savefig('inf_roc_curve.png', bbox_inches='tight')
    plt.close()
    
def calculate_metrics(loss_accum, all_labels, all_preds, time_taken_epoch, mode):
    accuracy = accuracy_score(all_labels, all_preds)
    precision, recall, f1, _ = precision_recall_fscore_support(all_labels, all_preds, average='binary')
    fpr, tpr, thresholds = roc_curve(all_labels, all_preds)
    auc_ = auc(fpr, tpr)
    cm = confusion_matrix(all_labels, all_preds, labels=[0,1])
    plot_confusion_matrix_and_roc(cm, fpr, tpr, auc_)
    cm = confusion_matrix(all_labels, all_preds)  
    logger.info(f'{mode} |  Loss: {loss_accum:.4f} | Accuracy: {accuracy:.4f} | Precision: {precision:.4f} | Recall: {recall:.4f} | F1 Score: {f1:.4f} | Timetaken {time_taken_epoch:.4f}ms')
    
 
def process_windows(group, window_size=2048):
    windows = []
    for i in range(0, len(group) - window_size + 1):
        window = group.iloc[i:i+window_size]
        if len(window) == window_size:
            windows.append(window)
    return windows

    
logger = get_logger()
device = 'cuda' if torch.cuda.is_available() else 'cpu'
torch.manual_seed(1337)
torch.cuda.manual_seed(1337)
torch.set_float32_matmul_precision('high')

logger = get_logger()
criterion = torch.nn.BCELoss()

yyyy = 2017
data_df = read_data(yyyy)

start_date = '2017-01-01'
end_date = '2017-01-31'
filtered_df = data_df[(data_df['Timestamp'] >= start_date) & (data_df['Timestamp'] <= end_date)]

all_windows = filtered_df.groupby('Site').apply(process_windows)
all_windows = [window for site_windows in all_windows for window in site_windows]
print("Created all windows", len(all_windows))

model = LSTMClassifier()
model = model.to(device)

checkpoint_path = os.path.join("/root/data/rrr/integrated_weather_dataset/scripts/lstm_code/checkpoints/best_model.pth")
model = torch.nn.DataParallel(model)
model.load_state_dict(torch.load(checkpoint_path)['model'])

csvfile = open('inf_results.csv', 'w', newline='') 
writer = csv.writer(csvfile)
writer.writerow(['Timestamp', 'Site', 'Latitude', 'Longitude', 'ZWD', 'Label', 'Prediction'])

model.eval()
all_preds = []
all_labels = []
loss_accum = 0.0
t0 = time.time()

def process_item(window):
    with torch.no_grad():
        # logger.info(f"Processing window {i+1}/{len(all_windows)}")
        X = np.array(window['ZWD'].values)
        y = window['Label'].values[-1]
        X = torch.tensor(X, dtype=torch.float32)[...,None].unsqueeze(0).to(device)
        y = torch.tensor(y, dtype=torch.float32).to(device)
        
        outputs = model(X)
        # with open('inf_results.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([str(window.iloc[-1]['Timestamp']), window.iloc[-1]['Site'], window.iloc[-1]['Latitude'], window.iloc[-1]['Longitude'], window.iloc[-1]['ZWD'], window.iloc[-1]['Label'], outputs.item()])
        

            
            # loss = criterion(outputs.squeeze(), y)
            
            # loss_accum += loss.item()
            
            # out = outputs.flatten().detach()
            # pred = (out > 0.5).float()
            # all_preds.extend(pred.cpu().numpy())
            # all_labels.append(y.cpu().numpy())
            
    # time_taken_epoch = (time.time()-t0)*1000
    # calculate_metrics(loss_accum/len(all_windows), all_labels, all_preds, time_taken_epoch, mode)

from tqdm import trange, tqdm
import concurrent.futures
import time



def parallel_process_with_process_pool(items, num_workers=50):
    results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        future_to_item = {executor.submit(process_item, item): item for item in items}
        for future in tqdm(concurrent.futures.as_completed(future_to_item), total=len(future_to_item), desc="Processing items"):
        # for future in concurrent.futures.as_completed(future_to_item):
            item = future_to_item[future]
            try:
                result = future.result()
                if result is not None:
                    results.append(result)
            except Exception as e:
                # pass
                print(f'Item {item} generated an exception: {e}')
    return results


answers = parallel_process_with_process_pool(all_windows, num_workers=10)