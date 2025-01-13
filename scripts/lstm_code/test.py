import os
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
from utils import get_logger

def plot_confusion_matrix_and_roc(epoch_cm, fpr, tpr, roc_auc):
    plt.figure(figsize=(10, 7))
    sns.heatmap(epoch_cm, annot=True, fmt='g', cmap='Blues', xticklabels=['Negative', 'Positive'], yticklabels=['Negative', 'Positive'])
    plt.xlabel('Predicted labels')
    plt.ylabel('True labels')
    plt.title('Confusion Matrix')
    plt.savefig('test_confusion_matrix.png', bbox_inches='tight')
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
    plt.savefig('test_roc_curve.png', bbox_inches='tight')
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

def test(loader, mode="test"):
    model.eval()
    all_preds = []
    all_labels = []
    loss_accum = 0.0
    t0 = time.time()
    with torch.no_grad():
        for i, (batch_X, batch_y) in enumerate(loader):
            batch_X = batch_X.to(device)
            batch_y = batch_y.to(device).squeeze()
            outputs = model(batch_X.unsqueeze(-1))
            loss = criterion(outputs.squeeze(), batch_y)
            loss_accum += loss.item()
            
            out = outputs.flatten().detach()
            pred = (out > 0.5).float()
            all_preds.extend(pred.cpu().numpy())
            all_labels.extend(batch_y.cpu().numpy())
            
    time_taken_epoch = (time.time()-t0)*1000
    calculate_metrics(loss_accum/len(loader), all_labels, all_preds, time_taken_epoch, mode)

 
if __name__ == '__main__':
    
    
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    torch.manual_seed(1337)
    torch.cuda.manual_seed(1337)
    torch.set_float32_matmul_precision('high')
    
    logger = get_logger()
    criterion = torch.nn.BCELoss()
    
    yyyy = 2006
    
    df = dd.read_parquet(f'/root/data/rrr/AR/dataset/parquet/{yyyy}_n.parquet')
    df['Label'] = (df['Guan_AR_Label'] | df['Rutz_AR_Label'].astype(int))
    df = df.drop(columns=["Guan_AR_Label", "Rutz_AR_Label"])
    df = df.sort_values(by=['Site', 'Timestamp'])
    df = df.reset_index().reset_index()
    df = df.drop(columns=["index"])
    df = df.rename(columns={'level_0': 'index'})
    df = df.compute()
        
    test_df = dd.read_parquet(f'/root/data/rrr/AR/windowed_lstm/{yyyy}.parquet')
    test_df = test_df.compute()
    test_dataset = WindowedDataset(df, test_df)

    logger.info(f'Number of test windows: {len(test_dataset)}')

    test_loader = DataLoader(test_dataset, batch_size=64, shuffle=False)

    logger.info(f'Number of test batches: {len(test_loader)}')
    
    model = LSTMClassifier()
    model = model.to(device)
    
    checkpoint_path = os.path.join("/root/data/rrr/AR/windowed_lstm/checkpoints", "model_lr_20.pth")

    model.load_state_dict(torch.load(checkpoint_path)['model'])
 
    test(test_loader)

# 2024-08-16 07:20:55,428 [INFO ] Number of test windows: 13897
# 2024-08-16 07:20:55,903 [INFO ] Number of test batches: 218
# 2024-08-16 07:21:08,560 [INFO ] test |  Loss: 0.5759 | Accuracy: 0.7125 | Precision: 0.7118 | Recall: 0.7041 | F1 Score: 0.7079 | Timetaken 9573.0009ms

# 2024-08-16 07:45:25,688 [INFO ] Number of test windows: 27758
# 2024-08-16 07:45:26,264 [INFO ] Number of test batches: 434
# 2024-08-16 07:45:49,927 [INFO ] test |  Loss: 0.5853 | Accuracy: 0.7045 | Precision: 0.7083 | Recall: 0.6949 | F1 Score: 0.7015 | Timetaken 19319.9887ms

# 2024-08-16 17:16:45,779 [INFO ] Number of test windows: 27758
# 2024-08-16 17:16:45,843 [INFO ] Number of test batches: 434
# 2024-08-16 17:17:05,604 [INFO ] test |  Loss: 0.5865 | Accuracy: 0.7035 | Precision: 0.7026 | Recall: 0.7055 | F1 Score: 0.7040 | Timetaken 17684.8996ms