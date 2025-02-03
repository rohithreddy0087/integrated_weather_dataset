import os, sys
import math
import numpy as np
import pandas as pd
import dask.dataframe as dd
from sklearn.metrics import confusion_matrix
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats
from sklearn.metrics import confusion_matrix, cohen_kappa_score, roc_curve, auc
from datetime import timedelta

df1 = dd.read_parquet('/root/data/rrr/integrated_weather_dataset/data/integrated/parquet_fixed/2006.parquet')
pdf1 = df1.compute()
pdf1_cleaned = pdf1.dropna(subset=['Precipitation_Label_approx'])
labels = list(zip(
    pdf1_cleaned['Guan_Label_approx'], 
    pdf1_cleaned['Rutz_Label_approx'], 
    pdf1_cleaned['Precipitation_Label_approx'], 
    pdf1_cleaned['ffw']
))
print(labels.size)
def create_window_labels(labels, window_size=2048, stride=32):
    n_samples = len(labels)
    method_names = ['Guan', 'Rutz', 'Precipitation', 'FFW']
    window_labels = []
    
    labels_array = np.array(labels)
    
    for start in range(0, n_samples, stride):
        window = labels_array[start:min(start + window_size, n_samples)]
        if len(window) < window_size / 2: 
            continue
            
        window_verdict = []
        for method_idx in range(4):
            method_data = window[:, method_idx]
            ones_count = np.sum(method_data == 1)
            zeros_count = len(method_data) - ones_count
            window_verdict.append(1 if ones_count > zeros_count else 0)
            
        window_labels.append(window_verdict)
    
    return np.array(window_labels), method_names

def create_event_labels(window_labels):
    events = []
    method_names = ['Guan', 'Rutz', 'Precipitation', 'FFW']
    
    for method_idx in range(4):
        method_windows = window_labels[:, method_idx]
        current_event = False
        event_count = 0
        
        for label in method_windows:
            if label == 1:
                if not current_event:
                    current_event = True
                    event_count += 1
            else:
                current_event = False
                
        events.append(event_count)
        
    return np.array(events), method_names

def create_confusion_matrices(labels, level_type="window"):
    method_names = ['Guan', 'Rutz', 'Precipitation', 'FFW']
    n_methods = len(method_names)
    confusion_matrix = np.zeros((n_methods, n_methods))
    
    labels = np.array(labels)  # Ensure labels is a 2D numpy array
    
    for i in range(n_methods):
        for j in range(n_methods):
            if level_type == "window":
                matches = np.sum(labels[:, i] == labels[:, j])
                confusion_matrix[i, j] = matches / len(labels)
            else:  # event level
                if max(labels[i], labels[j]) == 0:
                    confusion_matrix[i, j] = 1  # Perfect agreement if both have 0 events
                else:
                    confusion_matrix[i, j] = 1 - abs(labels[i] - labels[j]) / max(labels[i], labels[j])
                
    return confusion_matrix, method_names

def plot_heatmaps(labels):
    window_labels, method_names = create_window_labels(labels)
    window_confusion, _ = create_confusion_matrices(window_labels, "window")
    
    event_labels, _ = create_event_labels(window_labels)
    event_confusion, _ = create_confusion_matrices(event_labels, "event")
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    sns.heatmap(window_confusion, 
                annot=True, 
                fmt='.2f', 
                cmap='YlGnBu',
                xticklabels=method_names,
                yticklabels=method_names,
                ax=ax1)
    ax1.set_title('Window-Level Agreement')
    
    sns.heatmap(event_confusion,
                annot=True,
                fmt='.2f',
                cmap='YlGnBu',
                xticklabels=method_names,
                yticklabels=method_names,
                ax=ax2)
    ax2.set_title('Event-Level Agreement')
    
    plt.tight_layout()
    return fig

fig = plot_heatmaps(labels)
fig.savefig('agreement_heatmap.png')
