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

from torch.distributed import init_process_group, destroy_process_group
from torch.nn.parallel import DistributedDataParallel as DDP
import torch.distributed as dist

from sklearn.metrics import accuracy_score, precision_recall_fscore_support, roc_curve, confusion_matrix, auc
from sklearn.model_selection import train_test_split

from dataset import WindowedDataset
from model import LSTMClassifier

#torchrun --standalone --nproc_per_node=4 train.py

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


os.environ["WANDB_API_KEY"] = "265429f30a001efd9e8ebb1a3b986c59b0ed621b"
wandb.login()

ddp = int(os.environ.get('RANK', -1)) != -1
if ddp:
    assert torch.cuda.is_available(), "for now i think we need CUDA for DDP"
    init_process_group(backend='nccl')
    ddp_rank = int(os.environ['RANK'])
    ddp_local_rank = int(os.environ['LOCAL_RANK'])
    ddp_world_size = int(os.environ['WORLD_SIZE'])
    device = f'cuda:{ddp_local_rank}'
    torch.cuda.set_device(device)
    master_process = ddp_rank == 0 
else:
    ddp_rank = 0
    ddp_local_rank = 0
    ddp_world_size = 1
    master_process = True
    device = "cpu"
    if torch.cuda.is_available():
        device = "cuda"

def plot_confusion_matrix(epoch_cm):
    plt.figure(figsize=(10, 7))
    sns.heatmap(epoch_cm, annot=True, fmt='g', cmap='Blues', xticklabels=['Negative', 'Positive'], yticklabels=['Negative', 'Positive'])
    plt.xlabel('Predicted labels')
    plt.ylabel('True labels')
    plt.title('Confusion Matrix')
    plt.savefig('confusion_matrix.png', bbox_inches='tight')
    plt.close()
    
def calculate_metrics(epoch, loss_accum, tp, fp, fn, tn, mode):
    accuracy = (tp + tn) / (tp + fp + fn + tn)
    precision = tp / (tp + fp) if tp + fp > 0 else 0.0
    recall = tp / (tp + fn) if tp + fn > 0 else 0.0
    f1 = 2 * (precision * recall) / (precision + recall) if precision + recall > 0 else 0.0
    cm = np.array([[tn, fp], [fn, tp]])
    plot_confusion_matrix(cm)
    logger.info(f'{mode} Epoch: {(epoch):05d} |  Loss: {loss_accum:.4f} | Accuracy: {accuracy:.4f} | Precision: {precision:.4f} | Recall: {recall:.4f} | F1 Score: {f1:.4f}')
    metrics = {f"{mode}/loss": loss_accum, 
                f"{mode}/accuracy" : accuracy,
                f"{mode}/precision" : precision,
                f"{mode}/recall" : recall,
                f"{mode}/fscore" : f1,
                f"{mode}/confusion_matrix": wandb.Image('confusion_matrix.png'),
                f"{mode}/epoch": epoch}
    wandb.log(metrics)

warmup_steps = 2000
max_steps = 20000
max_lr = 6e-4
min_lr = 1e-5

def get_lr(iter):
    if iter < warmup_steps:
        return max_lr * (iter+1)/warmup_steps
    if iter > max_steps:
        return min_lr
    
    decay_ratio = (iter - warmup_steps)/ (max_steps - warmup_steps)
    assert 0 <= decay_ratio <=1
    cosine_lr = 0.5 * (1.0 + math.cos(math.pi * decay_ratio))
    return min_lr + cosine_lr*(max_lr - min_lr)
    
def train(epoch, mode = "train"):
    
    model.train()
    train_len = len(train_dataloader)//batch_size
    loss_accum = 0.0
    tp, fp, tn, fn, count = 0, 0, 0, 0, 0
    for step in range(train_len):
        batch_X, batch_y = train_dataloader.getitem()
        iter = epoch*train_len+step
        batch_X = batch_X.to(device)
        batch_y = batch_y.to(device).squeeze()
        optimizer.zero_grad()
        outputs = model(batch_X.unsqueeze(-1))
        loss = criterion(outputs.squeeze(), batch_y)
        loss.backward()
        if ddp:
            dist.all_reduce(loss.detach(), op=dist.ReduceOp.AVG)  
            
        lr = get_lr(iter)
        for param_group in optimizer.param_groups:
            param_group['lr'] = lr
        optimizer.step()
        torch.cuda.synchronize()
        loss_accum += loss.item()
        count += 1
        
        if iter % 100 == 0 and iter>1:
            out = outputs.flatten().detach()
            preds = (out > 0.5).float()

            tp += torch.sum((preds == 1) & (batch_y == 1)).item()
            fp += torch.sum((preds == 1) & (batch_y == 0)).item()
            tn += torch.sum((preds == 0) & (batch_y == 0)).item()
            fn += torch.sum((preds == 0) & (batch_y == 1)).item()
            
            if ddp:
                tp_norm = torch.tensor(tp, dtype=torch.long, device=device)
                fp_norm = torch.tensor(fp, dtype=torch.long, device=device)
                tn_norm = torch.tensor(tn, dtype=torch.long, device=device)
                fn_norm = torch.tensor(fn, dtype=torch.long, device=device)
                loss_norm = torch.tensor(loss_accum, dtype=torch.float, device=device)
                count_norm = torch.tensor(count, dtype=torch.long, device=device)
                
                dist.all_reduce(tp_norm, op=dist.ReduceOp.SUM)
                dist.all_reduce(fp_norm, op=dist.ReduceOp.SUM)
                dist.all_reduce(tn_norm, op=dist.ReduceOp.SUM)
                dist.all_reduce(fn_norm, op=dist.ReduceOp.SUM)
                dist.all_reduce(loss_norm, op=dist.ReduceOp.SUM)
                dist.all_reduce(count_norm.detach(), op=dist.ReduceOp.SUM)
                
                tp_norm, fp_norm, tn_norm, fn_norm, loss_accum_norm, count_norm = tp_norm.item(), fp_norm.item(), tn_norm.item(), fn_norm.item(), loss_norm.item(), count_norm.item()
                
            if master_process:
                calculate_metrics(iter, loss_accum_norm/count_norm, tp_norm, fp_norm, fn_norm, tn_norm, mode="train_batch")
                save_model(iter)
        
        if master_process:
            logger.debug(f'Train step: {(iter):05d} | lr: {lr:.8f} | Loss: {loss.item():.4f} | curr_pos: {train_dataloader.curr_pos}')
            step_metrics = {f"{mode}_batch/loss": loss.item(), 
                    f"{mode}_batch/step" : step,
                    f"{mode}_batch/lr" : lr
                    }
            wandb.log(step_metrics)
        
    if ddp:
        tp_norm = torch.tensor(tp, dtype=torch.long, device=device)
        fp_norm = torch.tensor(fp, dtype=torch.long, device=device)
        tn_norm = torch.tensor(tn, dtype=torch.long, device=device)
        fn_norm = torch.tensor(fn, dtype=torch.long, device=device)
        loss_norm = torch.tensor(loss_accum, dtype=torch.float, device=device)
        count_norm = torch.tensor(count, dtype=torch.long, device=device)
        
        dist.all_reduce(tp_norm, op=dist.ReduceOp.SUM)
        dist.all_reduce(fp_norm, op=dist.ReduceOp.SUM)
        dist.all_reduce(tn_norm, op=dist.ReduceOp.SUM)
        dist.all_reduce(fn_norm, op=dist.ReduceOp.SUM)
        dist.all_reduce(loss_norm, op=dist.ReduceOp.SUM)
        dist.all_reduce(count_norm.detach(), op=dist.ReduceOp.SUM)
        tp, fp, tn, fn, loss_accum, count = tp_norm.item(), fp_norm.item(), tn_norm.item(), fn_norm.item(), loss_norm.item(), count_norm.item()
        
    if master_process:
        calculate_metrics(iter, loss_accum/count, tp, fp, fn, tn, "train")
        save_model(iter)
        
def test(epoch, mode="val"):
    model.eval()
    val_len = len(val_dataloader)//batch_size
    loss_accum = 0.0
    tp, fp, tn, fn, count = 0, 0, 0, 0, 0
    t0 = time.time()
    with torch.no_grad():
        for step in range(val_len):
            batch_X, batch_y = val_dataloader.getitem()
            batch_X = batch_X.to(device)
            batch_y = batch_y.to(device).squeeze()
            outputs = model(batch_X.unsqueeze(-1))
            loss = criterion(outputs.squeeze(), batch_y)
            loss_accum += loss.item()
            
            out = outputs.flatten().detach()
            preds = (out > 0.5).float()

            tp += torch.sum((preds == 1) & (batch_y == 1)).item()
            fp += torch.sum((preds == 1) & (batch_y == 0)).item()
            tn += torch.sum((preds == 0) & (batch_y == 0)).item()
            fn += torch.sum((preds == 0) & (batch_y == 1)).item()
            count += 1
                        
    if ddp:
        tp_norm = torch.tensor(tp, dtype=torch.long, device=device)
        fp_norm = torch.tensor(fp, dtype=torch.long, device=device)
        tn_norm = torch.tensor(tn, dtype=torch.long, device=device)
        fn_norm = torch.tensor(fn, dtype=torch.long, device=device)
        loss_norm = torch.tensor(loss_accum, dtype=torch.float, device=device)
        count_norm = torch.tensor(count, dtype=torch.long, device=device)
        
        dist.all_reduce(tp_norm, op=dist.ReduceOp.SUM)
        dist.all_reduce(fp_norm, op=dist.ReduceOp.SUM)
        dist.all_reduce(tn_norm, op=dist.ReduceOp.SUM)
        dist.all_reduce(fn_norm, op=dist.ReduceOp.SUM)
        dist.all_reduce(loss_norm, op=dist.ReduceOp.SUM)
        dist.all_reduce(count_norm.detach(), op=dist.ReduceOp.SUM)
        tp, fp, tn, fn, loss_accum, count = tp_norm.item(), fp_norm.item(), tn_norm.item(), fn_norm.item(), loss_norm.item(), count_norm.item()
        
    if master_process:
        calculate_metrics(epoch, loss_accum/count, tp, fp, fn, tn, "val")
        save_model(epoch)
        
def save_model(iter):
    output = math.ceil(iter/5000)*5000
    checkpoint_path = os.path.join("/root/data/rrr/integrated_weather_dataset/scripts/lstm_code/checkpoints", f"model_lr_{output}.pth")
    checkpoint = {
        'model': model.state_dict(),
        'optimizer': optimizer.state_dict(),
        'step': epoch
        }
    torch.save(checkpoint, checkpoint_path)
 
if __name__ == '__main__':
    
    epochs = 20
    batch_size = 64
    if master_process:
        wandb.init(
            project="icid", 
            name= "LSTM-DDP", 
            config={
            "learning_rate": 1e-4,
            "architecture": "LSTM",
            "dataset": "Windowed Time Series(2004-2017)",
            "epochs": epochs,
        })
    
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    torch.manual_seed(1337)
    torch.cuda.manual_seed(1337)
    torch.set_float32_matmul_precision('high')
    
    logger = get_logger()
    
    train_dataloader = WindowedDataset( process_rank=ddp_rank, num_process=ddp_world_size, batch_size=batch_size, split = "train") 
    val_dataloader = WindowedDataset(process_rank=ddp_rank, num_process=ddp_world_size, batch_size=batch_size, split = "val") 
    

    logger.info(f'Number of train batches: {len(train_dataloader)//batch_size}')
    logger.info(f'Number of val batches: {len(val_dataloader)//batch_size}')
    
    model = LSTMClassifier()
    model = model.to(device)
    # model = torch.compile(model)

    if ddp:
        model = DDP(model, device_ids=[ddp_local_rank])
    
    raw_model = model.module if ddp else model
    
    # model = torch.load('/root/data/rrr/AR/windowed_gcn/checkpoints/model_00000.pth')
    optimizer = torch.optim.Adam(raw_model.parameters(), lr=max_lr)
    criterion = torch.nn.BCELoss()

    for epoch in range(epochs):
        train(epoch)
        test(epoch)
        
    if ddp:
        destroy_process_group()