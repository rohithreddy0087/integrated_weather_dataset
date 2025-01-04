
import time
import numpy as np
from torch.utils.data import Dataset, DataLoader
import torch
import dask.dataframe as dd

window_size = 2048

splits = {
    "train": [2005,2006,2007,2008,2010,2011,2012,2013,2015],
    "val": [2004, 2009, 2014],
    "test": [2016, 2017]
}

class WindowedDataset():
    def __init__(self, process_rank, num_process, batch_size,
                       data_dir = "/root/data/rrr/integrated_weather_dataset/data/integrated",
                       split = "train"):
        self.process_rank = process_rank
        self.num_process = num_process
        self.batch_size = batch_size
        self.data_dir = data_dir
        self.years = splits[split]
       
        self.total_dataset_len = self.len_dataset()

        self.curr_year_idx = -1
        self.load_year_data()
        
    def len_dataset(self):
        dataset_files = [f"{self.data_dir}/{yyyy}.csv" for yyyy in self.years]
        data_df = dd.read_csv(dataset_files, blocksize='32MB')  
        return len(data_df)

    def __len__(self):
        return self.total_dataset_len
        
    def read_data(self, yyyy):
        t0 = time.time()
        csv_file = f"{self.data_dir}/{yyyy}.csv",        
        df = dd.read_csv(csv_file, blocksize='32MB')  
        df['Label'] = (df['Rutz_Label_approx'].astype(int) | df['Guan_Label_approx'].astype(int))
        df = df.drop(columns=["Rutz_Label_approx", "Guan_Label_approx"])
        df = df.sort_values(by=['Site', 'Timestamp'])
        df = df.reset_index().reset_index()
        df = df.drop(columns=["index"])
        df = df.rename(columns={'level_0': 'index'})
        df = df.compute()
        print(f"Data loading completed for the year {yyyy}, time taken is {time.time() - t0}")
        return df
    
    def load_year_data(self):
        if self.curr_year_idx == len(self.years) - 1:
            self.curr_year_idx = -1
        self.curr_year_idx += 1
        self.data_df = self.read_data(self.years[self.curr_year_idx])
        self.curr_dataset_len = len(self.data_df)
        self.curr_pos = self.process_rank*self.batch_size
    
    def getitem(self):
        zwd_values = []
        labels = []
        for i in range(self.batch_size):
            start_idx = self.data_df.loc[self.curr_pos+i]["StartIndex"]
            end_idx = self.data_df.loc[self.curr_pos+i]["EndIndex"]
            
            filtered_ddf = self.data_df.loc[start_idx:end_idx]
            if len(filtered_ddf) < window_size:
                return None, None
            
            zwd_values.append(filtered_ddf['ZWD'].values)
            labels.append(self.data_df.loc[self.curr_pos+i]["Label"])
        
        self.curr_pos += self.batch_size*self.num_process
        
        if self.curr_pos+self.batch_size*self.num_process+1 > self.curr_dataset_len - 1 and self.curr_year_idx < len(self.years):
            self.load_year_data()
        return torch.tensor(np.array(zwd_values), dtype=torch.float), torch.tensor(labels, dtype=torch.float)


if __name__ == '__main__':
    
    train_dataset0 = WindowedDataset(process_rank=0, num_process=4, batch_size=64, split = "train")  
    train_dataset1 = WindowedDataset(process_rank=1, num_process=4, batch_size=64, split = "train")  
    train_dataset2 = WindowedDataset(process_rank=2, num_process=4, batch_size=64, split = "train")  
    train_dataset3 = WindowedDataset(process_rank=3, num_process=4, batch_size=64, split = "train")  
    print(f'Number of train graphs: {len(train_dataset0)}')
    
    for i in range(len(train_dataset0)):
        train_dataset0.getitem(i)
        train_dataset1.getitem(i)
        train_dataset2.getitem(i)
        train_dataset3.getitem(i)
        
    
    val_dataset = WindowedDataset(split = "val")
    test_dataset = WindowedDataset(split = "test")

    print(f'Number of train graphs: {len(train_dataset)}')
    print(f'Number of val graphs: {len(val_dataset)}')
    print(f'Number of test graphs: {len(test_dataset)}')

    train_loader = DataLoader(train_dataset, batch_size=48, shuffle=False)
    val_loader = DataLoader(val_dataset, batch_size=48, shuffle=False)

    for _ in range(2):
        for da in train_loader:
            pass
        print("=======================")

# class WindowedDataset(Dataset):
#     def __init__(self, data_df, label_df):
#         self.data_df = data_df
#         self.label_df = label_df

#     def __len__(self):
#         return len(self.label_df)
    
#     def __getitem__(self, idx):
        
#         start_idx = self.label_df.loc[idx]["StartIndex"]
#         end_idx = self.label_df.loc[idx]["EndIndex"]
        
#         filtered_ddf = self.data_df.loc[start_idx:end_idx]
#         if len(filtered_ddf) < window_size:
#             return None, None
        
#         zwd_values = filtered_ddf['ZWD'].values
#         label = self.label_df.loc[idx]["Label"]
#         # if label != int(filtered_ddf.iloc[-1]['Guan_AR_Label'] | filtered_ddf.iloc[-1]['Rutz_AR_Label']):
#         #     print(idx, label, int(filtered_ddf.iloc[-1]['Guan_AR_Label'] | filtered_ddf.iloc[-1]['Rutz_AR_Label']))
#         return torch.tensor(zwd_values, dtype=torch.float), torch.tensor(label, dtype=torch.float)