import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import matplotlib.dates as mdates
import os
import math

results_file = "/root/data/rrr/integrated_weather_dataset/inf_results_2017_2.csv"
results_df = pd.read_csv(results_file)
results_df['Timestamp'] = pd.to_datetime(results_df['Timestamp'], format='mixed')

results_df['Predictions'] = results_df['Prediction'].apply(lambda x: 1 if x >= 0.72 else 0)

def identify_unique_events(df):
    df = df.sort_values(['Site', 'Timestamp'])
    
    try:
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='mixed')
    except ValueError:
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%Y-%m-%d %H:%M:%S')
    
    df['EventID_Label'] = 0
    df['EventID_Prediction'] = 0
    
    for site, group in df.groupby('Site'):
        event_id_label = 0
        event_id_pred = 0
        last_label_1_time = None
        last_label_2_time = None
        
        for i, row in group.iterrows():
            if row['Label'] == 1:
                if last_label_1_time is None or (row['Timestamp'] - last_label_1_time).total_seconds() > 3 * 3600:
                    event_id_label += 1
                
                df.at[i, 'EventID_Label'] = event_id_label
                last_label_1_time = row['Timestamp']
            elif last_label_1_time is not None and (row['Timestamp'] - last_label_1_time).total_seconds() <= 3 * 3600:
                df.at[i, 'EventID_Label'] = event_id_label
            else:
                last_label_1_time = None
            
            if row['Prediction'] > 0.71:
                if last_label_2_time is None or (row['Timestamp'] - last_label_2_time).total_seconds() > 3 * 3600:
                    event_id_pred += 1
                
                df.at[i, 'EventID_Prediction'] = event_id_pred
                last_label_2_time = row['Timestamp']
            elif last_label_2_time is not None and (row['Timestamp'] - last_label_2_time).total_seconds() <= 3 * 3600:
                df.at[i, 'EventID_Prediction'] = event_id_pred
            else:
                last_label_2_time = None
    
    return df

def create_site_plots(results_df, output_folder, sites_per_plot=10):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    sites = results_df['Site'].unique()
    num_plots = math.ceil(len(sites) / sites_per_plot)
    
    date_range = pd.date_range(start='2017-01-01',
                              end='2017-07-01',
                              freq='D') 
    
    for plot_num in range(num_plots):
        start_idx = plot_num * sites_per_plot
        end_idx = min((plot_num + 1) * sites_per_plot, len(sites))
        current_sites = sites[start_idx:end_idx]
        
        fig, ax = plt.subplots(figsize=(20, 12))
        
        ax.grid(True, which='major', alpha=0.3)
        ax.set_axisbelow(True)
        
        for i, site in enumerate(current_sites):
            site_data = results_df[results_df['Site'] == site]
            
            label_events = ax.scatter(site_data['Timestamp'][site_data['EventID_Label'] >= 1], 
                                    [i] * len(site_data[site_data['EventID_Label'] >= 1]), 
                                    color='red', s=100, marker='o', 
                                    label='Actual Event' if i == 0 else "",
                                    alpha=0.7)
            
            pred_events = ax.scatter(site_data['Timestamp'][site_data['EventID_Prediction'] >= 1], 
                                   [i-0.2] * len(site_data[site_data['EventID_Prediction'] >= 1]), 
                                   color='blue', s=80, marker='^', 
                                   label='Predicted Event' if i == 0 else "",
                                   alpha=0.7)
        
        ax.set_yticks(range(len(current_sites)))
        ax.set_yticklabels(current_sites, fontsize=10)
        
        # Enhance x-axis
        ax.set_xlim(date_range.min(), date_range.max())
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        
        # Rotate and align the tick labels so they look better
        plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
        
        # Add labels and title
        plt.xlabel('Time Period (2017)', fontsize=12, labelpad=10)
        plt.ylabel('Weather Station Sites', fontsize=12, labelpad=10)
        plt.title(f'Weather Events Detection - 17 (Group {plot_num + 1})\nActual vs Predicted Events', 
                 fontsize=14, pad=20)
        
        # Enhance legend
        legend = plt.legend(loc='upper right', fontsize=10, 
                          bbox_to_anchor=(1.15, 1),
                          borderaxespad=0.)
        legend.get_frame().set_alpha(0.9)
        
        # Add subtle background color to differentiate rows
        for i in range(len(current_sites)):
            if i % 2:
                ax.axhspan(i-0.5, i+0.5, color='gray', alpha=0.1)
        
        # Adjust layout
        plt.tight_layout()
        
        filename = os.path.join(output_folder, f'sites_plot_group_{plot_num + 1}.png')
        plt.savefig(filename, bbox_inches='tight', dpi=300)
        plt.close()

if __name__ == "__main__":
    print("Sample timestamps:", results_df['Timestamp'].head())
    
    results_df = identify_unique_events(results_df)
    results_df.to_csv('label_2017.csv')
    
    output_folder = "/root/data/rrr/integrated_weather_dataset/scripts/analysis/lstm_results_2"
    
    create_site_plots(results_df, output_folder, sites_per_plot=10)
    
    print(f"Plots have been saved to the '{output_folder}' directory.")