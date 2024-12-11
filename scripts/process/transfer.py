import shutil
import os
from tqdm import tqdm

def move_csv_files(source_folder, destination_folder):
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)
    
    csv_files = [file for file in os.listdir(source_folder) if file.endswith('.csv')]
    
    for file_name in tqdm(csv_files, desc="Moving files", unit="file"):
        full_source_path = os.path.join(source_folder, file_name)
        full_destination_path = os.path.join(destination_folder, file_name)
        shutil.move(full_source_path, full_destination_path)

source = "/root/data//rrr/ES3-TACLS/AR/dataset/troposphere_data/"
destination = "/root/data/rrr/integrated_weather_dataset/data/processed/Troposphere/"
move_csv_files(source, destination)
