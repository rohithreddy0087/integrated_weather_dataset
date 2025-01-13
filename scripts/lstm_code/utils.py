import pickle
import numpy as np
import pandas as pd
import dask.dataframe as dd
from geopy.distance import geodesic
import logging

min_lon = -120
max_lon = -115
min_lat = 31.5
max_lat = 38

def convert_longitude(longitude):
    if longitude > 180:
        return longitude - 360
    else:
        return longitude
    
def get_stations_list():
    station_list_file = '/root/data/rrr/ES3-TACLS/AR/catalogues/original/stations.txt'

    sites_df = pd.read_csv(station_list_file, delim_whitespace=True)
    sites_df['Site'] = sites_df['Site'].str.upper()
    sites_df['Longitude'] = sites_df['Longitude'].apply(lambda x: convert_longitude(x))

    sites_df = sites_df[(sites_df['Latitude'] >= min_lat) & (sites_df['Latitude'] <= max_lat) &
                    (sites_df['Longitude'] >= min_lon) & (sites_df['Longitude'] <= max_lon)]
    sites_df = sites_df.reset_index(drop=True)
    sites_df = sites_df[['Site', 'Latitude', 'Longitude']].drop_duplicates()
    sites_list = list(sites_df.itertuples(index=False, name=None))
    return sites_list


def get_adjacency_matrix(locations):
    """
    Given a list of locations (site, lat, lon), build an adjacency matrix based on inverse geospatial distance.
    
    Parameters:
    locations (list of tuples): List of tuples where each tuple contains (site, latitude, longitude).
    
    Returns:
    numpy.ndarray: Adjacency matrix based on inverse geospatial distance.
    """
    site_idx_dict = {}
    idx_site_dict = {}

    num_locations = len(locations)
    
    adj_matrix = np.zeros((num_locations, num_locations))
    
    # Calculate geospatial distance and inverse distance
    for i in range(num_locations):
        site_idx_dict[locations[i][0]] = i
        idx_site_dict[i] = locations[i][0]
        for j in range(num_locations):
            if i != j:
                coord1 = (locations[i][1], locations[i][2])
                coord2 = (locations[j][1], locations[j][2])
                distance = geodesic(coord1, coord2).kilometers
                if distance > 0 and distance < 1:
                    adj_matrix[i][j] = 0.9
                elif distance > 1:
                    adj_matrix[i][j] = 1 / distance
                else:
                    adj_matrix[i][j] = 0
            else:
                adj_matrix[i][j] = 1  # Diagonal elements are 1 (no self-loop)
    
    return adj_matrix, site_idx_dict, idx_site_dict

def get_logger():
    log_formatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s] %(message)s")
    logger = logging.getLogger("AR")
    fileHandler = logging.FileHandler("/root/data/rrr/ES3-TACLS/AR/windowed_lstm/debug.log")
    fileHandler.setFormatter(log_formatter)
    logger .addHandler(fileHandler)
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(log_formatter)
    logger .addHandler(consoleHandler)
    logger.setLevel(logging.DEBUG)
    return logger
    

if __name__ == '__main__':
    sites_list = get_stations_list()
    adjacency_matrix, site_idx_dict, _ = get_adjacency_matrix(sites_list)
    
    with open('/root/data/rrr/ES3-TACLS/AR/windowed_gcn/meta.pkl', 'wb') as f:
        save_dict = {'adjacency_matrix': adjacency_matrix, 'site_idx_dict': site_idx_dict}
        pickle.dump(save_dict, f)