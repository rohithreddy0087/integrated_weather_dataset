import logging
import pandas as pd

# Define a grid range
MIN_LON = -120
MAX_LON = -115
MIN_LAT = 31.5
MAX_LAT = 38

# Define time range
START_YEAR = 2005
END_YEAR = 2024

def get_logger(name = 'dataset', log_file = 'debug.log'):
    """helper function to get a logger object

    Args:
        name (str, optional): Logger name. Defaults to 'dataset'.
        log_file (str, optional): Debug file name. Defaults to 'debug.log'.

    Returns:
        logger object: returns a logger object
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    file_handler.setLevel(logging.DEBUG)
    console_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(log_file)
    
    console_formatter = logging.Formatter('%(asctime)s - %(message)s')
    file_formatter = logging.Formatter('%(asctime)s - %(message)s')

    console_handler.setFormatter(console_formatter)
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger

# Following snippet gives unique stations with respective latitude,longitude
def convert_longitude(longitude):
    """Converts longitude from [0,360] to [-180, 180]

    Args:
        longitude (float): longitude in [0,360]

    Returns:
        float: longitude in [-180, 180]
    """
    if longitude > 180:
        return longitude - 360
    else:
        return longitude
    
def read_sites(station_list_file = 'stations.txt'):
    """Read sites from the stations file

    Args:
        station_list_file (str, optional): path to stations file. Defaults to 'stations.txt'.

    Returns:
        dataframe: pandas dataframe with fields as Site, Lat, Lon
        List: list of unique sites
    """
    sites_df = pd.read_csv(station_list_file, delim_whitespace=True)
    sites_df['Site'] = sites_df['Site'].str.upper()
    sites_df['Longitude'] = sites_df['Longitude'].apply(lambda x: convert_longitude(x))
    sites_df = sites_df[(sites_df['Latitude'] >= MIN_LAT) & (sites_df['Latitude'] <= MAX_LAT) &
                    (sites_df['Longitude'] >= MIN_LON) & (sites_df['Longitude'] <= MAX_LON)]
    sites_df = sites_df.reset_index(drop=True)
    sites_df = sites_df[['Site', 'Latitude', 'Longitude']].drop_duplicates()
    sites_list = list(sites_df.itertuples(index=False, name=None))
    return sites_df, sites_list