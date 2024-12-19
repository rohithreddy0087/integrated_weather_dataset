import pandas as pd
import numpy as np
import time

def process_site(site, zwd_df, guan_df, rutz_df, LAT_TOL=0.25, LON_TOL=0.3125):
    site_mask = zwd_df["Site"] == site
    site_data = zwd_df[site_mask].copy()
    site_data["Timestamp"] = site_data["Timestamp"].dt.round('5min')
    
    result_columns = {
        "Guan_exact_match_label": np.nan,
        "Guan_Label": 0,
        "Rutz_exact_match_label": np.nan,
        "Rutz_Label": 0,
        "IVT": np.nan
    }
    for col in result_columns:
        site_data[col] = result_columns[col]
    
    lat, lon = site_data.iloc[0][["Latitude", "Longitude"]]
    
    guan_spatial_mask = (
        (guan_df["Latitude"] >= lat - LAT_TOL)
        & (guan_df["Latitude"] <= lat + LAT_TOL)
        & (guan_df["Longitude"] >= lon - LON_TOL)
        & (guan_df["Longitude"] <= lon + LON_TOL)
    )
    rutz_spatial_mask = (
        (rutz_df["Latitude"] >= lat - LAT_TOL)
        & (rutz_df["Latitude"] <= lat + LAT_TOL)
        & (rutz_df["Longitude"] >= lon - LON_TOL)
        & (rutz_df["Longitude"] <= lon + LON_TOL)
    )
    
    guan_matches = guan_df[guan_spatial_mask]
    rutz_matches = rutz_df[rutz_spatial_mask]
    
    site_timestamps = site_data["Timestamp"].values
    
    if not guan_matches.empty:
        guan_timestamps = guan_matches["Timestamp"].values
        guan_labels = guan_matches["Guan_AR_Label"].values
        
        time_diffs = np.abs(site_timestamps[:, np.newaxis] - guan_timestamps)
        
        exact_mask = time_diffs <= np.timedelta64(2, 'm')
        exact_indices = np.where(exact_mask.any(axis=1), time_diffs.argmin(axis=1), -1)
        
        closest_mask = time_diffs <= np.timedelta64(3, 'h')
        closest_indices = np.where(closest_mask.any(axis=1), time_diffs.argmin(axis=1), -1)
        
        mask = exact_indices != -1
        site_data.loc[mask, "Guan_exact_match_label"] = guan_labels[exact_indices[mask]]
        
        mask = closest_indices != -1
        site_data.loc[mask, "Guan_Label"] = guan_labels[closest_indices[mask]]
    
    if not rutz_matches.empty:
        rutz_timestamps = rutz_matches["Timestamp"].values
        rutz_labels = rutz_matches["Rutz_AR_Label"].values
        rutz_ivt = rutz_matches["IVT"].values
        
        time_diffs = np.abs(site_timestamps[:, np.newaxis] - rutz_timestamps)
        
        exact_mask = time_diffs <= np.timedelta64(2, 'm')
        exact_indices = np.where(exact_mask.any(axis=1), time_diffs.argmin(axis=1), -1)
        
        closest_mask = time_diffs <= np.timedelta64(3, 'h')
        closest_indices = np.where(closest_mask.any(axis=1), time_diffs.argmin(axis=1), -1)
        
        mask = exact_indices != -1
        site_data.loc[mask, "Rutz_exact_match_label"] = rutz_labels[exact_indices[mask]]
        
        mask = closest_indices != -1
        site_data.loc[mask, "Rutz_Label"] = rutz_labels[closest_indices[mask]]
        site_data.loc[mask, "IVT"] = rutz_ivt[closest_indices[mask]]
    
    if 'Day' in site_data.columns:
        site_data = site_data.drop(columns=['Day'])
    temp_data = site_data.sort_values(by = ["Timestamp","Site"]).reset_index(drop=True)
    return temp_data

for year in range(2005, 2023):  # 2005 to 2022
    print(f"\nProcessing year {year}")
    
    zwd_df = pd.read_csv(f'/root/data/rrr/integrated_weather_dataset/data/processed/Troposphere/{year}.csv')
    guan_df = pd.read_csv(f'/root/data/rrr/integrated_weather_dataset/data/processed/Guan/{year}.csv')
    rutz_df = pd.read_csv(f'/root/data/rrr/integrated_weather_dataset/data/processed/Rutz/{year}.csv')
    output_file = f'/root/data/rrr/integrated_weather_dataset/data/final_icid/{year}.csv'
    print(f"All dataset loaded for {year}")

    zwd_df["Timestamp"] = pd.to_datetime(zwd_df["Timestamp"],format='mixed')
    guan_df["Timestamp"] = pd.to_datetime(guan_df["Timestamp"],format='mixed')
    rutz_df["Timestamp"] = pd.to_datetime(rutz_df["Timestamp"],format='mixed')
    rutz_df = rutz_df.rename(columns={
        'longitude': 'Longitude', 
        'latitude': 'Latitude',
        'ARs': 'Rutz_AR_Label'
    })

    start_time = time.time()
    sites = sorted(zwd_df["Site"].unique())
    first_site = sites[0]
    first_site_data = process_site(first_site, zwd_df, guan_df, rutz_df)
    first_site_data.to_csv(output_file, index=False)
    print(f"Processed and saved site: {first_site}")
    
    for site in sites[1:]:
        try:
            print(f"Processing site: {site}")
            site_data = process_site(site, zwd_df, guan_df, rutz_df)
            site_data.to_csv(output_file, mode='a', header=False, index=False)
            print(f"Saved results for site: {site}")
        except Exception as e:
            print(f"Error processing site {site}: {str(e)}")
            continue
    
    end_time = time.time()
    print(f"Year {year} completed in {end_time - start_time} seconds")
    
# LAT_TOL = 0.25
# LON_TOL = 0.3125
# CLOSEST_TIME_WINDOW = pd.Timedelta(hours=3)

# def round(timestamp):
#     return pd.Timestamp(timestamp).round('5min')


# def precompute_spatial_matches(zwd_df, ar_df, LAT_TOL, LON_TOL):
#     spatial_matches = {}
#     for _, site_row in zwd_df.iterrows():
#         site_id = site_row["Site"]
#         lat1, lon1 = site_row["Latitude"], site_row["Longitude"]
#         spatial_mask = (
#             (ar_df["Latitude"] >= lat1 - LAT_TOL)
#             & (ar_df["Latitude"] <= lat1 + LAT_TOL)
#             & (ar_df["Longitude"] >= lon1 - LON_TOL)
#             & (ar_df["Longitude"] <= lon1 + LON_TOL)
#         )
#         spatial_matches[site_id] = ar_df[spatial_mask].reset_index(drop=True)
#     return spatial_matches
# def process_site_day(site, zwd_site_df, ar_site_df, source):
#     exact_match_labels = []
#     closest_match_labels = []
#     ivt_values = [] if source == 'Rutz' else None
#     CLOSEST_TIME_WINDOW = pd.Timedelta(hours=3)
#     EXACT_TIME_TOLERANCE = pd.Timedelta(minutes=2)
    
#     zwd_site_df = zwd_site_df.copy()
#     zwd_site_df["Timestamp"] = zwd_site_df["Timestamp"].apply(round)
#     zwd_site_df["Timestamp"] = pd.to_datetime(zwd_site_df["Timestamp"])

#     for _, zwd_row in zwd_site_df.iterrows():
#         rounded_time = zwd_row["Timestamp"]
#         exact_label = np.nan
#         closest_label = 0
#         ivt = np.nan if source == 'Rutz' else None
        
#         if not ar_site_df.empty:
#             time_difference = abs(ar_site_df["Timestamp"] - rounded_time)
#             exact_matches = ar_site_df[time_difference <= EXACT_TIME_TOLERANCE]

#             if not exact_matches.empty:
#                 exact_label = exact_matches.iloc[0][f"{source}_AR_Label"]
#                 if source == 'Rutz':
#                     ivt = exact_matches.iloc[0]["IVT"]
            
#             time_diffs = abs(ar_site_df["Timestamp"] - rounded_time)
#             time_diffs_within_window = time_diffs[time_diffs <= CLOSEST_TIME_WINDOW]
#             if not time_diffs_within_window.empty:
#                 closest_index = time_diffs_within_window.idxmin()
#                 closest_match = ar_site_df.loc[closest_index]
#                 closest_label = closest_match[f"{source}_AR_Label"]
#                 if source == 'Rutz': 
#                     ivt = closest_match["IVT"]
        
#         exact_match_labels.append(exact_label)
#         closest_match_labels.append(closest_label)
#         if source == 'Rutz':
#             ivt_values.append(ivt)
    
#     zwd_site_df[f"{source}_exact_match_label"] = exact_match_labels
#     zwd_site_df[f"{source}_Label"] = closest_match_labels
#     if source == 'Rutz':
#         zwd_site_df["IVT"] = ivt_values
    
#     return zwd_site_df

# def process_data(zwd_df, guan_df, rutz_df, LAT_TOL=0.25, LON_TOL=0.3125):
#     zwd_sites_df = zwd_df[["Site", "Latitude", "Longitude"]].drop_duplicates()
    
#     guan_spatial_matches = precompute_spatial_matches(zwd_sites_df, guan_df, LAT_TOL, LON_TOL)
#     rutz_spatial_matches = precompute_spatial_matches(zwd_sites_df, rutz_df, LAT_TOL, LON_TOL)
    
#     zwd_df["Day"] = zwd_df["Timestamp"].dt.date
#     guan_df["Day"] = guan_df["Timestamp"].dt.date
#     rutz_df["Day"] = rutz_df["Timestamp"].dt.date
    
#     all_results = []
#     for site_id in zwd_sites_df["Site"].unique():
#         print(f"Processing site: {site_id}")
        
#         zwd_site_df = zwd_df[zwd_df["Site"] == site_id].reset_index(drop=True)
#         guan_site_df = guan_spatial_matches.get(site_id, pd.DataFrame())
#         rutz_site_df = rutz_spatial_matches.get(site_id, pd.DataFrame())
        
#         for day in zwd_site_df["Day"].unique():
#             zwd_day_df = zwd_site_df[zwd_site_df["Day"] == day].reset_index(drop=True)
            
#             labeled_day_df = process_site_day(site_id, zwd_day_df, guan_site_df, 'Guan')
#             labeled_day_df = process_site_day(site_id, labeled_day_df, rutz_site_df, 'Rutz')
            
#             all_results.append(labeled_day_df)
#             print(f"Processed day: {day} for site: {site_id}")
            
#     final_result = pd.concat(all_results, ignore_index=True)
#     final_result = final_result.drop(columns=['Day'])
    
#     return final_result

# df1 = zwd_df[0:100000]
# df2 = guan_df[0:100000]
# df3 = rutz_df[0:100000]

# final_result = process_data(df1, df2, df3)