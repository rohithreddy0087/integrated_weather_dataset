import pandas as pd
import numpy as np
import time

zwd_df = pd.read_csv('/gnn/rrr/integrated_weather_dataset/data/processed/Troposphere/2004.csv')
guan_df = pd.read_csv('/gnn/rrr/integrated_weather_dataset/data/processed/Guan/2004.csv')
rutz_df = pd.read_csv('/gnn/rrr/integrated_weather_dataset/data/processed/Rutz/2004.csv')
print("All dataset loaded")

zwd_df["Timestamp"] = pd.to_datetime(zwd_df["Timestamp"])
guan_df["Timestamp"] = pd.to_datetime(guan_df["Timestamp"])
rutz_df["Timestamp"] = pd.to_datetime(rutz_df["Timestamp"])
rutz_df = rutz_df.rename(columns={
    'longitude': 'Longitude', 
    'latitude': 'Latitude',
    'ARs': 'Rutz_AR_Label'
})

print(rutz_df)
print("Start the process")
LAT_TOL = 0.25
LON_TOL = 0.3125
TIME_TOLERANCE = pd.Timedelta(seconds = 120)
CLOSEST_TIME_WINDOW = pd.Timedelta(hours=3)

start_time = time.time()
def precompute_spatial_matches_guan(zwd_df, ar_df):
    spatial_matches = {}
    for _, site_row in zwd_df.iterrows():
        site_id = site_row["Site"]
        lat1, lon1 = site_row["Latitude"], site_row["Longitude"]
        spatial_mask = (
            (guan_df["Latitude"] >= lat1 - LAT_TOL)
            & (guan_df["Latitude"] <= lat1 + LAT_TOL)
            & (guan_df["Longitude"] >= lon1 - LON_TOL)
            & (guan_df["Longitude"] <= lon1 + LON_TOL)
        )
        spatial_matches[site_id] = guan_df[spatial_mask].reset_index(drop=True)
    return spatial_matches

def precompute_spatial_matches_rutz(zwd_df, ar_df):
    spatial_matches = {}
    for _, site_row in zwd_df.iterrows():
        site_id = site_row["Site"]
        lat1, lon1 = site_row["Latitude"], site_row["Longitude"]
        spatial_mask = (
            (rutz_df["Latitude"] >= lat1 - LAT_TOL)
            & (rutz_df["Latitude"] <= lat1 + LAT_TOL)
            & (rutz_df["Longitude"] >= lon1 - LON_TOL)
            & (rutz_df["Longitude"] <= lon1 + LON_TOL)
        )
        spatial_matches[site_id] = rutz_df[spatial_mask].reset_index(drop=True)
    return spatial_matches

def process_site_day_guan(site, zwd_site_df, ar_site_df):
    exact_match_labels = []
    closest_match_labels = []

    for _, zwd_row in zwd_site_df.iterrows():
        time1 = zwd_row["Timestamp"]

        exact_label = np.nan
        closest_label = 0

        if not ar_site_df.empty:
            time_diffs = abs(ar_site_df["Timestamp"] - time1)
            exact_matches = ar_site_df[time_diffs <= TIME_TOLERANCE]

            closest_match = None
            if not exact_matches.empty:
                exact_label = exact_matches.iloc[0]["Guan_AR_Label"]
            else:
                time_diffs_within_window = time_diffs[time_diffs <= CLOSEST_TIME_WINDOW]
                if not time_diffs_within_window.empty:
                    closest_index = time_diffs_within_window.idxmin()
                    closest_match = ar_site_df.loc[closest_index]
                    closest_label = closest_match["Guan_AR_Label"]

        exact_match_labels.append(exact_label)
        closest_match_labels.append(closest_label)

    if len(exact_match_labels) == len(zwd_site_df):
        zwd_site_df["Guan_exact_match_label"] = exact_match_labels
        zwd_site_df["Guan_Label"] = closest_match_labels
        
    else:
        raise ValueError("Length of labels does not match the number of rows in the site dataframe.")

    return zwd_site_df

def process_site_day_rutz(site, zwd_site_df, ar_site_df):
    exact_match_labels = []
    closest_match_labels = []

    for _, zwd_row in zwd_site_df.iterrows():
        time1 = zwd_row["Timestamp"]

        exact_label = np.nan
        closest_label = 0

        if not ar_site_df.empty:
            time_diffs = abs(ar_site_df["Timestamp"] - time1)
            exact_matches = ar_site_df[time_diffs <= TIME_TOLERANCE]

            closest_match = None
            if not exact_matches.empty:
                exact_label = exact_matches.iloc[0]["Rutz_AR_Label"]
            else:
                time_diffs_within_window = time_diffs[time_diffs <= CLOSEST_TIME_WINDOW]
                if not time_diffs_within_window.empty:
                    closest_index = time_diffs_within_window.idxmin()
                    closest_match = ar_site_df.loc[closest_index]
                    closest_label = closest_match["Rutz_AR_Label"]

        exact_match_labels.append(exact_label)
        closest_match_labels.append(closest_label)

    if len(exact_match_labels) == len(zwd_site_df):
        zwd_site_df["Rutz_exact_match_label"] = exact_match_labels
        zwd_site_df["Rutz_Label"] = closest_match_labels
    else:
        raise ValueError("Length of labels does not match the number of rows in the site dataframe.")

    return zwd_site_df
        
def process_guans(zwd_df, ar_df):
    zwd_sites_df = zwd_df[["Site", "Latitude", "Longitude"]].drop_duplicates()

    ar_spatial_matches = precompute_spatial_matches_guan(zwd_sites_df, ar_df)
    zwd_df["Day"] = zwd_df["Timestamp"].dt.date
    ar_df["Day"] = ar_df["Timestamp"].dt.date
    all_results = []
    for site_id in zwd_sites_df["Site"].unique():
        print("Guan Labels Running")
        print(f"Processing site: {site_id}")

        zwd_site_df = zwd_df[zwd_df["Site"] == site_id].reset_index(drop=True)
        ar_site_df = ar_spatial_matches.get(site_id, pd.DataFrame())

        for day in zwd_site_df["Day"].unique():
            zwd_day_df = zwd_site_df[zwd_site_df["Day"] == day].reset_index(drop=True)

            labeled_day_df = process_site_day_guan(site_id, zwd_day_df, ar_site_df)
            all_results.append(labeled_day_df)

            print(f"Processed day: {day} for site: {site_id}")
    final_result = pd.concat(all_results, ignore_index=True)

    print(f"labeled data done")
    return final_result

def process_rutz(zwd_df, ar_df):
    zwd_sites_df = zwd_df[["Site", "Latitude", "Longitude"]].drop_duplicates()

    ar_spatial_matches = precompute_spatial_matches_rutz(zwd_sites_df, ar_df)
    zwd_df["Day"] = zwd_df["Timestamp"].dt.date
    ar_df["Day"] = ar_df["Timestamp"].dt.date
    all_results = []
    for site_id in zwd_sites_df["Site"].unique():
        print("Rutz Labels Running")
        print(f"Processing site: {site_id}")

        zwd_site_df = zwd_df[zwd_df["Site"] == site_id].reset_index(drop=True)
        ar_site_df = ar_spatial_matches.get(site_id, pd.DataFrame())

        for day in zwd_site_df["Day"].unique():
            zwd_day_df = zwd_site_df[zwd_site_df["Day"] == day].reset_index(drop=True)

            labeled_day_df = process_site_day_rutz(site_id, zwd_day_df, ar_site_df)
            all_results.append(labeled_day_df)

            print(f"Processed day: {day} for site: {site_id}")
    final_result = pd.concat(all_results, ignore_index=True)

    print(f"labeled data done")
    return final_result

df1 = zwd_df[0:1000]
df2 = guan_df[0:1000]
df3 = rutz_df[0:1000]

final_result1 = process_guans(df1,df2)
final_result2 = process_rutz(final_result1,df3)

final_result2 = final_result2.drop(columns=['Day'])
end_time = time.time()
print("For 1000 columns, time taken", end_time - start_time)
print(final_result2)
