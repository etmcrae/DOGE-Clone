import os
from datetime import datetime
from time import sleep

import numpy as np
import pandas as pd
import requests as req
import validators
from bs4 import BeautifulSoup
from ratelimit import limits, sleep_and_retry
from selenium.webdriver import Firefox
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from tqdm import tqdm

import requests
import zipfile
import io


# === Step 1: Download and extract CSVs from USAspending ===
def fetch_usaspending_data(file_name):
    print(f"Downloading {file_name} from USAspending...")
    download_url = f"https://files.usaspending.gov/award_data_archive/FY2025_All_Contracts_Full_20250506.zip"
    dataframes = []  # List to store DataFrames from each CSV
    try:
        response = requests.get(download_url)
        response.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_files_found = False
            for file in z.namelist():
                if file.endswith('.csv'):
                    csv_files_found = True
                    print(f"Extracting and reading {file}")
                    # Read the CSV into a pandas DataFrame
                    # low_memory=False is often needed for large CSVs to avoid mixed type warnings
                    try:
                        df = pd.read_csv(z.open(file), low_memory=False)
                        dataframes.append(df)
                    except Exception as read_error:
                        print(f"Error reading CSV file {file}: {read_error}")

            if not csv_files_found:
                print(f"No CSV file found in {file_name}")

    except requests.exceptions.RequestException as e:
        print(f"Failed to download {file_name}: {e}")
    except zipfile.BadZipFile:
        print(f"Failed to open zip file: {file_name}. It might be corrupted or not a zip file.")
    except Exception as e:
        print(f"An unexpected error occurred during download or extraction: {e}")

    return dataframes # Return a list of DataFrames

# === Step 2: Fetch DOGE contracts (limited to 500 results) ===
def fetch_doge_contracts(per_page=500):

    url = f"https://api.doge.gov/savings/contracts?per_page={per_page}"
    print(f"Fetching DOGE contracts from {url} (per_page={per_page})...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Convert the JSON response to a pandas DataFrame
        df = pd.DataFrame(data)
        print(f"Retrieved {len(df)} contracts from DOGE API.")
        return df
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch DOGE contracts from API: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during API fetch: {e}")

    return pd.DataFrame() # Return an empty DataFrame on failure


# === Step 3: Merge on specified keys ===
def merge_data(df1, df2, left_on, right_on):
    if left_on not in df1.columns:
        print(f"'{left_on}' not found in the first dataset (USAspending).")
        return pd.DataFrame()
    if right_on not in df2.columns:
        print(f"'{right_on}' not found in the second dataset (DOGE).")
        return pd.DataFrame()

    print(f"Merging datasets on '{left_on}' (USAspending) and '{right_on}' (DOGE)...")
    # Perform the outer merge
    merged = pd.merge(df1, df2, left_on=left_on, right_on=right_on, how='outer')
    print(f"Merged {len(df1)} rows (USAspending) + {len(df2)} rows (DOGE) → {len(merged)} total rows.")
    return merged

# === Main Execution Flow ===
def main():
    # Define the filename for the USAspending data
    usaspending_file_name = "FY2025_All_Contracts_Full_20250506.zip"
    # Define the merge keys for each dataset
    usaspending_merge_key = 'award_id_piid'
    doge_merge_key = 'piid'

    # Fetch data from USAspending
    usaspending_dataframes = fetch_usaspending_data(usaspending_file_name)

    # Check if data loading was successful and concatenate DataFrames
    if not usaspending_dataframes:
        print("❌ No USAspending dataframes were loaded. Cannot proceed with merge.")
        return

    print(f"Concatenating {len(usaspending_dataframes)} USAspending dataframes...")
    df_usaspending = pd.concat(usaspending_dataframes, ignore_index=True)
    print(f"Total rows in concatenated USAspending data: {len(df_usaspending)}")

    # Fetch data from DOGE API
    df_doge = fetch_doge_contracts()

    # Check if DOGE data loading was successful
    if df_doge.empty:
         print("❌ Failed to load DOGE data. Cannot proceed with merge.")
         return

    # Perform the merge using the specified keys
    merged_df = merge_data(df_usaspending, df_doge, left_on=usaspending_merge_key, right_on=doge_merge_key)

    # Check if the merge was successful (i.e., returned a non-empty DataFrame if data was loaded)
    if merged_df.empty and (not df_usaspending.empty and not df_doge.empty):
        print("⚠️ Merge resulted in an empty DataFrame. Check merge keys and data.")
    elif not merged_df.empty:
        # Save the merged DataFrame to a CSV file
        output_filename = "merged_award_data_by_piid.csv"
        merged_df.to_csv(output_filename, index=False)
        print(f"✅ Saved merged data to '{output_filename}'")

# Execute the main function when the script is run directly
if __name__ == "__main__":
    main()
