import os
import pandas as pd

# Folder path containing the files
folder_path = '/Users/nitin14.patil/Library/CloudStorage/OneDrive-RelianceCorporateITParkLimited/Documents/python_work/PNL Finance/vas/'

# Get a list of all CSV files in the folder
files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

# Initialize an empty list to store DataFrames
dfs = []

# Loop through all the files and read them
for file in files:
    file_path = os.path.join(folder_path, file)
    df = pd.read_csv(file_path)  # Adjust if using other formats like pd.read_excel()
    dfs.append(df)

# Concatenate all the DataFrames along axis=0
merged_df = pd.concat(dfs, axis=0, ignore_index=True)

# Display the resulting DataFrame
print(merged_df)

merged_df.to_csv('vas-consolidated-data.csv')
