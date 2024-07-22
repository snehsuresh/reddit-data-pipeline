import pandas as pd
import os
from utils.constants import STATIC_PATH

# List all files in the static folder
files = os.listdir(STATIC_PATH)

# Initialize an empty list to hold DataFrames
dataframes = []

# Read and filter each CSV file
for file in files:
    file_path = os.path.join(STATIC_PATH, file)
    df = pd.read_csv(file_path, usecols=['name', 'genre'])
    dataframes.append(df)

# Concatenate all DataFrames
merged_df = pd.concat(dataframes, ignore_index=True)

# Save the combined DataFrame to a new CSV file in the static folder
output_file_path = os.path.join(STATIC_PATH, 'merged_file.csv')
merged_df.to_csv(output_file_path, index=False)

print(f"Files merged and saved as '{output_file_path}'.")