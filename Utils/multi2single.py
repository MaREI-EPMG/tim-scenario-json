#Utility to split scenarios from multiscenario export from VEDA in single batch job

import pandas as pd
import os
import glob
from pathlib import Path

# Get a list of all CSV files in the directory
csv_files = glob.glob('veda_csv/*.csv')

# Loop through each CSV file
for csv_file in csv_files:
  # Read the CSV file into a pandas DataFrame
  df = pd.read_csv(csv_file)
  
  # Sort the DataFrame by the first column, might be redundant but keep to ensure order
  df = df.sort_values(df.columns[0])
  
  # Group by the first column and write each group to a new CSV file in a new folder
  for name, group in df.groupby(df.columns[0]):
      folder_path = Path(f'output_csv/{name}')
      folder_path.mkdir(parents=True, exist_ok=True)
      group.to_csv(f'{folder_path}/{Path(csv_file).stem}.csv', index=False)
