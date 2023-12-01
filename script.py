

import pathlib
import os
import json
import pandas as pd
from timeslib.misc import read_data_csv
import glob

# Specify the path to the directory
dir_path = 'Utils/output_csv'

# Get a list of all files and directories in the directory
folders = os.listdir(dir_path)

input_folders = [os.path.join(dir_path, f) for f in folders if os.path.isdir(os.path.join(dir_path, f))]

output_folder = pathlib.Path('Output/')

with open("./tim-tables-info/table_info.json", "r") as file:
    table_info = json.load(file)
    
with open("./tim-tables-info/colours.json", "r") as file:
    colours_info = json.load(file)

# Create an empty list to store DataFrames


for input_folder in input_folders:
    # get list of all input data files with a certain name extension
    path_list = sorted(pathlib.Path(input_folder).rglob("*.csv"))

    #Read csv data into a dataframe

    # Create an empty DataFrame
    dfs = []
    # Read data into the dataframe
    for a_table in table_info.keys():
        for a_table_rule in table_info[a_table].keys():
            file_path = pathlib.Path(input_folder) / (a_table_rule + ".csv")
            if file_path.exists():
                df = read_data_csv(file_path,
                                {a_table_rule: table_info[a_table][a_table_rule]})
                if df is not None:
                    df["tableName"] = a_table
                    dfs.append(df)
    # Concatenate DataFrames if the list is not empty
    if dfs:
        data = pd.concat(dfs, ignore_index=True)
    else:
        data = pd.DataFrame()


    assert len(data.index), "The dataframe is empty. No data has been read."

    data = data.groupby([i for i in data.columns if not i == "total"]).agg("sum")
    data = data.reset_index()
    ################################################################################################################
    ##########Print to JSON files###################################################################################
    ################################################################################################################
    for aScenario in data["scenario"].unique():
        (output_folder/aScenario).mkdir(parents=True, exist_ok=True)
        for aTable in data[data["scenario"]==aScenario]["tableName"].unique():
            data_dict = {"name": aScenario}
            series_dict_list = list()
            for aSeries in data[(data["scenario"]==aScenario) &
                                (data["tableName"]==aTable)]["seriesName"].unique():
                series_data = data[(data["scenario"]==aScenario) &
                                   (data["tableName"]==aTable) &
                                   (data["seriesName"]==aSeries)][["year","total"]].to_dict("split")["data"]
                series_dict_list += [{"seriesName": aSeries,
                                "seriesValues": series_data}]
            data_dict["data"] = series_dict_list
            with open(output_folder / aScenario / (aTable + ".json"), "w", encoding="utf-8") as file:
                js_str = json.dumps(data_dict, indent=2)
                file.write(js_str)
                
    charts_info = {}
    for aTable in data["tableName"].unique():
        seriesNames = list(data[(data["tableName"] == aTable)]["seriesName"].unique())
        seriesColours = [colours_info[item] if item in colours_info.keys() else "#A020F0" for item in seriesNames]
        seriesUnit = data[(data["tableName"] == aTable)]["label"].unique()[0]
        charts_info[aTable] = {
                    "seriesNames": seriesNames,
                    "colorScale": seriesColours,
                    "unit": seriesUnit
        }

    with open(output_folder / "chartsInfo.json", "w", encoding="utf-8") as file:
                js_str = json.dumps(charts_info, indent=2)
                file.write(js_str)