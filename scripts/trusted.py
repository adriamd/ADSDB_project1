import os
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

# drop all columns with more than alpha misses frequency or are constants
def drop_columns(df,alpha=0.9, constants=True):
    types_df = [str(x) for x in list(df.dtypes)]
    columns_names = list(df.columns)
    missings = 0
    n = len(df)
    drop_list = []

    for i in range(len(columns_names)):

        if types_df[i] == "float64" or types_df[i] == "int32":
            missings = df[columns_names[i]].isna().sum()
            
            if constants and min(df[columns_names[i]]) == max(df[columns_names[i]]):
                drop_list.append(columns_names[i])
                continue

        elif types_df[i] == "object":
            counts = dict(df[columns_names[i]].value_counts())
            if "unknow" in counts:
                missings = counts["unknow"]

            if constants and len(counts) == 1:
                drop_list.append(columns_names[i])
                continue

        if missings/n > alpha:
            drop_list.append(columns_names[i])

    return df.drop(drop_list,axis=1)

# transform all codes of missings to np.nan for numeric and unknown for the others
def standar_missings(df):
    types_df = [str(x) for x in list(df.dtypes)]
    columns_names = list(df.columns)

    for i in range(len(columns_names)):

        col_name = columns_names[i]

        if types_df[i] == "float64" or types_df[i] == "int32":
            df[col_name] = df[col_name].replace(-999, np.nan)

        elif types_df[i] == "object":
            df[col_name] = df[col_name].replace(np.nan, "unknow")
            df[col_name] = df[col_name].replace("", "unknow")
            df[col_name] = df[col_name].replace("xxxxxx", "unknow")
            df[col_name] = df[col_name].replace("nan", "unknow")
            df[col_name] = df[col_name].replace("......", "unknow")
            df[col_name] = df[col_name].replace("NOT AVAILABLE", "unknow")


def formatted2trusted(Objects):

    processed_table = []
    with open('logs/processed_data_trusted.txt', "r") as f0:
        for line in f0:
            processed_table.append(line.rstrip())

    for datasource in Objects:
        id = datasource["id"]
        db = "data/formatted/db_{}.db".format(id)
        con = duckdb.connect(database=db, read_only=True)

        tables = con.execute("SHOW TABLES").fetchall()
        tables_names = [x[0] for x in tables]

        tables_not_processed = [t for t in tables_names if t not in processed_table]
        if len(tables_not_processed) == 0:
            continue

        dataframes = []
        for table in tables_not_processed:
            dataframes.append(con.execute("SELECT * FROM {}".format(table)).fetchdf())

        con.close()

        df = pd.concat(dataframes)
        standar_missings(df)
        df.drop_duplicates()
        df = drop_columns(df)

        my_file = Path("data/trusted/db_{}.db".format(id))
        if my_file.is_file():
            con = duckdb.connect(database="data/trusted/db_{}.db".format(id), read_only=False)
            df_original = con.execute("SELECT * FROM {}".format(id)).fetchdf()
            df = pd.concat([df,df_original])
            df.drop_duplicates()
            df = drop_columns(df)
            con.execute("CREATE OR REPLACE TABLE {} AS SELECT * FROM df".format(id))
            con.close()

        else:
            con = duckdb.connect(database="data/trusted/db_{}.db".format(id), read_only=False)
            con.execute("CREATE TABLE {} AS SELECT * FROM df".format(id))
            con.close()

        if os.path.exists("scripts/trusted_{}.py".format(id)):
            os.system("python3 ./scripts/trusted_{}.py".format(id))

        with open('logs/processed_data_trusted.txt', "a") as f0:
            for table in tables_not_processed:
                f0.write(table + "\n")


if __name__ == "__main__":
    sys.path.append('..')
    sys.path.append('.')
    from helper import *

    setwd()
    Objects = Objects()
    formatted2trusted(Objects)
