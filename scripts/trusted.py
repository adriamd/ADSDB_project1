import os
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd



def profiling_df(df):
    types_df = [str(x) for x in list(df.dtypes)]
    columns_names = list(df.columns)
    missings = dict()

    for i in range(len(columns_names)):

        if types_df[i] == "float64" or types_df[i] == "int32":
            missings[columns_names[i]] = df[columns_names[i]].isna().sum()

        elif types_df[i] == "object":
            counts = dict(df[columns_names[i]].value_counts())
            if "unknow" in counts:
                missings[columns_names[i]] = counts["unknow"]
            else:
                missings[columns_names[i]] = 0

    cols = []
    miss = []
    for x, y in missings.items():
        cols.append(x)
        miss.append(y)

    return {"Columns": cols, "Missings": miss}


def standarMissings(df):
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


def formatted2trusted():

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
        standarMissings(df)
        df.drop_duplicates()

        my_file = Path("data/trusted/db_{}.db".format(id))
        if my_file.is_file():
            con = duckdb.connect(database="data/trusted/db_{}.db".format(id), read_only=False)
            df_original = con.execute("SELECT * FROM {}".format(id)).fetchdf()
            df = pd.concat([df,df_original])
            df.drop_duplicates()
            con.execute("CREATE OR REPLACE TABLE {} AS SELECT * FROM df".format(id))
            con.close()

        else:
            con = duckdb.connect(database="data/trusted/db_{}.db".format(id), read_only=False)
            con.execute("CREATE TABLE {} AS SELECT * FROM df".format(id))
            con.close()

        prof = pd.DataFrame.from_dict(profiling_df(df))
        con = duckdb.connect(database="data/trusted/db_{}.db".format(id), read_only=False)
        con.execute("CREATE OR REPLACE TABLE profiling AS SELECT * FROM prof")
        con.close()

        with open('logs/processed_data_trusted.txt', "a") as f0:
            for table in tables_not_processed:
                f0.write(table + "\n")


if __name__ == "__main__":
    sys.path.append('..')
    sys.path.append('.')
    from helper import *

    setwd()
    Objects = Objects()
    formatted2trusted()
