import numpy as np
import pandas as pd
import sys
import os
import duckdb
import matplotlib.pyplot as plt
import seaborn as sns

def level_preprocesing(df):
    df["LEVEL_"].replace("N","unknown", inplace=True)
    df["LEVEL_"].replace("1","preschool", inplace=True)
    df["LEVEL_"].replace("2","elementary_school", inplace=True)
    df["LEVEL_"].replace("3","middle_school", inplace=True)
    df["LEVEL_"].replace("4","high_school", inplace=True)
    return df


def type_preprocesing(df):
    df["TYPE"].replace(np.nan,"unknown", inplace=True)
    for i in range(1,5):
        df["TYPE"].replace(i,"Type "+str(i), inplace=True)
    return df

def status_preprocesing(df):
    df["STATUS"].replace(np.nan,"unknown", inplace=True)
    for i in range(1,9):
        df["STATUS"].replace(i,"STATUS "+str(i), inplace=True)
    return df


def trusted_schools():

    con = duckdb.connect(database="data/trusted/db_schools.db", read_only=False)
    df = con.execute("SELECT * FROM schools;").fetchdf()

    df = level_preprocesing(df)
    df = type_preprocesing(df)
    df = status_preprocesing(df)
    
    con.execute("CREATE OR REPLACE TABLE schools AS SELECT * FROM df;")
    con.close

if __name__ == "__main__":
    trusted_schools()