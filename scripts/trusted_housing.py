import numpy as np
import pandas as pd
import sys
import os
import duckdb
import matplotlib.pyplot as plt
import seaborn as sns

#"cats_allowed","dogs_allowed","smoking_allowed","wheelchair_access","comes_furnished"

def binary_preprocesing(df,cols):
    for i in cols:
        df[i].replace(0,"No", inplace=True)
        df[i].replace(1,"Yes", inplace=True)

    return df

def non_0_variables(df,cols):
    for i in cols:
        df[i].replace(0, np.nan, inplace=True)
    return df

def trusted_housing():

    con = duckdb.connect(database="data/trusted/db_housing.db", read_only=False)
    df = con.execute("SELECT * FROM housing;").fetchdf()

    df = binary_preprocesing(df,["cats_allowed","dogs_allowed","smoking_allowed","wheelchair_access","comes_furnished","electric_vehicle_charge"])
    df = non_0_variables(df,["price","sqfeet"])

    con.execute("CREATE OR REPLACE TABLE housing AS SELECT * FROM df;")
    con.close

if __name__ == "__main__":
    trusted_housing()