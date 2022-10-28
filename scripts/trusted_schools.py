import numpy as np
import pandas as pd
import sys
import os
import duckdb
import matplotlib.pyplot as plt
import seaborn as sns
#from helper import *

def trusted_schools():

    con = duckdb.connect(database="data/trusted/db_schools.db", read_only=False)
    df_schools = con.execute("SELECT * FROM schools;").fetchdf()

    df_schools["LEVEL_"].replace("N","unknown", inplace=True)
    df_schools["LEVEL_"].replace("1","preschool", inplace=True)
    df_schools["LEVEL_"].replace("2","elementary_school", inplace=True)
    df_schools["LEVEL_"].replace("3","middle_school", inplace=True)
    df_schools["LEVEL_"].replace("4","high_school", inplace=True)
    
    con.execute("CREATE OR REPLACE TABLE schools AS SELECT * FROM df_schools;")
    con.close

if __name__ == "__main__":
    trusted_schools()