# MOVE THE TABLES FROM LANDING TEMPORAL TO LANDING PERSISTANT

# import libraries
import sys
import os
import re

# get the list of datasources to process
sys.path.append('..')
sys.path.append('.')
from helper import *
setwd()
Objects = Objects()


def storeLogTemp(tbl, file):
    file.write(tbl + "\n")

def landingTemp2landingPers():
    
    # read all the files in the landing_temp folder
    files_temp = os.listdir('data/landing_temp')
    # read list of processed files
    processedFiles = []
    with open('logs/processed_data_temporal.txt', "r") as f0:
        for line in f0:
            processedFiles.append(line.rstrip())
    # list of files to be processed
    files_temp = [file for file in files_temp if file not in processedFiles]
    
    # open file to store logs in append mode
    f = open('logs/processed_data_temporal.txt', "a")

    # process files
    for datasource in Objects:
        id = datasource["id"]
        format = datasource["format"]

        # select the files to be processed of the current datasource
        pattern = datasource["landing_temp_name"]
        files_datasource = [file for file in files_temp if re.match(pattern, file)]

        for file in files_datasource:
            #tbl_id = re.search(datasource["landing_temp_tblID"], file).group(1) # id of the table within the dataset
            # try ... except Exception: tbl_id = pattern -> this can be used in case the dataset contains a single table without any identifier in the name
            try:
                tbl_id = re.search(datasource["landing_temp_tblID"], file).group(1) # id of the table within the dataset
            except Exception:
                tbl_id = datasource["landing_temp_tblID"] # for datasets with a single table without any identifier in the name

            from_file = os.path.join("data", "landing_temp", file)
            to_file = os.path.join("data", "landing_pers", id, f"{id}_{tbl_id}.{format}")
            os.system(f"cp {from_file} {to_file}")
            storeLogTemp(file, f)

landingTemp2landingPers()