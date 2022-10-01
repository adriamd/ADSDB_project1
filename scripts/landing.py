# MOVE THE TABLES FROM LANDING TEMPORAL TO LANDING PERSISTANT

# import libraries
import sys
import os
import re
from datetime import datetime as dt

# get the list of datasources to process
sys.path.append('..')
sys.path.append('.')
from helper import *
setwd()
Objects = Objects()

timestamp = dt.now().strftime("%Y%m%d_%H%M%S")

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
            # copy the file with a standarized name
            if "landing_temp_tblID" in datasource:
                tbl_id = re.search(datasource["landing_temp_tblID"], file).group(1) # id of the table within the dataset
                to_file = f"{id}_{tbl_id}_{timestamp}.{format}"
            else:
                to_file = f"{id}_{timestamp}.{format}"

            from_file = os.path.join("data", "landing_temp", file)
            to_file = os.path.join("data", "landing_pers", id, to_file)
            os.system(f"cp {from_file} {to_file}")
            storeLogTemp(file, f)

landingTemp2landingPers()