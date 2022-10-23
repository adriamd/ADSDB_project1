# MOVE THE TABLES FROM LANDING PERSISTANT TO FORMATTED
# CHANGE THE FORMAT OF THE TABLES TO DUCKDB

# import libraries
import sys
import os
import duckdb

def StoreLogPers(tbl, file):
    file.write(tbl + "\n")


# it stores the csv tables in the landing zone in a duckdb database
def ProcessCSV(Datasource, Connection, Files, LogWriter):

    id = Datasource["id"]
    # csv delimiter
    delim = Datasource["delim"] if "delim" in Datasource else ","
    
    for file in Files:
        tablename = file.split(".")[0]
        Connection.execute(f"""
            CREATE TABLE test_{tablename}
            AS SELECT * FROM read_csv_auto(
                './data/landing_pers/{id}/{file}',
                HEADER = TRUE,
                SAMPLE_SIZE=-1,
                DELIM = '{delim}'
            ) 
            """)
        StoreLogPers(file, LogWriter)
    

def LandingPers2Formatted(Objects):

    # list of files already processed:
    processedFiles = []
    with open('logs/processed_data_persistant.txt', "r") as f0:
        for line in f0:
            processedFiles.append(line.rstrip())

    # open file to store logs in append mode
    f = open('logs/processed_data_persistant.txt', "a")

    # process datasets:
    for datasource in Objects:
        id = datasource["id"]
        format = datasource["format"]

        # files to process:
        files = [file for file in os.listdir(f'data/landing_pers/{id}') if not file in processedFiles]
        if len(files) == 0:
            continue

        # open database connection
        con = duckdb.connect(database=f"data/formatted/db_{id}.db", read_only=False)
        
        if format == "csv":
            ProcessCSV(datasource, con, files, f)
        #elif format == "xlsx":
        #    ProcessXLSX(datasource, con, files, f)
        else:
            print(f"Format {format} is not supported\nSupported formats: csv")

        # close database
        con.close()

    f.close()


if __name__ == "__main__":
    sys.path.append('..')
    sys.path.append('.')
    from helper import *

    setwd()
    Objects = Objects()
    LandingPers2Formatted(Objects)
