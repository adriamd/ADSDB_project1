import sys
import helper
from scripts.formatted import LandingPers2Formatted
from scripts.landing import landingTemp2landingPers
from scripts.trusted import formatted2trusted

# from exploitation import *
# eda?

import os

def reset_landing():
    # remove files
    datasets = os.listdir('data/landing_pers')
    for ds in datasets:
        if not ds==".keep":
            for f in os.listdir(os.path.join('data/landing_pers', ds)):
                os.remove(os.path.join('data/landing_pers', ds, f))
    # reset log
    open('logs/processed_data_temporal.txt', 'w').close()

def reset_formatted():
    # remove files
    datasets = os.listdir('data/formatted')
    for ds in datasets:
        if not ds==".keep":
            os.remove(os.path.join('data/formatted', ds))
    # reset log
    open('logs/processed_data_persistant.txt', 'w').close()

def reset_trusted():
    # remove files
    datasets = os.listdir('data/trusted')
    for ds in datasets:
        if not ds==".keep":
            os.remove(os.path.join('data/trusted', ds))
    # reset log
    open('logs/processed_data_trusted.txt', 'w').close()

def reset_exploitation():
    os.remove('data/exploitation/exploitation.db')

def read_datasets_conf():
    keep_reading = True
    conf = []
    while(keep_reading):
        newconf = {}
        id = input("Enter dataset id or click 0 to finish the process\n")
        if id == "0":
            keep_reading = False
        else:
            newconf['id'] = id
            format = input("Select a file format. Allowed formats: [1] CSV\n")
            format = {"1":"csv"}[format]
            newconf['format'] = format
            if format == "csv":
                delim = input("Enter the CSV delimiter\n")
                newconf['delim'] = delim
            landing_temp_name = input("Enter a regular expression for the files in temporal zone\n")
            newconf['landing_temp_name'] = landing_temp_name
            landing_temp_tblID = input(f"Enter a regular expression to get the ID of each table within {id} dataset\n")
            newconf['landing_temp_tblID'] = landing_temp_tblID
            conf.append(newconf)

    save_conf = input("Save selected configuration? Y/N\n")
    if save_conf.lower() == "y":
        conf_name = input("Enter new configuration name: ")
        with open(conf_name + '.json', "w") as f:
            f.write(str(conf).replace("'", '"'))
        print(conf_name + '.json saved')
    return conf


def select_action():
    x = input("Select an option:\n[1] Run zones\n[2] Reset a zone\n[3] Exit\n")
    if x == "3": return
    if x == "1":
        y = input("Select list of datasets:\n[1] Use default datasets\n[2] Use a json file with datasets\n[3] Enter custom configuration\n")
        if y == "1":
            Objects = helper.Objects()
        if y == "2":
            Objects = helper.Objects(filename = input("filename: "))
        if y == "3":
            #print("Option in development. Using the default configuration.") # TODO
            Objects = read_datasets_conf()
            #Objects = helper.Objects()

        select_process(Objects)
    if x == "2":
        select_reset_zone()

def select_process(Objects):
    z = input("Select process to execute:\n[1] Landing temp -> Landing pers\n[2] Landing pers -> Formatted\n[3] Formatted -> Trusted\n[4] Trusted -> Exploitation\n[5] Run all\n[6] Go back\n")
    if z == "1":
        landingTemp2landingPers(Objects)
        print("done!")
        return select_process(Objects)
    if z == "2":
        LandingPers2Formatted(Objects)
        print("done!")
        return select_process(Objects)
    if z == "3":
        formatted2trusted(Objects)
        print("done!")
        return select_process(Objects)
    if z == "4":
        print("TRUSTED TO EXPLOITATION NOT READY YET\n") #TODO
        # estaria guai passarli parametres tipus: distancia per utilitzar en les queries, limit de files en la query
        return select_process(Objects)
    if z == 5:
        landingTemp2landingPers(Objects)
        LandingPers2Formatted(Objects)
        formatted2trusted(Objects)
        print("falta la exploitation\n") # TODO
        return select_action(Objects)
    if z == "6":
        return select_action()

def select_reset_zone():
    x = input("Select zone\n[1] Landing persistant\n[2] Formatted zone\n[3] Trusted zone\n[4] Exploitation zone\n[5] All zones\n[6] Go back\n")
    if x == "1":
        reset_landing()
        print("Landing zone has been reset")
        return select_reset_zone()
    if x == "2":
        reset_formatted()
        print("Formatted zone has been reset")
        return select_reset_zone()
    if x == "3":
        reset_trusted()
        print("Trusted zone has been reset")
        return select_reset_zone()
    if x == "4":
        reset_exploitation()
        print("Exploitation zone has been reset")
        return select_reset_zone()
    if x == "5":
        reset_landing()
        reset_formatted()
        reset_trusted()
        reset_exploitation()
        print("All zones have been reset")
        return select_action()
    if x == "6":
        return select_action()




if __name__ == "__main__":
    select_action()
    