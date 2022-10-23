# here useful functions that can be used accross different steps

import json
import os

# return a dictionary with the list of
# datasets to be processed and all the
# parameters needed for the preprocessing
def Objects(filename = 'Objects.json'):
    with open(filename) as f:
        Object = json.load(f)
    return Object

# set the working directory to the root of the project
# otherwise it would use the folder of the python file
def setwd():
    if os.getcwd().replace("\\", "/").split("/")[-1] in ["notebooks", "scripts"]:
        os.chdir("..")