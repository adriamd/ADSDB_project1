# here useful functions that can be used accross different steps

import json
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

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

# input: dataframe
# output: statistics of the numerical features from the dataframe
def numeric_description(df):
    df_numeric_description = df.describe().transpose()
    df_numeric_description["missings"] = len(df) - df_numeric_description["count"]
    #df.shape #len df
    df_numeric_description["missing ratio (%)"] = round(df_numeric_description["missings"]*100/len(df), 2)
    return df_numeric_description.drop(['count'], axis=1)

# input: dataframe
# output: histograms of the numerical features from the dataframe
def hist(df, bins=50):
    name_columns = list(df.columns)
    types_columns = [str(x) for x in list(df.dtypes)]
    columnss = [(name_columns[i],types_columns[i]) for i in range(len(name_columns))]
    
    for i,t in columnss:

        if t != "float64" and t != "int32":
            continue

        df[i].hist(bins=bins)
        plt.title(i)
        plt.show()

def boxplot(df):
    name_columns = list(df.columns)
    types_columns = [str(x) for x in list(df.dtypes)]
    columnss = [(name_columns[i],types_columns[i]) for i in range(len(name_columns))]
    
    for i,t in columnss:

        if t != "float64" and t != "int32":
            continue

        df.loc[:, [i]].boxplot();
        plt.title(i)
        plt.show()

def correlation(df):

    corr = df.corr()
    sns.heatmap(corr, vmax=.3, center=0,
                square=True, linewidths=.5, cbar_kws={"shrink": .5})

def barplot(df,top=10,freq=False):
    n = len(df)
    name_columns = list(df.columns)
    types_columns = [str(x) for x in list(df.dtypes)]
    columns = [(name_columns[i],types_columns[i]) for i in range(len(name_columns))]
    

    for i,t in columns:

        if t != "object":
            continue

        if freq:
            df[i].value_counts().head(top).apply(lambda x : x/n).plot( kind='bar')
        else:
            df[i].value_counts().head(top).plot( kind='bar')
        plt.title(i)
        plt.show()


def description_categorical(df):
    n = len(df)
    name_columns = list(df.columns)
    types_columns = [str(x) for x in list(df.dtypes)]
    columns = [(name_columns[i],types_columns[i]) for i in range(len(name_columns))]
    
    cat_dict = dict({"": [],"#Levels":[],"Max freq":[],"Min freq":[],"#Unknows":[],"Unknows (%)":[]})

    for i,t in columns:

        if t != "object":
            continue

        freq = dict(df[i].apply(lambda x: str(x)).value_counts())
        freq_list = [(y,x) for x,y in freq.items()]

        max_freq = max(freq_list)[1] + " (" + str(max(freq_list)[0]) 
        max_freq += (", " + str(round(max(freq_list)[0]*100/n,2)) + ")")

        min_freq = min(freq_list)[1] + " (" + str(min(freq_list)[0]) 
        min_freq += (", " + str(round(min(freq_list)[0]*100/n,2)) + ")")

        cat_dict[""].append(i)
        cat_dict["#Levels"].append(len(freq_list))
        cat_dict["Max freq"].append(max_freq)
        cat_dict["Min freq"].append(min_freq)
        
        if "unknow" in freq:
            cat_dict["#Unknows"].append(freq["unknow"])
            cat_dict["Unknows (%)"].append(round(freq["unknow"]*100 /n,2))

        else:
            cat_dict["#Unknows"].append(0)
            cat_dict["Unknows (%)"].append(0)

    return pd.DataFrame(cat_dict)