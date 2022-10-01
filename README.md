# ADSDB_project1

run the python files in ./scripts to process each step from one zone to the following. the notebooks in ./notebooks have been used to develop the code

(maybe it could be useful to have a main.py file that runs all the zones?)

Objects.json contains the properties of all the objects to be processed. So, to add a new object to the pipeline, it is only needed to add it to Objects.json

The logs of which files are processed can be stored in ./logs




data:

- eurostat: https://ec.europa.eu/eurostat/databrowser/view/PRC_HICP_MIDX__custom_3484234/default/table?lang=en
- ine:


## Objects.json structure

- id: identifier of the datasource
- format: extension of the original file in the landing zone. allowed formats so far: csv, xlsx
- delim: in case of format=csv, the delimiter character
- landing_temp_name: regular expression to identify which files of the landing zone correspond to the datasource
- (optional) landing_temp_tblID: identifier of the table within the dataset