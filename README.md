# ADSDB_project1

Store the original files in ./data/landing_temp and run the python files in ./scripts to process each step from one zone to the following one. The notebooks in ./notebooks have been used to develope the code.

Or run main.py to orquestrate all steps

The logs of which files are processed are stored in ./logs, so files already processed are not processed again.

## Data

So far we have this: (???)
- eurostat: https://ec.europa.eu/eurostat/databrowser/view/PRC_HICP_MIDX__custom_3484234/default/table?lang=en
- ine:

Objects.json contains the properties of all the objects to be processed. So, to add a new object to the pipeline, it is only needed to add it to Objects.json

Otherwise, the orchestration process in main.py allows to select other configurations



## Landing and formatted zones

Since these two steps are the same for every possible datasource (trusted and explotation zones may require adhoc processing for each datasource), Objects.json contains the properties of all the objects to be processed. So, to add a new object to the pipeline, it is only needed to add it to Objects.json.

### Objects.json structure

- id: identifier of the datasource
- format: extension of the original file in the landing zone. allowed formats so far: csv, xlsx
- (optional) delim: in case of format=csv, the delimiter character
- landing_temp_name: regular expression to identify which files of the landing zone correspond to the datasource
- (optional) landing_temp_tblID: identifier of the table within the dataset