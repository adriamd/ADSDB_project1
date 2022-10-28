# ADSDB_project1

## Context 
For this project, we chose to focus on the price of renting houses in the United States.

To carry out this project we chose as the main dataset one of US houses, that give us some characteristics like square feet, number of beds and its price (rent per month). In order to complement this information we select two additional datasets. The first one is about hospitals in the US and the second one is about schools.These two datasets will enrich the houses datasets giving how many hospitals and schools are near one house, also give us some characteristics like the type of hospital (general, psychiatric, long term care, etc.), the owner (governmental, proprietary, non-profit, etc.) or in the case of schools if is preschool, elementary, middle or high school.

The analytical question is if we can calculate the rent per month of one house based on knowledge of the house, square feet of house, number of beds, baths, etc. and the proximity of hospitals and schools.

## Development & Operations

To run the code, one needs first to store the initial data in the temporal landing zone. The initial data can be found in this Google Drive folder and the temporal landing zone is the directory data/landing_temp in the Github repository.

Once the initial data is in the temporal landing zone, running either the operations scripts in the folder scripts or the orchestration file main.py will transform the data across the different zones,  represented by the various subdirectories in data.

The complete joint in the exploitation zone takes a long time (1h), so it is better to limit the rows used (1000 rows) for a quick test.

The repository also includes a virtual environment (in the folder venv) with the required Python libraries and versions needed.
