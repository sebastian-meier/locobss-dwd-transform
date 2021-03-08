# locobss-dwd-transform
Transform DWD data for customized story-telling

## Overview
This set of scripts:
- Creates a set of postgresql tables. 
- Creates a spatial grid based on the weather data
- Imports historic data into the database
- Queries polygons agains the database and receives a timeseries dataset with (min/max/average@timestamp)

## Preparations
- Install dependencies through requirements.txt (there is also a yml file for a conda environment)
- Create .env file (see .env-sample)

## Process

### Creating the postgresql tables
```
python setup.py
```

### Download data from the German Weather Service (DWD)
There are lots of different data sets on the dwd server, this script has been tested with the following folders: drought_index, frost_days, hot_days, ice_days, precipitation, precipGE30mm_days, snowcover_days, summer_days
You can download the data easily via FTP: opendata.dwd.de (no login required), PATH: /climate_environment/CDC/grids_germany/annual/

### Create the spatial grid (data needs to be in the INPUT folder)
Provide the path to one of the gzipped asc files as the base for the grid
```
python setup.py PATH_TO_FILE.asc.gz
```

### Import the data
Set the INPUT_PATH to point to your folder containing the DWD data. Provide a comma separated list of data sets you want to import
```
python import.py drought_index,frost_days
```



## Data Source @ DWD
https://opendata.dwd.de/climate_environment/CDC/grids_germany/annual/

### Simplification of timeseries

```bash
geo2topo timeseries.geojson > timeseries.topo.json
toposimplify -p 0.00000001 -F timeseries.topo.json -o timeseries.simple.topo.json
topoquantize 1e5 timeseries.simple.topo > timeseries.simple2.topo.json
```