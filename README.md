# nyc-taxi-data
nyc open taxi data
# Readme
This solution implements sodapy Socrata module for fetching the Taxi Rides data from NYC Open Data site
(https://data.cityofnewyork.us/Transportation/2018-Yellow-Taxi-Trip-Data/t29m-gskq/data)

it makes use of two built in classes SocrataDataframe and DataBase
in the SocrataDataframe.py file we see a column_dict which is built for this specific dataset

### dataframe_to_db.py
This file sends the resulting dataframe to a table in the db (sqlite3)
we can pass three arguments from terminal
*-db (path for the database)
*-t or --table (table name)
*-s or --start-date (start date to process in format year-month-day)
*-e or --end-date (end date to process in format year-month-day)
*-l 0r --limit (limit the number of rows)
#### example run
```python
python dataframe_to_db.py -db ./taxi.db -t taxi2018 -s 2018-01-01 -e 2018-12-31 -l 1000
```
### explore.py
This file is more flexible and allow us to edit the query_text for the Socrata API and get the result in a dataframe wich can later be processed and store the results
