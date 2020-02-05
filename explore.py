import datetime
from datetime import date, timedelta
import pandas as pd
from pandas.io import sql
from socrata_dataframe import SocrataDataframe
from database_conn import DataBase

socrata_dataframe = SocrataDataframe('data.cityofnewyork.us','t29m-gskq',1000)
query_text = '''select VendorID
,tpep_pickup_datetime
,tpep_dropoff_datetime
,passenger_count
,trip_distance
,RatecodeID
,store_and_fwd_flag
,PULocationID
,DOLocationID
,payment_type
,fare_amount
,extra
,mta_tax
,tip_amount
,tolls_amount
,improvement_surcharge
,total_amount
where total_amount > 10
limit 1000'''

# get dataframe
result_dataframe = socrata_dataframe.get_dataframe_query(query_text)
print(result_dataframe.dtypes)

#value_counts for passenger_count column
print(result_dataframe['passenger_count'].value_counts())

#add new column trip_duration
result_dataframe["trip_duration"] = result_dataframe["tpep_dropoff_datetime"] - result_dataframe["tpep_pickup_datetime"]

#sort by trip_duration
sorted = result_dataframe.sort_values("trip_duration",ascending=True)
print(sorted)

#query trip_duration > 1 hour
print(result_dataframe.query('trip_duration > datetime.timedelta(hours=1)'))
print(result_dataframe.loc[result_dataframe["trip_duration"] >= datetime.timedelta(hours=1)])

#group by passenger_count
print(result_dataframe.groupby('passenger_count')['trip_distance', 'total_amount'].mean())
