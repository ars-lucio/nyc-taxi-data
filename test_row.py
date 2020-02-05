import pytest
import datetime
from datetime import date, timedelta
import pandas as pd
from pandas.io import sql
from socrata_dataframe import SocrataDataframe
from database_conn import DataBase


def test_row_length(num):
    socrata_dataframe = SocrataDataframe('data.cityofnewyork.us', 't29m-gskq', 1000)
    query_text = f'''select VendorID
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
    limit {num}'''
    result_dataframe = socrata_dataframe.get_dataframe_query(query_text)
    assert len(result_dataframe) == num
