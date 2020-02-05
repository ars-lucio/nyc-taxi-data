import click
import datetime
from datetime import date, timedelta
import pandas as pd
from pandas.io import sql
from SocrataDataframe import SocrataDataframe
from DataBase import DataBase

#python dataframe_to_db.py -db ./taxi.db -t taxi2018 -s 2018-01-01 -e 2018-12-31 -l 1000

@click.command()
@click.option("--db", "-db", "db_file", required=True,
    help="Path to db",
    type=click.Path(exists=True, dir_okay=False, readable=True),
)
@click.option("--table","-t","table_name",
    help="Start Date",
    )
@click.option("--start-date","-s","start_date",
    help="Start Date",
    )

@click.option("--end-date","-e","end_date",
    help="End Date",
    )
@click.option("--limit","-l","limit",
    help="limit of rows",
    )


def main(db_file,table_name,start_date,end_date,limit):
    socrata_dataframe = SocrataDataframe('data.cityofnewyork.us','t29m-gskq',1000)
    dt_start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    dt_end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
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
    where tpep_pickup_datetime BETWEEN {dt_start_date:'%Y-%m-%dT%H:%M:%S.%f'} AND {dt_end_date:'%Y-%m-%dT%H:%M:%S.%f'}
    limit {limit}'''
    df = socrata_dataframe.get_dataframe_query(query_text)
    selected = df[['total_amount','passenger_count','trip_distance','tpep_pickup_datetime','tpep_dropoff_datetime','PULocationID','DOLocationID']]

    db = DataBase(db_file)
    table_taxi = f'''CREATE TABLE IF NOT EXISTS {table_name}
             (
             total_amount           REAL    NOT NULL,
             passenger_count        INT   NOT NULL,
             trip_distance          REAL     NOT NULL,
             tpep_pickup_datetime           timestamp,
             tpep_dropoff_datetime          timestamp,
             PULocationID          INT,
             DOLocationID          INT);'''

    db.run_string(table_taxi)
    print(db.run_string("SELECT name FROM sqlite_master WHERE type='table';"))
    selected.to_sql(table_name, db.conn,if_exists='append', index = False)
    table = pd.read_sql_query(f"SELECT * from {table_name}", db.conn)
    print(table)



if __name__ == "__main__":
    main()
