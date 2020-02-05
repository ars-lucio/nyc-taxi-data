import unittest
from socrata_dataframe import SocrataDataframe
from sodapy import Socrata


class TestDataframe(unittest.TestCase):
    def test_row_length(self):
        limit_rows = 200
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
        limit {limit_rows}'''
        result_dataframe = socrata_dataframe.get_dataframe_query(query_text)
        self.assertEqual(len(result_dataframe), limit_rows, "Should be same number of rows")

    def test_columns(self):
        socrata_dataframe = SocrataDataframe('data.cityofnewyork.us', 't29m-gskq', 1000)
        metadata = client = Socrata('data.cityofnewyork.us', None).get_metadata('t29m-gskq')
        columns = [x['name'] for x in metadata['columns']]
        self.assertEqual(columns, socrata_dataframe.columns, "Columns should be equal")






if __name__ == '__main__':
    unittest.main()
