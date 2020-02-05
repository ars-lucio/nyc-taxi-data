from sodapy import Socrata
import pandas as pd

column_dict = {'VendorID':'int', 'tpep_pickup_datetime':'date', 'tpep_dropoff_datetime':'date', 'passenger_count':'int', 'trip_distance':'float', 'RatecodeID':'int', 'store_and_fwd_flag': None, 'PULocationID':'int', 'DOLocationID':'int', 'payment_type':'int', 'fare_amount':'float', 'extra':'float', 'mta_tax':'float', 'tip_amount':'float', 'tolls_amount':'float', 'improvement_surcharge':'float', 'total_amount':'float'}
def convert_dataframe(dataframe,column_dict):
    for i in dataframe.columns:
        if column_dict[i] == None:
            pass
        elif column_dict[i] == 'date':
            dataframe[i] = pd.to_datetime(dataframe[i])
        elif (column_dict[i] == 'float') or (column_dict[i] == 'int'):
            dataframe[i] = dataframe[i].astype(column_dict[i])
    return dataframe


class SocrataDataframe():
    def __init__(self,socrata_domain,dataset,timeout=60,limit=200,socrata_token=None):
        self.socrata_domain = socrata_domain
        self.dataset = dataset
        self.socrata_token = socrata_token
        #self.socrata_token = os.environ["SOCRATA_TOKEN"]
        self.timeout = timeout
        self.limit = limit
        self.client = self.get_client()
        self.metadata = self.get_metadata()
        self.columns = self.get_columns()

    def get_client(self):
        client = Socrata(self.socrata_domain, self.socrata_token)
        client.timeout = self.timeout
        return client

    def get_dataframe(self):
        results = self.client.get(self.dataset, limit=self.limit)
        results_df = pd.DataFrame.from_records(results)
        return convert_dataframe(results_df,column_dict)

    def get_dataframe_query(self,query_text):
        results = self.client.get(self.dataset, query=query_text)
        results_df = pd.DataFrame.from_records(results)
        return convert_dataframe(results_df,column_dict)

    def get_metadata(self):
        metadata = self.client.get_metadata(self.dataset)
        return metadata


    def get_columns(self):
        return [x['name'] for x in self.metadata['columns']]
