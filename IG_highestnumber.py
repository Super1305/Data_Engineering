"""
This is plugin: Get 3 top locations by the number of residents
"""

from airflow import AirflowException
from airflow.models.baseoperator import BaseOperator
import logging
import requests
import pandas as pd


class HighestNumbersOperator(BaseOperator):

    def __init__(self, path: str = '/tmp/top.csv', **kwargs) -> None:
        super().__init__(**kwargs)
        self.path = path

    def get_top_location(self):
        z = []
        for i in range(1, 127):
            api_url = 'https://rickandmortyapi.com/api/location/' + str(i)
            r = requests.get(api_url)
            if r.status_code == 200:
                logging.info("OK")
                z.append([i, r.json().get('name'), r.json().get('type'), r.json().get('dimension'),
                          len(r.json().get('residents'))])
            else:
                    logging.warning("HTTP STATUS {}".format(r.status_code))
                    raise AirflowException('Error while loading api_url')
        df = pd.DataFrame(z, columns=['id', 'name', 'type', 'dimension', 'resident_cnt']). \
           sort_values('resident_cnt', ascending=False).head(3)
        df[['id', 'resident_cnt']] = df[['id', 'resident_cnt']].astype('uint8')
        df.drop_duplicates(subset=['name'], inplace=True)
        logging.info(df)
        return df

    def execute(self, context):
        df2 = self.get_top_location()
        df2.to_csv(self.path, sep=',', encoding='utf-8', index=False, header=False)