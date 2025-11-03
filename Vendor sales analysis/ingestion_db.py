import pandas as pd
from sqlalchemy import create_engine
import time
import os
import logging

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(filename)s - %(message)s",
    filemode='a'
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    """this function will ingest dataframe into databse table"""
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

def load_raw_data():
    start=time.time()
    for file in os.listdir('data'):
        """this Function will load CSVs as dataframe and ingest into db"""
        if '.csv' in file:
            df = pd.read_csv('data/'+file)
            logging.info(f"ingesting {file} in db")
            ingest_db(df, file[:-4], engine)
        end=time.time()
        total_time_taken=(end-start)/60
        logging.info('INGESTION COMPLETE')

        logging.info(f'\nTotal time taken: {total_time_taken}minute')

if __name__=='__main__':
    load_raw_data()