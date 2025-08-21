import sys 
import logging
import traceback
import pandas as pd
import requests
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) # Always load .env from the script folder so that relative paths work even when called by a scheduler

load_dotenv(os.path.join(BASE_DIR, ".env")) #load secrets and config from .env

# File logger for scheduler runs
LOG_PATH = os.path.join(BASE_DIR, "etl.log") #set log file path that is stable regardless of where the script runs from
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s" #write information into the .logs
)
# read configuration from environment
url = os.getenv("API_URL")
api_key = os.getenv("API_KEY")
username = os.getenv("DB_USERNAME")
password = os.getenv("PASSWORD")
host = os.getenv("DB_HOST")
port = 5432
database = os.getenv("DB_NAME")

headers = {
    "accept": "application/json",
    "X-Api-Key": api_key,
}

def extract_data(url):
    '''this function gets a response from a url and returns a dataframe'''
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        df = pd.json_normalize(data)
        return df
    else:
        raise Exception(f"Error fetching data: {response.status_code}- {response.text}")
df = extract_data(url)

#load raw data to DB
engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')
df.to_sql('raw_listing', engine, if_exists='replace', index=False)

def transform_data(df):
    listing_data_cleaned = df[['id', 'formattedAddress', 'city',
       'state', 'zipCode', 'county', 'latitude', 'longitude', 'propertyType',
       'bedrooms', 'bathrooms', 'squareFootage', 'lotSize', 'yearBuilt',
       'status', 'price', 'listingType', 'listedDate', 'removedDate',
       'createdDate', 'lastSeenDate', 'daysOnMarket', 'mlsName', 'mlsNumber',
       'listingAgent.name', 'listingAgent.phone', 'listingAgent.email',
       'listingAgent.website', 'listingOffice.name', 'listingOffice.phone',
       'listingOffice.email', 'listingOffice.website','history.2025-03-31.event', 
       'history.2025-03-31.price', 'history.2025-03-31.listingType','history.2025-03-31.listedDate', 
       'history.2025-03-31.removedDate', 'history.2025-03-31.daysOnMarket']].copy()
    listing_data_cleaned.rename(columns={
        'formattedAddress': 'full_address','zipCode': 'postal_code'}, inplace=True
        )
    listing_data_cleaned.columns= (
        listing_data_cleaned.columns
        .str.strip()
        .str.replace(r'([A-Z])', r'_\1', regex=True)
        .str.replace(" ", "_")
        .str.replace(r'[^0-9a-zA-Z_]', r'_', regex=True)
        .str.lower()
        .str.replace('__', '_')
        .str.strip('_')
        )
    return listing_data_cleaned
listing_data_cleaned = transform_data(df)

#load transformed data to DB
engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')
listing_data_cleaned.to_sql('transformed_listing', engine, if_exists='replace', index=False)

def main() -> int:
    logging.info("ETL start")
    try:
        df = extract_data(url)
        df.to_sql("raw_listing", engine, if_exists="replace", index=False)

        df_list_clean = transform_data(df)
        df_list_clean.to_sql("transformed_listing", engine, if_exists="replace", index=False)

        logging.info("Rows: raw=%s clean=%s", len(df), len(df_list_clean))
        logging.info("ETL success")
        return 0
    except Exception:
        logging.error("ETL failed\n%s", traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main())