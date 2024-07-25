import pandas as pd
import unicodedata as ud
from sqlalchemy.engine import URL, create_engine
from sqlalchemy.sql import text
from dotenv import load_dotenv
import os
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(filename='etl.log', encoding='utf-8', level=logging.DEBUG)

load_dotenv(override=True)  # take environment variables from .env.

# Transformations functions
def encoding_transformation(s):
    """
    Handle encoding issues.
    inputs:
        s (str) -> bad encoding/decoding string. i.e  -> ArizÃ³na
    outputs:
        utf8_s -> well decoded string. i.e -> Arizona
    """
    s = str(s).encode('ISO-8859-1').decode()
    n_s = ud.normalize('NFKD', s)
    r_a = n_s.encode('ascii', errors='ignore')
    utf8_s = r_a.decode('utf-8').lower().capitalize()
    return utf8_s

def gender_transformation(s):
    """
    Gender number standards.
        The four codes specified in ISO/IEC 5218 are:
        0 = Not known;
        1 = Male;
        2 = Female;
        9 = Not applicable.
    inputs:
        s (str) -> 1,2,male,fema
    outputs
        'Male' or 'Female' or 'Not known'
    """
    s = str(s).lower()
    if s in ('1', 'male'):
        return 'Male'
    elif s in ('2', 'fema'):
        return 'Female'
    else:
        return 'Not known'
    
def transformation_gender(df, col):
    logger.info('Applying gender standarization.')
    df[col] = df[col].apply(gender_transformation)
    return df

def transformation_dates(df, col):
    logger.info('Parsing multiple date formats.')
    df[col] = pd.to_datetime(df[col])
    df[col] = df[col].dt.strftime('%Y-%m-%d')
    return df

def transformation_encoding(df, col):
    logger.info('Parsing encoding issues.')
    df[col] = df[col].apply(encoding_transformation)
    return df

# ETL functions
def extract(filepath) -> pd.DataFrame:
    return pd.read_excel(filepath)

def transform(df) -> pd.DataFrame:
    curated_df = (df
              .pipe(transformation_dates, col='day_of_birthday')
              .pipe(transformation_encoding, col='state')
              .pipe(transformation_gender, col='gender')
              )
    return curated_df

def load(df, tablename, schema) -> bool:
    conn_url = URL.create("mssql+pyodbc", query={"odbc_connect": os.getenv('conn_str')})
    engine = create_engine(conn_url)
    with engine.connect() as connection:
        connection.execute(text(f'TRUNCATE TABLE {schema}.{tablename}'))
        result = df.to_sql(tablename,con=connection, schema=schema, if_exists='append', chunksize=5000, index=False)
    return result != None

# Main function
def run():
    logger.info('Starting data extraction.')
    df = extract('data/raw/Case_study_health_care.xlsx')
    logger.info('Data successfully readed from Excel file.')
    logger.info('Applying data transformations.')
    transformed_df = transform(df)
    logger.info('Starting data load')
    data_load = load(transformed_df, 'health_data', 'healthcare')
    if data_load:
        logger.info('Data loaded correctly.')
    else:
        logger.error('Data error loading.')


if __name__ == '__main__':
    run()