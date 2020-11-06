from sqlalchemy import create_engine
import pymysql
import pandas as pd
from google.cloud import bigquery
import yaml

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("config", help="Path to configuration file", type=str)
args = parser.parse_args()
config_file = args.config

# Read configuration file
print('INFO: Reading configuration file from: \n\t {0}'.format(config_file))

with open(config_file) as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
    # print(config)

# Getting configuration from file
print('INFO: Getting configuration from file')
# db
db_user = config['source-db']['user']
db_pass = config['source-db']['pass']
db_host = config['source-db']['host']
db_port = config['source-db']['port']
db_name = config['source-db']['db']
# target table
bq_project = config['target-table']['project']
bq_dataset = config['target-table']['dataset']
bq_tableid = config['target-table']['table']
# query file
query_file_path = config['query-file']
# temporary local storage path
temp_storage_path = config['temp-storage']

# Getting query from file
print('INFO: Getting query from file: \n\t {0}'.format(query_file_path))
query_file = open(query_file_path,"r") 
query = ''.join(query_file.readlines())

print('INFO: Setting database connection')
# Setting up connection with sql alchemy
db_connection_str = 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}'.format(db_user, db_pass, db_host, db_port, db_name)
db_connection = create_engine(db_connection_str)

# Reading data
print(f'INFO: Reading data from: \n\t {db_connection_str}')
df = pd.read_sql(query, con=db_connection)

print('INFO: Data sample')
print(df.head())
print('INFO: Data description')
print(df.describe())
print('INFO: Data types')
print(df.dtypes)

#### Store locally as parquet + snappy and then upload to BQ ####
print('INFO: Storing data in parquet format to: \n\t {0}'.format(temp_storage_path))
df.to_parquet(temp_storage_path, compression='snappy')


client = bigquery.Client()
table_id = '{0}.{1}.{2}'.format(bq_project, bq_dataset, bq_tableid)
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
)

# Read table from local parquet
with open(temp_storage_path, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

job.result()  # Waits for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)



#################################################################
####                          OPTION 3                       ####
####      Store local as parquet + snappy -> GCS -> BQ       ####
# client = bigquery.Client()
# table_id = "<project>.<dataset>.<table>"
# OUTPUT_PATH='/tmp/example.parquet'
# df.to_parquet(OUTPUT_PATH, compression='snappy')

# # Upload data to GCS
# bucket_name = "<bucket>"
# destination_blob_name = "<storage-object-name>"

# storage_client = storage.Client()
# bucket = storage_client.bucket(bucket_name)
# blob = bucket.blob(destination_blob_name)
# blob.upload_from_filename(OUTPUT_PATH)

# # Read parquet from GS
# job_config = bigquery.LoadJobConfig(
#     source_format=bigquery.SourceFormat.PARQUET,
#     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
# )

# uri = f"gs://{bucket_name}/{destination_blob_name}"
# job = client.load_table_from_uri(
#     uri, table_id, job_config=job_config
# ) 

# job.result()  # Waits for the job to complete.

# table = client.get_table(table_id)  # Make an API request.
# print(
#     "Loaded {} rows and {} columns to {}".format(
#         table.num_rows, len(table.schema), table_id
#     )
# )



