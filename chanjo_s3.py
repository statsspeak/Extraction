import mysql.connector
import pandas as pd
import boto3
from io import StringIO
import os
import logging
import configparser
from datetime import datetime

def execute_data_pipeline():
    logging.basicConfig(filename='data_pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Read configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Read database configuration from config file
    db_host = config.get('DATABASE', 'host')
    db_user = config.get('DATABASE', 'user')
    db_password = config.get('DATABASE', 'password')
    db_name = config.get('DATABASE', 'database')

    # Read S3 configuration from config file
    s3_bucket_name = config.get('S3', 'bucket_name')
    s3_key_prefix = config.get('S3', 'key_prefix')

    # Set batch size
    batch_size = 100000

    # Read SQL queries from file
    with open('queries.sql', 'r') as f:
        queries = f.read().split(';')

    # Connect to database
    with mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
    ) as mydb:
        for query in queries:
            # Ignore empty queries
            if not query.strip():
                continue

            try:
                # Execute query and fetch results in batches
                table_name = query.split('FROM ')[-1].split()[0]
                for df in pd.read_sql(query, mydb, chunksize=batch_size):
                    # Write data to memory as CSV
                    csv_buffer = StringIO()
                    df.to_csv(csv_buffer, index=False)

                    # Upload CSV to S3
                    s3 = boto3.resource('s3')
                    s3.Object(s3_bucket_name, f"{s3_key_prefix}/{table_name}.csv").put(Body=csv_buffer.getvalue())

                logging.info(f"Data pipeline for table {table_name} executed successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            except Exception as e:
                logging.error(f"Error executing query {query}: {str(e)}")

def main():
    execute_data_pipeline()

if __name__ == '__main__':
    main()
