import mysql.connector
import pandas as pd
import boto3
from io import StringIO
import os
import logging
import configparser
from datetime import datetime

logging.basicConfig(filename='data_pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Read configuration file
config = configparser.ConfigParser()
with open('config.ini') as f:
    config.read_file(f)

# Read database configuration from config file
db_host = config.get('DATABASE', 'host')
db_user = config.get('DATABASE', 'user')
db_password = config.get('DATABASE', 'password')
db_name = config.get('DATABASE', 'database')

# Read S3 configuration from config file
s3_bucket_name = config.get('S3', 'bucket_name')
s3_key_prefix = config.get('S3', 'key_prefix')

# Read AWS access keys from environment variables
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

# Set batch size
batch_size = 100000

def main():
    try:
        # Connect to database
        with mysql.connector.connect(
                host=db_host,
                user=db_user,
                password=db_password,
                database=db_name
        ) as mydb:
            # Process data in batches
            for df in pd.read_sql('SELECT * FROM transactions', mydb, chunksize=batch_size):
                # Write data to memory as CSV
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)

                # Upload CSV to S3
                s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
                s3.Object(s3_bucket_name, f"{s3_key_prefix}/transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv").put(Body=csv_buffer.getvalue())

        logging.info(f"Data pipeline executed successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except Exception as e:
        logging.error(f"Error: {str(e)}")

if __name__ == '__main__':
    main()