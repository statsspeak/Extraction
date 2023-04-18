# Data Pipeline for SQL Query Results to S3

This code is a Python script that executes a data pipeline for transferring SQL query results from a MySQL database to Amazon S3. The script reads SQL queries from a file and executes them against the specified MySQL database. The results of each query are then fetched in batches and uploaded to an S3 bucket in CSV format.

## Dependencies

This script requires the following Python libraries:

- `mysql-connector`: To connect to the MySQL database and execute queries.
- `pandas`: To fetch query results in batches and write to CSV format.
- `boto3`: To upload the CSV files to S3.
- `configparser`: To read the configuration file.
- `logging`: To log pipeline execution details.
- `datetime`: To record execution timestamps.

## Configuration

This script requires a configuration file named `config.ini` in the same directory as the script.


## Execution

To execute the script, simply run it using Python. The script will read the SQL queries from `queries.sql` file in the same directory and execute them against the MySQL database specified in the `config.ini` file. The script will then fetch the results in batches and upload them to the specified S3 bucket in CSV format.

The execution log will be written to `data_pipeline.log` file in the same directory.


