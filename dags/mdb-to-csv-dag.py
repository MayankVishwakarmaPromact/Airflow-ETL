import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# File paths constants
DATA_DIR = '/opt/airflow/data/'
POSTGRES_CONN_ID = 'postgresql://airflow:airflow@postgres:5432/mdbfiles'  # Connection ID defined in Airflow

def list_mdb_files():
    """List all .mdb files in the data directory."""
    # print(os.listdir(DATA_DIR))
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.mdb')]
    return files

def extract_table_data(mdb_file, table_name):
    """Extract data from .mdb file and convert it to CSV."""
    csv_file_path = os.path.join(DATA_DIR, f"{mdb_file}_{table_name}.csv")
    command = f"mdb-export {os.path.join(DATA_DIR, mdb_file)} {table_name} > {csv_file_path}"
    os.popen(command)

def read_each_mdb_file(**kwargs):
    """Read all .mdb files in the data directory and extract tables."""
    mdb_files = kwargs['ti'].xcom_pull(task_ids='list_mdb_files')
    for mdb_file in mdb_files:
        command = f"mdb-tables -1 {os.path.join(DATA_DIR, mdb_file)}"
        result = os.popen(command).read().strip().split('\n')
        tables = [table for table in result if table]
        for table in tables:
            extract_table_data(mdb_file, table)

def process_csv_to_postgres():
    """Process CSV files and load them into the PostgreSQL database."""
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("PySpark_MDB_Processing") \
        .getOrCreate()

    # Process all CSV files in the data directory
    csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    for csv_file in csv_files:
        df = spark.read.csv(os.path.join(DATA_DIR, csv_file), header=True, inferSchema=True)
        # Add logic to save DataFrame to PostgreSQL if needed
        pandas_df = df.toPandas()
        pandas_df.to_sql(csv_file, con=create_engine(POSTGRES_CONN_ID), if_exists='replace', index=False)

    spark.stop()

# Define the Airflow DAG
default_args = {
    'start_date': datetime(2023, 9, 6),
}

with DAG(
    dag_id='mdb-to-csv-dag', 
    schedule_interval='0 */3 * * *',  
    default_args=default_args, 
    catchup=False) as dag:

    # Task: List all .mdb files in the data directory
    list_mdb_files_task = PythonOperator(
        task_id='list_mdb_files',
        python_callable=list_mdb_files
    )
    
    # Task: Read each .mdb file and extract tables
    read_each_mdb_file_task = PythonOperator(
        task_id='read_each_mdb_file',
        python_callable=read_each_mdb_file
    )
    
    # Task: Process the extracted CSV files and load them into PostgreSQL
    process_csv_to_postgres_task = PythonOperator(
        task_id='process_csv_to_postgres',
        python_callable=process_csv_to_postgres
    )

    # Task dependencies
    list_mdb_files_task >> read_each_mdb_file_task >> process_csv_to_postgres_task
    # list_mdb_files_task >> read_each_mdb_file_task 
