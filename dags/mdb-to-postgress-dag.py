import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
import subprocess

# File paths constants
DATA_DIR = '/opt/airflow/data/'
POSTGRES_CONN_ID = 'postgresql://airflow:airflow@postgres:5432/mdbfiles'

def get_spark_session():
    return SparkSession.builder \
        .appName("MSAccessToPostgres") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.3.6") \
        .config("spark.driver.extraClassPath", "/opt/jars/postgresql-42.3.6.jar") \
        .config("spark.executor.extraClassPath", "/opt/jars/postgresql-42.3.6.jar") \
        .getOrCreate()

def stop_spark_session(spark):
    spark.stop()

def get_mdb_files():
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.mdb')]
    print(f"Files: {files}")
    return files

def get_tables_list(mdb_file):
    command = f"mdb-tables -1 {os.path.join(DATA_DIR, mdb_file)}"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    tables = result.stdout.strip().split('\n')
    print(f"Tables: {tables}")
    return tables

def get_msaccess_con_url(mdb_file):
    access_db_path = os.path.join(DATA_DIR, mdb_file)
    # This format might need to be adapted based on your JDBC-ODBC bridge configuration
    return f"jdbc:odbc:Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};Dbq={access_db_path}"

def transfer_data_from_access_to_postgres(**kwargs):
    spark = get_spark_session()
    postgres_engine = create_engine(POSTGRES_CONN_ID)
    mdb_files = get_mdb_files()

    for mdb_file in mdb_files:
        msaccess_conn_url = get_msaccess_con_url(mdb_file)
        tables = get_tables_list(mdb_file)

        for table in tables:
            access_df = spark.read \
                .format("jdbc") \
                .option("url", msaccess_conn_url) \
                .option("dbtable", table) \
                .load()

            access_df.write \
                .format("jdbc") \
                .option("url", POSTGRES_CONN_ID) \
                .option("dbtable", f"{mdb_file}_{table}") \
                .option("user", "airflow") \
                .option("password", "airflow") \
                .mode("overwrite") \
                .save()

    stop_spark_session(spark)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'msaccess_to_postgres',
    default_args=default_args,
    description='Transfer data from MS Access to PostgreSQL using PySpark',
    schedule_interval='@once', 
)

transfer_task = PythonOperator(
    task_id='transfer_access_to_postgres',
    python_callable=transfer_data_from_access_to_postgres,
    dag=dag,
)

transfer_task
