from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from datetime import datetime

# Step 1: Extract Data from .mdb using mdb-export (BashOperator)
def extract_mdb():
    """Extract data from .mdb file and convert it to CSV."""
    # Command to extract .mdb data to CSV using MDBTools
    command = "mdb-export /opt/airflow/data/sample.mdb Table1 > /opt/airflow/data/extracted_data.csv"
    return command

# Step 2: Process the extracted CSV file using PySpark
def process_data_with_pyspark():
    """Read and process CSV data using PySpark."""
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("PySpark_MDB_Processing") \
        .getOrCreate()

    # Load CSV data extracted from .mdb
    df = spark.read.csv("/opt/airflow/data/extracted_data.csv", header=True, inferSchema=True)

    # Perform some transformation (Example: Show the data)
    df.show()

    # Example: Write the transformed data back to another CSV
    df.write.csv("/opt/airflow/data/processed_data.csv", header=True)

    # Stop SparkSession
    spark.stop()

# Define the Airflow DAG
default_args = {
    'start_date': datetime(2023, 9, 6),
}

with DAG(dag_id='extract_mdb_pyspark_dag', schedule_interval='@once', default_args=default_args, catchup=False) as dag:

    # Task 1: Extract data from .mdb file
    extract_mdb_task = BashOperator(
        task_id='extract_mdb_task',
        bash_command=extract_mdb()
    )

    # Task 2: Process extracted data with PySpark
    process_data_task = PythonOperator(
        task_id='process_data_with_pyspark',
        python_callable=process_data_with_pyspark
    )

    # Task dependency: Process data after extraction
    extract_mdb_task >> process_data_task
