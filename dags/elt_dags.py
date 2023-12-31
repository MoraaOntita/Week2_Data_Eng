from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
import psycopg2
from airflow.operators.sensors import ExternalTaskSensor

# Define default_args dictionary to set default parameters for the DAG
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 24, 8, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG with the specified default_args
dag = DAG(
    'elt_process',
    default_args=default_args,
    description='ELT Process DAG',
    schedule_interval=timedelta(days=1),
)

# Define a Python function to extract data from CSV files
def extract_data():
    # Implement your data extraction logic here
    print("Extracting data from CSV files")

# Define a Python function to load data from CSV to PostgreSQL
def load_data_to_postgres(**kwargs):
    # Extract execution date from the DAG run context
    execution_date = kwargs['execution_date']
    
    # Formating the date in the required format (e.g., '20181024')
    formatted_date = execution_date.strftime('%Y%m%d')

    # Specify the directory where your CSV files are stored (same directory as DAG script)
    csv_directory = '/home/moraa/10Academy/Week-2'

    # Generate the CSV file path based on the date (assuming the file is named consistently)
    csv_file_path = f"{csv_directory}{formatted_date}20181024_d1_0830_0900.csv"

    # Implement your PostgreSQL connection and data loading logic here
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        user='postgres',
        password='Musy19',
        database='swarm_week2db',
    )

    cursor = conn.cursor()

    # Create the target table if it doesn't exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS your_target_table (
        column1 VARCHAR,
        column2 INT,
        -- Add more columns and their data types as needed
    );
    """

    cursor.execute(create_table_sql)
    conn.commit()

    # Specify the columns from the CSV file to copy into the table
    columns_to_copy = ('column1', 'column2')
    
    with open(csv_file_path, 'r') as f:
        next(f)
        cursor.copy_from(f, 'your_target_table', sep=',', columns=columns_to_copy)

    conn.commit()
    conn.close()

# Define a Python function to trigger dbt for transformations
def run_dbt():
    # Implement your dbt run logic here
    print("Running dbt for transformations")

# Define tasks for each step of the ELT process

# Task to extract data from CSV files
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

# Task to load data into PostgreSQL
load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_data_to_postgres,
    provide_context=True,
    dag=dag,
)

# Task to run dbt for transformations
dbt_task = PythonOperator(
    task_id='run_dbt',
    python_callable=run_dbt,
    dag=dag,
)

# Define a final task as a dummy task to signify the end of the DAG
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define the task dependencies
extract_task >> load_task >> dbt_task >> end_task

sensor_task = ExternalTaskSensor(
    task_id='wait_for_data_availability',
    external_dag_id='external_data_dag',
    external_task_id='data_available_task',
    poke_interval=300,  # Adjust the interval as needed
    timeout=600,  # Adjust the timeout as needed
    mode='poke',
    dag=dag,
)
