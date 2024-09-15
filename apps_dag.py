from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 14),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'mali_prediction_new',
    default_args=default_args,
    description='A DAG for APPS Data Engineering Case',
    schedule_interval='@daily'
)

PATH_TO_INSTALL = '/output/installs.json'
PATH_TO_EVENTS = '/output/events.json'
PATH_TO_COSTS = '/output/costs.json'

PATH_TO_INSTALL_PROCESSED = '/output/installs_processed.csv'
PATH_TO_EVENTS_PROCESSED  = '/output/events_processed.csv'
PATH_TO_COSTS_PROCESSED  = '/output/costs_processed.csv'

# Define a function to fetch data from the API and save it to disk
def fetch_data_from_api(api_url, file_name):
    print("fetching_data")
    print()
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()  # Get the JSON data
        # Save data to a JSON file
        with open(file_name, 'w') as f:
            json.dump(data, f)
    else:
        raise Exception(f"Failed to fetch data from {api_url}")

# Function to load data from the disk and create a Pandas DataFrame
def load_data_to_dataframe(file_name):
    with open(file_name, 'r') as f:
        data = json.load(f)
    return pd.DataFrame(data)

# Function to process and aggregate data
def process_data():
    print("processing_data")
    # Load the JSON files into DataFrames
    install_df = load_data_to_dataframe(PATH_TO_INSTALL)
    events_df = load_data_to_dataframe(PATH_TO_EVENTS)
    cost_df = load_data_to_dataframe(PATH_TO_COSTS)

    # Example processing: filtering, aggregation, merging
    #install_filtered = install_df[install_df['status'] == 'active']
    #network_aggregated = network_df.groupby('network_type').agg({'bandwidth': 'mean'})
    #cost_filtered = cost_df[cost_df['total_cost'] > 1000]

    install_df.to_csv(PATH_TO_INSTALL_PROCESSED, index=False)
    events_df.to_csv(PATH_TO_EVENTS_PROCESSED, index=False)
    cost_df.to_csv(PATH_TO_COSTS_PROCESSED, index=False)


def create_final_table():
    # Load the JSON files into DataFrames
    install_df = pd.read_csv(PATH_TO_INSTALL_PROCESSED)
    events_df = pd.read_csv(PATH_TO_EVENTS_PROCESSED)
    cost_df = pd.read_csv(PATH_TO_COSTS_PROCESSED)

    final_table = install_df
    
    # Save the final table to CSV
    final_table.to_csv('/output/final_table.csv', index=False)

def delete_raw_data():
    print("Deleting raw data")
    os.remove(PATH_TO_EVENTS)
    os.remove(PATH_TO_INSTALL)
    os.remove(PATH_TO_COSTS)

    os.remove(PATH_TO_EVENTS_PROCESSED)
    os.remove(PATH_TO_INSTALL_PROCESSED)
    os.remove(PATH_TO_COSTS_PROCESSED)

# Define the tasks
fetch_install_data = PythonOperator(
    task_id='fetch_install_data',
    python_callable=fetch_data_from_api,
    op_kwargs={'api_url': 'https://dataeng-interview-project-zomet35f7q-uc.a.run.app/installs', 'file_name': PATH_TO_INSTALL},
    dag=dag
)

fetch_network_data = PythonOperator(
    task_id='fetch_network_data',
    python_callable=fetch_data_from_api,
    op_kwargs={'api_url': 'https://dataeng-interview-project-zomet35f7q-uc.a.run.app/events', 'file_name': PATH_TO_EVENTS},
    dag=dag
)

fetch_cost_data = PythonOperator(
    task_id='fetch_cost_data',
    python_callable=fetch_data_from_api,
    op_kwargs={'api_url': 'https://dataeng-interview-project-zomet35f7q-uc.a.run.app/network_cost?cost_date={}'.format(datetime.today().strftime('%Y-%m-%d')), 'file_name': PATH_TO_COSTS},
    dag=dag
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag
)

delete_raw_data_task = PythonOperator(
    task_id='delete_raw_data',
    python_callable=delete_raw_data,
    dag=dag
)

create_final_table_task = PythonOperator(
    task_id='create_final_table',
    python_callable=create_final_table,
    dag=dag
)

# Define the task dependencies
[fetch_install_data, fetch_network_data, fetch_cost_data] >> process_data_task >> create_final_table_task >> delete_raw_data_task
