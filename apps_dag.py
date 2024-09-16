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
    schedule_interval='0 12 * * *',
    
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

    #Process Installs

    # Convert 'installed_at' column to datetime
    install_df['installed_at'] = pd.to_datetime(install_df['installed_at'])

    # Format the installed_at as 'yyyy-mm-dd'
    install_df['installed_at'] = install_df['installed_at'].dt.strftime('%Y-%m-%d')

    # Rename the installed_at as install_date
    install_df.rename(columns={'installed_at': 'installed_date'}, inplace=True)

    #Fix network name spellings
    install_df['network_name'] = install_df['network_name'].replace({'TikTok': 'Tiktok', 'Google': 'Google Ads', 'ironSrc' : 'IronSource'})

    # Find the install count 
    install_grouped_df = install_df.groupby(['installed_date', 'network_name', 'campaign_name']).size().reset_index(name='install_count')

    #Process Events

    event_name_filter = ['AdImpression', 'GameStart']
    
    # selecting rows based on condition 
    events_df = events_df[events_df['event_name'].isin(event_name_filter)] 

    # Convert 'event_ts' column to datetime
    events_df['event_ts'] = pd.to_datetime(events_df['event_ts'])

    # Format the event_ts as 'yyyy-mm-dd'
    events_df['event_ts'] = events_df['event_ts'].dt.strftime('%Y-%m-%d')

    # Rename the event_ts as event_date
    events_df.rename(columns={'event_ts': 'event_date'}, inplace=True)

    merged_events_df = events_df.merge(install_df, on='user_id', how='left')

    # Aggregate merged_events_df table with 3 dimensions and calculate KPI's
    # Grouping by 'install_date', 'network_name', and 'campaign_name'

    grouped_df = merged_events_df.groupby(['installed_date', 'network_name', 'campaign_name']).agg(
        # Count distinct users who sent the 'GameStart' event
        game_start_count=('user_id', lambda x: x[merged_events_df['event_name'] == 'GameStart'].count()),
        
        # Sum of revenue when event_name is 'AdImpression' and ad_type is 'banner'
        banner_revenue=('ad_revenue', lambda x: x[(merged_events_df['event_name'] == 'AdImpression') & (merged_events_df['ad_type'] == 'banner')].sum()),
        
        # Sum of revenue when event_name is 'AdImpression' and ad_type is 'rewarded'
        interstitial_revenue=('ad_revenue', lambda x: x[(merged_events_df['event_name'] == 'AdImpression') & (merged_events_df['ad_type'] == 'interstitial')].sum()),

        # Sum of revenue when event_name is 'AdImpression' and ad_type is 'rewarded'
        rewarded_revenue=('ad_revenue', lambda x: x[(merged_events_df['event_name'] == 'AdImpression') & (merged_events_df['ad_type'] == 'rewarded')].sum())
    ).reset_index()

    #Process Cost 
    # Convert 'date' column to datetime
    cost_df['date'] = pd.to_datetime(cost_df['date'])
    cost_df['date'] = cost_df['date'].dt.strftime('%Y-%m-%d')

    # Aggregate cost table with 3 dimensions and sum the Costs
    cost_df = cost_df.groupby(['date', 'network_name', 'campaign_name'])['cost'].sum().reset_index()


    install_grouped_df.to_csv(PATH_TO_INSTALL_PROCESSED, index=False)
    grouped_df.to_csv(PATH_TO_EVENTS_PROCESSED, index=False)
    cost_df.to_csv(PATH_TO_COSTS_PROCESSED, index=False)


def create_final_table():
    # Load the JSON files into DataFrames
    install_grouped_df = pd.read_csv(PATH_TO_INSTALL_PROCESSED)
    grouped_df = pd.read_csv(PATH_TO_EVENTS_PROCESSED)
    cost_df = pd.read_csv(PATH_TO_COSTS_PROCESSED)

    final_df = grouped_df.merge(cost_df, left_on=['installed_date', 'network_name', 'campaign_name'], right_on=['date', 'network_name', 'campaign_name'], how='left')
    final_df = final_df.merge(install_grouped_df, on=['installed_date', 'network_name', 'campaign_name'], how='left')

    final_table = final_df[['installed_date', 'network_name', 'campaign_name', 'install_count', 'cost', 'game_start_count', 'banner_revenue', 'interstitial_revenue', 'rewarded_revenue']]
    final_table['cost'] = final_table['cost'].fillna(0)
    final_table['install_count'] = final_table['install_count'].fillna(0)
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
    op_kwargs={'api_url': 'https://dataeng-interview-project-zomet35f7q-uc.a.run.app/network_cost?cost_date={}'
               .format((datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')), 'file_name': PATH_TO_COSTS},
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
