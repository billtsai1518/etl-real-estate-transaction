from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime, timedelta
#import json
import pandas as pd
import io

# Default settings for all the dags in the pipeline
default_args = {
    "owner": "billtsai1518", 
    "start_date" : datetime(2024, 1, 1, 14, 00),
    "retries" : 1,
    "retry_delay": timedelta(minutes=5)
}

def _process_data(ti):

    data = ti.xcom_pull(task_ids = 'extract_data') # Extract the json object from XCom
    df = pd.read_csv(io.StringIO(data))
    df = df[df['備註'].isna()].drop(columns=['備註'])

    bins = [0, 10, 20, 30, 40, 50, float('inf')]
    labels = ['0-9', '10-19', '20-29', '30-39', '40-49', '>50']
    df['屋齡區間'] = pd.cut(df['屋齡'], bins=bins, labels=labels, right=False)

    # Export DataFrame to CSV
    df.to_csv('/tmp/processed_data.csv', sep=';', index=False, header=None)


def _store_data():
    '''
    This function uses the Postgres Hook to copy users from processed_data.csv
    and into the table
    
    '''
    # Connect to the Postgres connection
    hook = PostgresHook(postgres_conn_id = 'postgres')

    # Insert the data from the CSV file into the postgres database
    hook.copy_expert(
        sql = "COPY transactions FROM stdin WITH DELIMITER as ';'",
        filename='/tmp/processed_data.csv'
    )


with DAG('real_estate_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:


    # Dag #1 - Check if the API is available
    is_api_available = HttpSensor(
        task_id='is_api_available',
        method='GET',
        http_conn_id='real_estate_api',
        endpoint='',
        response_check= lambda response: '387000000A' in response.text,
        poke_interval = 5
    )

    # Dag #2 - Create a table
    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id='postgres',
        sql='''
            drop table if exists transactions;
            create table transactions(
                number INT NOT NULL,
                agency_code TEXT,
                city_code TEXT,
                area_code TEXT,
                dist TEXT,
                trans_obj TEXT,
                address TEXT,
                land_trans_area FLOAT,
                urban_zone TEXT,
                non_urban_zone TEXT,
                non_urban_plan TEXT,
                year TEXT,
                level TEXT,
                total_level TEXT,
                type TEXT,
                main_purpose TEXT,
                main_material TEXT,
                completion_date TEXT,
                age INT,
                build_trans_area FLOAT,
                room INT,
                hall INT,
                bathroom INT,
                partion TEXT,
                management_org TEXT,
                total_price TEXT,
                unit_price TEXT,
                parking_cat TEXT,
                parking_area TEXT,
                parking_price TEXT,
                place_status TEXT,
                age_interval TEXT
            );
        '''
    )

    # DAG #3 - Extract Data
    extract_data = SimpleHttpOperator(
            task_id = 'extract_data',
            http_conn_id='real_estate_api',
            method='GET',
            endpoint='',
            response_filter=lambda response: (response.text),
            log_response=True
    )

    # Dag #4 - Transform data
    transform_data = PythonOperator(
        task_id = 'transform_data',
        python_callable=_process_data

    )

    # Dag #5 - Load data
    load_data = PythonOperator(
        task_id = 'load_data',
        python_callable=_store_data

    )

    # Dependencies
    is_api_available >> create_table >> extract_data >> transform_data >> load_data
