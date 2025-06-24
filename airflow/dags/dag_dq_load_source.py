####
## Airflow DAG: DAG to load data
## Tech Design Answer by Mario Caesar // caesarmario87@gmail.com
####

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from load_utils import load_sqlite_to_postgres, load_csv_to_postgres

import sqlite3

default_args = {
    'owner': 'tmt',
    'start_date': datetime(2025, 6, 1),
}

pg_config = {
    'host': 'host.docker.internal',
    'port': 5436,
    'user': 'airflow',
    'password': 'airflow',
    'dbname': 'airflow'
}

def get_sqlite_tables(sqlite_path):
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tables

with DAG(
    dag_id='dag_dq_load_sqlite_csv_to_postgres',
    default_args=default_args,
    catchup=False,
    tags=['data-quality', 'source-loading'],
) as dag:

    sqlite_path = './data/olist.sqlite'
    sqlite_tables = get_sqlite_tables(sqlite_path)

    for tbl_name in sqlite_tables:
        PythonOperator(
            task_id=f'load_sqlite_{tbl_name}',
            python_callable=load_sqlite_to_postgres,
            op_kwargs={
                'sqlite_path': sqlite_path,
                'pg_config': pg_config,
                'table_name': tbl_name,
                'sqlite_query': f'SELECT * FROM {tbl_name}'
            }
        )

    task_csv = PythonOperator(
        task_id='load_csv',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_path': './data/customer_transactions.csv',
            'pg_config': pg_config,
            'table_name': 'customer_transactions'
        }
    )
