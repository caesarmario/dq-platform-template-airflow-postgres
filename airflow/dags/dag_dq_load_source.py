####
## Airflow DAG: DAG to load data
## Tech Design Answer by Mario Caesar // caesarmario87@gmail.com
####

# -- Imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import sqlite3
from datetime import datetime, timedelta

from load_utils import load_sqlite_to_postgres, load_csv_to_postgres

# -- DAG-level settings
default_args = {
    "owner"             : "caesarmario87@gmail.com",
    "depends_on_past"   : False,
    "start_date"        : datetime(2025, 5, 1),
    "retries"           : 1,
    "max_active_runs"   : 1,
    "retry_delay"       : timedelta(minutes=2),
}

# -- Postgre config
pg_config = {
    'host': 'host.docker.internal',
    'port': 5436,
    'user': 'airflow',
    'password': 'airflow',
    'dbname': 'airflow'
}

# -- Python function for sqlite
def get_sqlite_tables(sqlite_path):
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tables

# -- Define dag
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

    task_dq = TriggerDagRunOperator(
        task_id="task_dq",
        trigger_dag_id="dag_dq_check",
        dag=dag
    )
