####
## Airflow DAG: DAG to perform data quality check
## Tech Design Answer by Mario Caesar // caesarmario87@gmail.com
####
# -- Imports

from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from dq_utils.observability import check_freshness, check_completeness
from dq_utils.monitoring import run_dq_rules
from dq_utils.alerting import send_alerts_to_slack

# -- DAG-level settings
default_args = {
    "owner"             : "caesarmario87@gmail.com",
    "depends_on_past"   : False,
    "start_date"        : datetime(2025, 5, 1),
    "retries"           : 1,
    "max_active_runs"   : 1,
    "retry_delay"       : timedelta(minutes=2),
}

# -- Define dag
with DAG(
    dag_id='dag_data_quality_platform',
    default_args=default_args,
    schedule='@hourly',
    catchup=False,
    tags=['data-quality'],
) as dag:

    task_check_freshness = PythonOperator(
        task_id='check_table_freshness',
        python_callable=check_freshness,
    )

    task_check_completeness = PythonOperator(
        task_id='check_table_completeness',
        python_callable=check_completeness,
    )

    task_run_dq_rules = PythonOperator(
        task_id='run_monitoring_rules',
        python_callable=run_dq_rules,
    )

    task_alert_violations = PythonOperator(
        task_id='send_alerts',
        python_callable=send_alerts_to_slack,
        retries=0,
    )

    [task_check_freshness, task_check_completeness] >> task_run_dq_rules >> task_alert_violations
