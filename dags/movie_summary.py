from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.python import (
        PythonOperator, 
        PythonVirtualenvOperator, 
        BranchPythonOperator
        )
from pprint import pprint

with DAG(
    'movie_summmary',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
        'max_active_runs' : 1,
        'max_active_tasks' : 3,
    },
    description='hello world DAG',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'summary', 'describe'],
) as dag:
    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end')

    apply_type = EmptyOperator(
            task_id = 'apply.type',
            )

    merge_df = EmptyOperator(
            task_id = 'merge.df',
            )

    df_dup = EmptyOperator(
            task_id = 'df.dup',
            )

    summary_df = EmptyOperator(
            task_id = 'summary.df',
            )
    task_start >> apply_type >> merge_df >> df_dup >> summary_df >> task_end
