from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

def get_emp(id, rule="all_success"):
    op = EmptyOperator(task_id=id, trigger_rule=rule)
    return op

with DAG(
    'movie',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hello world DAG',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie'],
) as dag:
    task_start = get_emp('start')
    task_end = get_emp('end', 'all_done') 
    
    task_get_data = BashOperator(
            task_id='get.data',
            bash_command="""
                echo 'get.data'
            """
    )
    task_save_data = BashOperator(
            task_id='save.data',
            bash_command="""
                echo 'save.data'
            """
    )
    task_err = BashOperator(
            task_id="err.report",
            bash_command="""
                echo "err report"
            """,
            trigger_rule="one_failed"
    )

    task_start >> task_get_data >> task_save_data >> task_end
    task_get_data >> task_err >> task_end
