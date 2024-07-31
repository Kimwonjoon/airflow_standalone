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
    'make_parquet',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='hello world DAG',
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['make','parquet'],
) as dag:
    task_start = get_emp('start')
    task_end = get_emp('end', 'all_done') 
    
    task_check_done = BashOperator(
            task_id='check.done',
            bash_command="""
                DONE_FILE={{ var.value.IMPORT_DONE_PATH }}/{{ds_nodash}}/_DONE
                bash {{ var.value.CHECK_SH }} $DONE_FILE
            """
    )
    task_to_parquet = BashOperator(
            task_id='to.parquet',
            bash_command="""
                echo 'to.parquet'
                #mkdir -p "$HOME/data/parquet/{{ds_nodash}}/"
                READ_PATH="~/data/csv/{{ds_nodash}}/csv.csv"
                SAVE_PATH="~/data/parquet"
                python ~/airflow/par/mk_par.py $READ_PATH $SAVE_PATH
            """
    )
    task_make_done = BashOperator(
            task_id='make.done',
            bash_command="""
                figlet 'make.done.start'
                PAR_PATH={{ var.value.PARQUET_DONE_PATH }}/{{ds_nodash}}
                mkdir -p $PAR_PATH
                touch $PAR_PATH/_DONE

                figlet 'make.done.end'

            """
    )
    task_err = BashOperator(
            task_id="err.report",
            bash_command="""
                echo "err report"
            """,
            trigger_rule="one_failed"
    )

    task_start >> task_check_done >> task_to_parquet >> task_make_done >> task_end
    task_check_done >> task_err >> task_end
