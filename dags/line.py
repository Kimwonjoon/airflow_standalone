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

with DAG(
    'line',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    description='hello world DAG',
    schedule="10 2 * * *",
    start_date=datetime(2024, 8, 15),
    catchup=True,
    tags=["line"],
    max_active_runs=1,  # 동시에 실행될 수 있는 최대 DAG 인스턴스 수
#    max_active_tasks=3,  # 동시에 실행될 수 있는 최대 태스크 수
) as dag:
    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end')

    bash_job = BashOperator(
        task_id='bash.job',
        bash_command="""
        # 0 또는 1을 랜덤하게 생성
        RANDOM_NUM=$RANDOM
        REMAIN=$(( RANDOM_NUM % 3 ))
        echo "RANDOM_NUM:$RANDOM_NUM, REMAIN:$REMAIN"
        # -eq : equal 결국 remain = 0 이라면 성공
        if [ $REMAIN -eq 0 ]; then
            echo "작업이 성공했습니다."
            exit 0
        else
            echo "작업이 실패했습니다."
            exit 1
        fi
            """
        )
    notify_success = BashOperator(
        task_id='notify.success',
        bash_command="""
            curl -X POST -H 'Authorization: Bearer 0CexLLmrOvvWXwb21FLPvNXiwV3JKpqh7tcC9mjEmc2' -F 'message={{ dag_run.dag_id }} SUCCESS' https://notify-api.line.me/api/notify
            """,
        trigger_rule = 'one_success'
        )
    notify_fail = BashOperator(
        task_id='notify.fail',
        bash_command="""
            curl -X POST -H 'Authorization: Bearer 0CexLLmrOvvWXwb21FLPvNXiwV3JKpqh7tcC9mjEmc2' -F 'message={{ dag_run.dag_id }} FAIL' https://notify-api.line.me/api/notify
            """,
        trigger_rule = 'one_failed'
        )
            
    start >> bash_job >> [notify_success, notify_fail] >> end
