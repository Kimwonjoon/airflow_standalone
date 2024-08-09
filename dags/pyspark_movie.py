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
    'pyspark_movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    description='hello world DAG',
    schedule="10 2 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2016,1,1),
    catchup=True,
    tags=["pyspark"],
#    max_active_runs=1,  # 동시에 실행될 수 있는 최대 DAG 인스턴스 수
#    max_active_tasks=3,  # 동시에 실행될 수 있는 최대 태스크 수
) as dag:
    def re_parti(ds_nodash):
        from spark_repart.re_part import re_part
        re_part(ds_nodash)

    def branch_func(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        path = f'{home_dir}/data/movie/repartition/load_dt={ds_nodash}'
        # 이미 파일이 있다면 join_df로
        if os.path.exists(path):
            return join_df.task_id
        # 파일이 없다면 re_partition로
        else:
            return re_partition.task_id

    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end')

    branch_op = BranchPythonOperator(
            task_id="branch_op",
            python_callable=branch_func
            )
    
#    rm_dir = BashOperator(
#            task_id='rm.dir',
#            bash_command='rm -rf /home/kimpass189/data/movie/repartition/load_dt={{ ds_nodash }}',
#            trigger_rule = "all_success"
#    )

    re_partition = PythonOperator(
            task_id = "re.partition",
            python_callable=re_parti)
    join_df = BashOperator(
        task_id='join.df',
        bash_command="""
            spark-submit $AIRFLOW_HOME/pyspark_df/movie_join_df.py {{ds_nodash}}
            """,
        trigger_rule='all_done'
        )
    agg = BashOperator(
        task_id='agg',
        bash_command="""
            spark-submit $AIRFLOW_HOME/pyspark_df/sum_multi.py {{ds_nodash}}
            """,
        trigger_rule='all_done'
        )
            
    start  >> branch_op >> [join_df,re_partition]
    re_partition >> join_df >> agg >> end
