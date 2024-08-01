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
    REQUIREMENTS=["git+https://github.com/Kimwonjoon/mov_egg.git@0.5.2/egg"]
    def gen_empty(*ids):
        tasks = []
        for ind in ids:
            task = EmptyOperator(task_id = ind)
            tasks.append(task)
        return tuple(tasks)

    def gen_vpython(**kw):
        id = kw['id']
        func_o = kw['func_o']
        op_kw = kw['op']
#        task = PythonOperator(
#            task_id=id,
#            python_callable=func_o,
#            op_kwargs = op_kw
#            )
        task = PythonVirtualenvOperator(
            task_id=id,
            python_callable=func_o,
            requirements=REQUIREMENTS,
            system_site_packages=False,
            op_kwargs = op_kw
            )
        return task

    def proc_data(ds_nodash, **kwargs):
        print(ds_nodash)
        print(kwargs)
        print('preprocessing')

    def proc_merge(**kwargs):
        load_dt = kwargs['ds_nodash']
        from mov_egg.u import mer
        df = mer(load_dt)
        print("*" * 20)
        print(df)

    start, end = gen_empty('start', 'end')
            
    apply_type = gen_vpython(id = 'apply.type', op = {'It can move' : 'or not'}, func_o = proc_data)

    merge_df = gen_vpython(id = 'merge.df',
            op = {'It can move' : 'or not'},
            func_o = proc_merge)

    df_dup = gen_empty('df.dup')[0]

    summary_df = gen_empty('summary.df')[0]

    start  >> merge_df >> df_dup >> apply_type >> summary_df >> end
