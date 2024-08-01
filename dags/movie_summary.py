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
    'movie_summary',
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
    ################### 데이터 받아오는 곳 ##################

    def func_multi_nation(ds_nodash, multi_nation):
        from mov.api.call import save2df
        df = save2df(load_dt = ds_nodash, url_param = multi_nation)

        print(df[['movieCd', 'movieNm']].head(5))
        
        for k, v in multi_nation.items():
            df[k] = v
        
        #p_cols = list(url_param.keys()).insert(0, 'load_dt')
        p_cols = ['load_dt'] + list(multi_nation.keys())
        df.to_parquet('~/tmp/test_parquet', 
                partition_cols=p_cols
        )

    #########################################################

    def save_data(ds_nodash):
        from mov.api.call import get_key, echo, change2df
        df = change2df(load_dt = ds_nodash)

        print("*" * 10)
        print(df.head(10))
        print("*" * 10)
        print(df.dtypes)
        g = df.groupby('openDt')['audiCnt'].sum().reset_index()
        print(g.head())

    def branch_func(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        path = f'{home_dir}/tmp/test_parquet/load_dt={ds_nodash}'
        #path = os.path.join(home_dir,f'/tmp/test_parquet/load_dt={ds_nodash}')
        # 이미 파일이 있다면 rm_dir로
        if os.path.exists(path):
            return rm_dir.task_id
        # 파일이 없다면 get_data로
        else:
            return get_start.task_id, echo_task.task_id

    branch_op = BranchPythonOperator(
            task_id="branch_op", 
            python_callable=branch_func
            )

    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end')

    multi_y = PythonVirtualenvOperator(
            task_id='multi.y',
            python_callable=func_multi_nation,
            requirements=["git+https://github.com/Kimwonjoon/kim_movie.git@0.3.1/api"],
            op_kwargs = {'multi_nation' : {'multiMovieYn' : 'Y'}},
            system_site_packages=False,
            #venv_cache_path = "/home/kimpass189/tmp2/airflow_venv/get_data"
    ) # 다양성 영화
    multi_n = PythonVirtualenvOperator(
            task_id='multi.n',
            python_callable=func_multi_nation,
            requirements=["git+https://github.com/Kimwonjoon/kim_movie.git@0.3.1/api"],
            op_kwargs = {'multi_nation' : {'multiMovieYn' : 'N'}},
            system_site_packages=False,
            #venv_cache_path = "/home/kimpass189/tmp2/airflow_venv/get_data"
    ) # 상업 영화
    nation_k = PythonVirtualenvOperator(
            task_id='nation_k',
            python_callable=func_multi_nation,
            requirements=["git+https://github.com/Kimwonjoon/kim_movie.git@0.3.1/api"],
            op_kwargs = {'multi_nation' : {'repNationCd' : 'K'}},
            system_site_packages=False,
            #venv_cache_path = "/home/kimpass189/tmp2/airflow_venv/get_data"
    ) # 한국 영화
    nation_f = PythonVirtualenvOperator(
            task_id='nation_f',
            python_callable=func_multi_nation,
            requirements=["git+https://github.com/Kimwonjoon/kim_movie.git@0.3.1/api"],
            op_kwargs = {'multi_nation' : {'repNationCd' : 'F'}},
            system_site_packages=False,
            #venv_cache_path = "/home/kimpass189/tmp2/airflow_venv/get_data"
    ) # 외국 영화

    throw_err = BashOperator(
            task_id = 'throw.err',
            bash_command = "exit 1",
            trigger_rule = "all_done"
            )
    get_start = EmptyOperator(task_id='get.start', trigger_rule = 'all_done')

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

    get_end = EmptyOperator(task_id='get.end', trigger_rule = 'all_done')
    
    task_save_data = PythonVirtualenvOperator(
            task_id='save.data',
            python_callable=save_data,
            requirements=["git+https://github.com/Kimwonjoon/kim_movie.git@0.3.1/api"],
            system_site_packages=False,
            trigger_rule = "one_success",
            #venv_cache_path = "/home/kimpass189/tmp2/airflow_venv/get_data"
    )
    rm_dir = BashOperator(
            task_id='rm.dir',
            bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}',
            trigger_rule = "all_success"
    )
    echo_task = BashOperator(
            task_id = "echo.task",
            bash_command = "echo 'task'"
            )
    
    li = [multi_y, multi_n, nation_k, nation_f]
    task_start >>[branch_op, throw_err]
    branch_op >> [rm_dir, get_start, echo_task]
    rm_dir >> get_start
    throw_err >> task_save_data
    get_start >> li
    for i in li:
        i >> apply_type

    li  >> get_end >> task_save_data >> task_end
