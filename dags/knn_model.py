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
import pandas as pd
import os
#from sklearn.metrics import f1_score
#from sklearn.neighbors import KNeighborsClassifier
#import pickle
#from fishmlserv.model.manager import get_model_path

with DAG(
    'KNN-Model-predict',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    description='hello world DAG',
    schedule_interval='@once',
    #schedule="10 2 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015,1,2),
    catchup=True,
    tags=["KNN","ML","predict"],
#    max_active_runs=1,  # 동시에 실행될 수 있는 최대 DAG 인스턴스 수
#    max_active_tasks=3,  # 동시에 실행될 수 있는 최대 태스크 수
) as dag:
    # csv to parquet 저장
    def csvtoparquet():
        file_path = '/home/kimpass189/data/fish_parquet/fish.parquet'
        if os.path.exists(file_path):
            return True
        else:
            os.makedirs(os.path.dirname(file_path), exist_ok = False)
        df = pd.read_csv('~/data/fish_test_data.csv')
        df.columns = ["length", "weight","Label"]
        def func1(x):
            if x == "Bream":
                return 0
            else:
                return 1
        df['target'] = df['Label'].map(func1)
        print(df.head())
        df.to_parquet('~/data/fish_parquet/fish.parquet', index = False)
        return True
    
    def knn_predict_func():
        import pandas as pd
        import os
        from sklearn.metrics import f1_score
        from sklearn.neighbors import KNeighborsClassifier
        import pickle
        from fishmlserv.model.manager import get_model_path
        df = pd.read_parquet('~/data/fish_parquet/fish.parquet')
        # 독립변수, 종속변수 구분
        x_test = df[['length','weight']]
        y_test = df['target']
        # 예측
        model_path = get_model_path()

        with open(model_path, 'rb') as f:
            fish_model = pickle.load(f)

        prediction = fish_model.predict(x_test)
        score = f1_score(y_test, prediction)
        # 예측 결과 저장
        result_df = pd.DataFrame({'target' : y_test, 'pred' : prediction})
        result_path = '/home/kimpass189/data/fish_parquet/pred_result/result.parquet'
        if os.path.exists(result_path):
            return True
        else:
            os.makedirs(os.path.dirname(result_path), exist_ok = False)
        result_df.to_parquet(result_path, index = False) # 예측 결과 파일 parquet 저장
        print(score) # f1_score 출력
        return True
    
    def knn_group():
        result = pd.read_parquet('~/data/fish_parquet/pred_result/result.parquet')
        agg_result = result.groupby('pred').count()
        print(agg_result)
        return True
        
    
    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end')

    load_csv = PythonOperator(
            task_id = "load.csv",
            python_callable=csvtoparquet)

#    knn_predict = PythonOperator(
#            task_id = "knn.predict",
#            python_callable=knn_predict)
    knn_predict = PythonVirtualenvOperator(
            task_id='knn.predict',
            python_callable=knn_predict_func,
            requirements=["git+https://github.com/Kimwonjoon/fish_ml.git@0.7/manifest"],
            system_site_packages=False,
    )

    knn_agg = PythonOperator(
            task_id = "knn.agg",
            python_callable=knn_group
            )
    start >> load_csv >> knn_predict >> knn_agg >> end
