import pandas as pd
from sklearn.metrics import confusion_matrix
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    BranchPythonOperator,
)
import os
import requests
import json

with DAG(
        'fish_predict',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='fish pred Dag',
    schedule=None,
    catchup=True,
    tags=['fish','predict','data'],
) as dag:

    def loadcsv():
        file_path = "/home/kimpass189/data/fish_test_data.csv"
        save_path = '/home/kimpass189/data/fish_parquet_sang/'
        val_data=pd.read_csv(file_path)
        val_data['Label'][val_data['Label']=='Bream']=0
        val_data['Label'][val_data['Label']=='Smelt']=1
        if os.path.exists(save_path):
            return True
        else:
            os.makedirs(os.path.dirname(save_path), exist_ok = False)
        val_data.to_parquet(f"{save_path}/fish_test_data.parquet")

    def prediction():
        load_path = "/home/kimpass189/data/fish_parquet_sang/"
        save_path = "/home/kimpass189/data/fish_pred_parquet/"

        val_data=pd.read_parquet(load_path)
        val_data_cut=val_data[:100000]
        headers = {
            'accept': 'application/json',
        }

        pred_result=[]

        for i in range(len(val_data_cut)):
            params = {
            'length': val_data['Length'][i],
            'weight': val_data['Weight'][i],
            }
            response = requests.get('http://127.0.0.1:8765/fish', params=params, headers=headers)
            data=json.loads(response.text)
            if data['prediction'] == '도미':
                pred_result.append(0)
            else :
                pred_result.append(1)

        val_data_cut['pred_result']=pred_result

        if os.path.exists(save_path):
            val_data_cut.to_parquet(f"{save_path}/fish_pred.parquet")
        else:
            os.makedirs(os.path.dirname(save_path), exist_ok = False)
            val_data_cut.to_parquet(f"{save_path}/fish_pred.parquet")
  

    def aggregate():
        load_path = "/home/kimpass189/data/fish_pred_parquet/"
        save_path = "/home/kimpass189/data/fish_agg_parquet/"
        pdf=pd.read_parquet(load_path)
        cm=confusion_matrix(pdf['Label'],pdf['pred_result'])
        Real=['Real_Bream','Real_Smelt']
        pred=['pred_Bream','pred_Smelt']
        cm_df = pd.DataFrame(cm, index=Real, columns=pred)
        print(cm_df)
        if os.path.exists(save_path):
            cm_df.to_parquet(f"{save_path}/fish_agg.parquet")
        else:
            os.makedirs(os.path.dirname(save_path), exist_ok = False)
            cm_df.to_parquet(f"{save_path}/fish_agg.parquet")


    start=EmptyOperator(
        task_id="start"
    )
    end = EmptyOperator(
        task_id="end"
    )

    load_csv = PythonOperator(
        task_id="load.csv",
        python_callable = loadcsv ,
    )

    predict = PythonOperator(
        task_id="predict",
        python_callable = prediction ,
    )

    agg = PythonOperator(
        task_id="agg",
        python_callable = aggregate ,
    )



    start >> load_csv >> predict >> agg >> end
