# 필요 모듈 import
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

# DAG 정의
dag = DAG(
    'DAG_test',         # DAG_Name
    default_args={                  
        'retries': 1,               # 실패시 재시도 횟수
        },
    schedule_interval='@once',      # 배치 주기
    start_date=datetime(2023,4, 3)  # 배치 시작날짜 설정
)

# 함수 설정
def print_test():
    my_dict = {"a": ['1', '3'], "b": ['1', '2'], "c": ['2', '4']}
    data = pd.DataFrame(my_dict)
    print(data.head())

# task 설정
t1 = PythonOperator( 
    task_id = 'print_test', #task 이름 설정
    python_callable=print_test, # 불러올 함수 설정
    dag=dag #dag 정보 
)

# task 진행
t1
