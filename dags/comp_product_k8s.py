import os
import re
import json
import time
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator, get_current_context
from airflow.operators.bash import BashOperator
from airflow.models.xcom import LazyXComAccess
from airflow.exceptions import AirflowSkipException
from airflow.utils.dates import days_ago
from airflow.models import TaskInstance
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator



## 채널 및 업로드 정보 설정 
# 채널별 python 이미지 설정을 해야함. 
channel_config = {
    # "ably" : {"upload_pdf": True, "upload_xlsx": True},
    # "cjmall" : {"upload_pdf": True, "upload_xlsx": False},
    # "gsshop" : {"upload_pdf": True, "upload_xlsx": False},
    "hansome": {"upload_pdf": True, "upload_xlsx": False},
    # "kolon": {"upload_pdf": True, "upload_xlsx": False},
    # "lfmall": {"upload_pdf": True, "upload_xlsx": False},
    # "musinsa": {"upload_pdf": True, "upload_xlsx": True},
    # "musinsa_standard": {"upload_pdf": True, "upload_xlsx": True},
    # "naver_fashiontown": {"upload_pdf": True, "upload_xlsx": True},
    # "naverstyle": {"upload_pdf": True, "upload_xlsx": False},
    "sonyunara": {"upload_pdf": True, "upload_xlsx": False},
    # "stylenoriter": {"upload_pdf": True, "upload_xlsx": False},
    # "uniqlo": {"upload_pdf": False, "upload_xlsx": True},
    # "uniqlo_rank": {"upload_pdf": True, "upload_xlsx": False},
    #"topten" : {"upload_pdf" : False, "upload_xlsx":True, "python_image" : "myregistry.com/my-airflow-python:3.9"} 

}

channel_names = list(channel_config.keys())

with DAG(
    dag_id="comp_product_test_k8s_dag",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["COMP", "TEST"]
) as dag:

    # 1. craweler script 실행
    @task
    def run_crawler_script(channel_name):
        context = get_current_context()
        pod_task = KubernetesPodOperator(
            task_id = f"run_crawler_script_{channel_name}",
            name = f"run-crawler-{channel_name}",
            namespace = 'default',
            pod_template_file="/root/airflow/dags/template/pod_template.yaml",
            image = "172.31.11.141:5000/comp-image:v1.0.0",
            cmds = ["python", "-u", f"/app/crawler_main.py", channel_name],
            get_logs = True,
            #on_finish_action=PodOnFinishAction.KEEP_POD,
            is_delete_operator_pod = False,
            labels={
                "dag_id": "comp_product_test_k8s_dag",
                "task_id": "run_crawler_script",
                "channel": channel_name
            }
        )

        logs = pod_task.execute(context=context)
        if not logs:
            raise ValueError(f"[{channel_name}] No logs returned from pod execution.")

        # 출력 로그 중 JSON 부분 추출
        json_pattern = r'S3 UPLOAD SUCCESS[:\s]*\n?(?P<json>\{.*\})'
        match = re.search(json_pattern, logs.strip(), re.DOTALL)

        if not match:
            raise ValueError(f"Could not extract JSON from output: {logs.strip()}")

        # JSON 출력 검증
        try:
            json_result = json.loads(match.group())  # JSON 변환 확인
            return json_result
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON output from script: {logs.strip()}")
    run_crawler_script_task = run_crawler_script.expand(channel_name=channel_names)


    # 2. crawler 실행 후 저장한 S3 obj key 추출
    @task
    def extract_s3_keys_from_xcom(channel_name):
        context = get_current_context()
        task_instance = context["ti"]  # 현재 TaskInstance 가져오기

        result_lazy = task_instance.xcom_pull(task_ids="run_crawler_script", key="return_value")

        # LazyXComAccess 객체인지 확인 후 변환
        if isinstance(result_lazy, LazyXComAccess):
            result_list = list(result_lazy)  # LazyXComAccess → 리스트 변환
            print(f"Converted LazyXComAccess to list: {result_list}")  # 디버깅 로그
        else:
            result_list = result_lazy

        # 리스트 안에 딕셔너리 형태인지 확인 후 변환
        if isinstance(result_list, list) and len(result_list) > 0 and isinstance(result_list[0], dict):
            result = result_list[0]  # 리스트에서 딕셔너리 추출
        elif isinstance(result_list, dict):
            result = result_list  # 이미 딕셔너리라면 그대로 사용
        else:
            raise ValueError(f"Unexpected XCom result format: {type(result_list)} - {result_list}")

        print(f"Extracted final result: {result}")  # 최종 데이터 확인

        s3_keys = result.get("s3_bucket_keys", [])

        if not s3_keys:
            raise ValueError("No S3 keys found in XCom")

        return [{"channel": channel_name, "s3_key": key} for key in s3_keys] 
    extract_s3_keys_task = extract_s3_keys_from_xcom.expand(channel_name=channel_names)


    # 3. S3 obj key flatten
    @task 
    def flatten_s3_keys(list_of_keys):
        flattened = []
        for item in list_of_keys:
            if isinstance(item, list):
                flattened.extend(item)
            else:
                flattened.append(item)
        return flattened
    flattened_s3_keys = flatten_s3_keys(extract_s3_keys_task)


    # 4. S3KeySensor로 S3 obj key 저장확인
    with TaskGroup(group_id="s3_sensors") as s3_sensor_group:
        s3_key_sensor_task = S3KeySensor.partial(
        task_id="check_s3_file",
        bucket_name="bpcc-prd-s3-seoul-etl",
        wildcard_match=False,
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=600,
        trigger_rule=TriggerRule.ALL_SUCCESS,  # 모든 S3 Key가 확인된 경우에만 성공 처리
        ).expand(
            bucket_key=flattened_s3_keys.map(lambda x: x["s3_key"])
        )


    # 5. S3KeySensor 성공한 채널 목록 생성
    @task
    def get_successful_channels(flattened_keys):
        # flattened_keys는 각 원소가 {"channel": <채널>, "s3_key": ...} 형태임
        channels = {entry["channel"] for entry in flattened_keys}
        return list(channels)
    successful_channels = get_successful_channels(flattened_s3_keys)

 
    #6-1 PDF 업로드 태스크
    @task
    def run_upload_pdf_script(channel_name):
        if not channel_config.get(channel_name, {}).get("upload_pdf", False):
            print(f"[{channel_name}] Skipping PDF upload as per configuration")
            raise AirflowSkipException("Skipping PDF upload")
        
        context = get_current_context()
        pod_task = KubernetesPodOperator(
            task_id=f"run_upload_pdf_script_{channel_name}",
            name=f"upload-pdf-{channel_name}",
            namespace='default',
            pod_template_file="/root/airflow/dags/template/pod_template.yaml",
            image="172.31.11.141:5000/comp-image:v1.0.0",
            cmds=["python", "-u", f"/app/upload_pdf.py", "--channel", channel_name],
            get_logs=True,
            #on_finish_action=PodOnFinishAction.KEEP_POD,
            is_delete_operator_pod = False,
            labels={
            "dag_id": "comp_product_test_k8s_dag",
            "task_id": "run_upload_pdf_script",
            "channel": channel_name
            }
        )   

        logs = pod_task.execute(context=context)

        if "Error" in logs or "Exception" in logs:
            raise ValueError(f"[{channel_name}] PDF script error detected in logs: {logs}")
        if not logs.strip():
            raise ValueError(f"[{channel_name}] PDF script returned empty output")
        
        print(f"[{channel_name}] PDF Logs: {logs.strip()}")
        return f"[{channel_name}] PDF upload complete"

    run_upload_pdf_script_task = run_upload_pdf_script.expand(channel_name=successful_channels)

    # 6-2. XLSX 업로드 태스크
    @task
    def run_upload_xlsx_script(channel_name):
        if not channel_config.get(channel_name, {}).get("upload_xlsx", False):
            print(f"[{channel_name}] Skipping XLSX upload as per configuration")
            raise AirflowSkipException("Skipping XLSX upload")
        
        context = get_current_context()

        pod_task = KubernetesPodOperator(
            task_id=f"run_upload_xlsx_script_{channel_name}",
            name=f"upload-xlsx-{channel_name}",
            namespace='default',
            pod_template_file="/root/airflow/dags/template/pod_template.yaml",
            image="172.31.11.141:5000/comp-image:v1.0.0",
            cmds=["python", "-u", f"/app/upload_xlsx.py", "--channel", channel_name],
            get_logs=True,
            #on_finish_action=PodOnFinishAction.KEEP_POD,
            is_delete_operator_pod = False,
            labels={
            "dag_id": "comp_product_test_k8s_dag",
            "task_id": "run_upload_xlsx_script",
            "channel": channel_name
            }
        )    
        
        logs = pod_task.execute(context=context)

        if "Error" in logs or "Exception" in logs:
            raise ValueError(f"[{channel_name}] XLSX script error detected in logs: {logs}")

        if not logs.strip():
            raise ValueError(f"[{channel_name}] XLSX script returned empty output")

        print(f"[{channel_name}] XLSX Logs: {logs.strip()}")
        return f"[{channel_name}] XLSX upload complete"
    
    run_upload_xlsx_script_task = run_upload_xlsx_script.expand(channel_name=successful_channels)
    

    # TASK dependencies
    run_crawler_script_task >> extract_s3_keys_task >> flattened_s3_keys  >> s3_sensor_group >> successful_channels >> [run_upload_pdf_script_task, run_upload_xlsx_script_task]
