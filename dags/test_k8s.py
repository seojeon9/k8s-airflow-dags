from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

# DAG 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    "test_k8s_dag",
    default_args=default_args,
    schedule_interval=None,  # 수동 실행
    catchup=False,
)

# KubernetesPodOperator를 사용한 테스트 태스크
test_task = KubernetesPodOperator(
    namespace="airflow",  # Airflow가 실행되는 네임스페이스
    image="python:3.9",  # 실행할 컨테이너 이미지
    cmds=["python", "-c"],
    arguments=["print('Hello from Kubernetes Executor!')"],
    labels={"test": "k8s_executor"},
    name="python_image",
    task_id="python_image",
    get_logs=True,
    dag=dag,
)

test_task2 = KubernetesPodOperator(
    namespace="airflow",  # Airflow가 실행되는 네임스페이스
    pod_template_file="/root/airflow/dags/template/pod_template.yaml",
    image = "172.31.11.141:5000/comp-image:v1.0.0",
    cmds = ["python", "-u", f"/app/crawler_main.py", "hansome"],
    arguments=["print('Hello from Kubernetes Executor!')"],
    labels={"test": "k8s_executor"},
    name="custom_comp_image",
    task_id="custom_comp_image",
    is_delete_operator_pod = False,
    delete_worker_pods = False,
    get_logs=True,
    dag=dag,
)

test_task2
