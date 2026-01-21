from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 기본 설정
default_args = {
    'owner': 'jaeik',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의 (이 이름이 UI에 뜹니다)
with DAG(
    'hello_airflow_test', 
    default_args=default_args,
    description='첫 번째 테스트 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:

    # 태스크 정의
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='echo_hello',
        bash_command='echo "Hello Airflow on Kubernetes!"',
    )

    t1 >> t2