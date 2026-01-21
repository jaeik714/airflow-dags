from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'jaeik',
    'retries': 0, # 디버깅이니까 재시도 금지
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spark_debug',
    default_args=default_args,
    description='Spark 실행 로그 확인용 디버그 DAG',
    schedule_interval=None, # 수동 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['debug'],
) as dag:

    # 이 태스크는 파드를 생성하고 SparkPi를 실행합니다.
    debug_task = KubernetesPodOperator(
        task_id='run_spark_pi',
        name='debug-spark-pod',      # 1. 파드 이름 (kubectl에서 찾기 쉬움)
        namespace='airflow',         # 2. 네임스페이스
        image='apache/spark:3.4.2',  # 3. 검증된 공식 이미지
        
        # 4. 실행 명령어 (스파크 예제 실행)
        cmds=["/bin/bash", "-c"],
        arguments=[
            "/opt/spark/bin/spark-submit "
            "--master local[*] "
            "--class org.apache.spark.examples.SparkPi "
            "/opt/spark/examples/jars/spark-examples_2.12-3.4.2.jar 10"
        ],
        
        # [핵심] 파드가 끝나도 절대 삭제하지 않음 -> 터미널에서 로그 확인 가능!
        is_delete_operator_pod=False,
        
        get_logs=True,
        image_pull_policy='IfNotPresent',
    )