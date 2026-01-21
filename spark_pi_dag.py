from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta

# 기본 설정
default_args = {
    'owner': 'jaeik',
    'retries': 0, # 디버깅 중에는 재시도 끄기
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spark_pi_cluster',
    default_args=default_args,
    description='Spark Pi 분산 처리 (Driver 1개 + Executor 2개)',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['spark', 'cluster'],
) as dag:

    spark_pi_cluster = KubernetesPodOperator(
        task_id='run_spark_pi_cluster',
        name='spark-pi-driver',      # 파드 이름
        namespace='airflow',          # 네임스페이스
        image='apache/spark:3.4.2',   # debug에서 성공한 그 이미지
        image_pull_policy='IfNotPresent',
        
        # [중요] Airflow Worker의 권한을 사용하여 Executor를 생성합니다.
        service_account_name='airflow-worker', 
        
        cmds=["/bin/bash", "-c"],
        
        # Spark Submit 명령어 (분산 모드 핵심 설정)
        arguments=[
            "/opt/spark/bin/spark-submit "
            "--master k8s://https://kubernetes.default.svc "  # K8s API 서버 주소
            "--deploy-mode client "                           # 이 파드가 곧 Driver다!
            "--conf spark.executor.instances=2 "              # Executor 2개 생성 (분산 처리)
            "--conf spark.kubernetes.container.image=apache/spark:3.4.2 " # Executor도 같은 이미지 사용
            "--conf spark.kubernetes.namespace=airflow "      # Executor가 생성될 위치
            "--conf spark.kubernetes.authenticate.driver.serviceAccountName=airflow-worker " # Executor 권한
            "--class org.apache.spark.examples.SparkPi "      # 실행할 클래스
            "/opt/spark/examples/jars/spark-examples_2.12-3.4.2.jar 1000" # 반복 횟수 1000
        ],
        
        # 디버깅용 설정 (성공/실패 후에도 파드 남겨두기)
        is_delete_operator_pod=False,
        get_logs=True,
    )