from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta
from kubernetes.client import models as k8s

default_args = {
    'owner': 'jaeik',
    'retries': 0,
}

with DAG(
    'spark_hive_minio_ingestion',
    default_args=default_args,
    description='Spark를 이용한 Hive/MinIO 데이터 적재 테스트',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['spark', 'hive', 'minio'],
) as dag:

    # 1. Spark 작업 실행 (PySpark)
    spark_task = KubernetesPodOperator(
        task_id='run_hive_ingestion',
        name='spark-hive-ingestion-pod',
        namespace='airflow',
        image='apache/spark:3.4.2',  # PySpark가 포함된 공식 이미지
        
        # [핵심] Spark-Submit 명령어 구성
        cmds=["/bin/bash", "-c"],
        arguments=[
            "/opt/spark/bin/spark-submit "
            "--master local[*] "
            # A. S3 연결을 위한 필수 패키지 (자동 다운로드)
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            # B. MinIO 접속 정보 설정
            "--conf spark.hadoop.fs.s3a.endpoint=http://minio.airflow.svc.cluster.local:9000 "
            "--conf spark.hadoop.fs.s3a.access.key=admin "
            "--conf spark.hadoop.fs.s3a.secret.key=password123 "
            "--conf spark.hadoop.fs.s3a.path.style.access=true "
            "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            # C. Hive Metastore 연결 설정
            "--conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 "
            "--conf spark.sql.warehouse.dir=s3a://warehouse/ "
            # D. 실행할 스크립트 경로 (Git-Sync된 경로)
            "/opt/airflow/dags/repo/hive_batch.py" 
        ],
        
        # 2. DAG 파일이 있는 볼륨 마운트 (Git-Sync를 쓰는 경우)
        # 만약 볼륨 이름이 다르다면 환경에 맞춰 수정이 필요합니다.
        volumes=[
            k8s.V1Volume(
                name='dags-data',
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='airflow-dags')
            )
        ],
        volume_mounts=[
            k8s.V1VolumeMount(
                name='dags-data',
                mount_path='/opt/airflow/dags',
                read_only=True
            )
        ],
        
        is_delete_operator_pod=False,
        get_logs=True,
    )