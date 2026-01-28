from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime

with DAG(
    'spark_hive_minio_ingestion', 
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['debug']
) as dag:

    spark_task = KubernetesPodOperator(
        task_id='run_hive_ingestion',
        name='spark-hive-ingestion-pod',
        namespace='airflow',
        image='apache/spark:3.4.2',
        cmds=["/bin/bash", "-c"],
        arguments=[
            "/opt/spark/bin/spark-submit "
            "--master local[*] "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "--conf spark.hadoop.fs.s3a.endpoint=http://minio.airflow.svc.cluster.local:9000 "
            "--conf spark.hadoop.fs.s3a.access.key=admin "
            "--conf spark.hadoop.fs.s3a.secret.key=password123 "
            "--conf spark.hadoop.fs.s3a.path.style.access=true "
            "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            "--conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 "
            "--conf spark.sql.warehouse.dir=s3a://warehouse/ "
            # [주의] 자기 자신(pyspark_test.py)을 실행하도록 경로 설정
            "/opt/airflow/dags/repo/pyspark_test.py" 
        ],
        is_delete_operator_pod=False,
        get_logs=True,
    )