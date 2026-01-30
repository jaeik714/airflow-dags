from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime

with DAG(
    'run_scala_spark_job',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'scala', 'hive']
) as dag:

    run_scala = KubernetesPodOperator(
        task_id='execute_scala_jar',
        name='spark-scala-runner',
        namespace='airflow',
        image='apache/spark:3.4.2',
        
        # [핵심 1] 파드 시작 대기 시간 대폭 증가 (10분)
        startup_timeout_seconds=600,
        
        cmds=["/bin/bash", "-c"],
        
        # [핵심 2] 줄바꿈 없이 한 줄로 작성 (문법 에러 원천 차단)
        arguments=[
            "/opt/spark/bin/spark-submit --master local[*] --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 --conf spark.hadoop.fs.s3a.endpoint=http://minio.airflow.svc.cluster.local:9000 --conf spark.hadoop.fs.s3a.access.key=admin --conf spark.hadoop.fs.s3a.secret.key=password123 --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 --conf spark.sql.warehouse.dir=s3a://warehouse/ --class com.example.spark.HiveTest s3a://jars/sparkscalahive_2.12-1.0.jar"
        ],
        
        is_delete_operator_pod=False,
        get_logs=True,
    )