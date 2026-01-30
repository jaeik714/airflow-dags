from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
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
        startup_timeout_seconds=600,
        
        # 1. JAR 파일을 저장할 공유 볼륨 생성
        volumes=[
            k8s.V1Volume(name='jar-volume', empty_dir=k8s.V1EmptyDirVolumeSource())
        ],
        
        # 2. Spark 실행 전, MinIO에서 JAR 다운로드 (Init Container)
        init_containers=[
            k8s.V1Container(
                name="download-jar",
                image="amazon/aws-cli",  # AWS CLI 도구 사용
                env=[
                    k8s.V1EnvVar(name="AWS_ACCESS_KEY_ID", value="admin"),
                    k8s.V1EnvVar(name="AWS_SECRET_ACCESS_KEY", value="password123"),
                    k8s.V1EnvVar(name="AWS_ENDPOINT_URL", value="http://minio.airflow.svc.cluster.local:9000"),
                    k8s.V1EnvVar(name="AWS_REGION", value="us-east-1"), # MinIO는 기본적으로 이 리전 사용
                ],
                # s3://jars/... 경로에서 파일을 받아 /data/app.jar로 저장
                command=["sh", "-c", "aws s3 cp s3://jars/sparkscalahive_2.12-1.0.jar /data/app.jar --endpoint-url http://minio.airflow.svc.cluster.local:9000 --no-verify-ssl"],
                volume_mounts=[
                    k8s.V1VolumeMount(name="jar-volume", mount_path="/data")
                ]
            )
        ],
        
        # 3. Main 컨테이너 설정
        volume_mounts=[
            k8s.V1VolumeMount(name="jar-volume", mount_path="/data")
        ],
        
        cmds=["/bin/bash", "-c"],
        
        # [핵심] 이제 JAR 파일 경로는 s3a://가 아니라 로컬 경로(/data/app.jar)입니다.
        arguments=[
            "/opt/spark/bin/spark-submit --master local[*] --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 --conf spark.hadoop.fs.s3a.endpoint=http://minio.airflow.svc.cluster.local:9000 --conf spark.hadoop.fs.s3a.access.key=admin --conf spark.hadoop.fs.s3a.secret.key=password123 --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 --conf spark.sql.warehouse.dir=s3a://warehouse/ --class com.example.spark.HiveTest /data/app.jar"
        ],
        
        is_delete_operator_pod=False,
        get_logs=True,
    )