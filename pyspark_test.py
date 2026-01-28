from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime
from kubernetes.client import models as k8s

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
        
        # 1. 공용 공간(임시 볼륨) 생성
        volumes=[
            k8s.V1Volume(name='git-volume', emptyDir=k8s.V1EmptyDirVolumeSource())
        ],
        
        # 2. 파일 먼저 받아오기 (Init Container)
        init_containers=[
            k8s.V1Container(
                name="git-sync",
                image="registry.k8s.io/git-sync/git-sync:v3.6.5", # git-sync 공식 이미지
                env=[
                    k8s.V1EnvVar(name="GIT_SYNC_REPO", value="https://github.com/jaeik714/airflow-dags.git"),
                    k8s.V1EnvVar(name="GIT_SYNC_BRANCH", value="main"),
                    k8s.V1EnvVar(name="GIT_SYNC_ROOT", value="/opt/airflow/dags"),
                    k8s.V1EnvVar(name="GIT_SYNC_ONE_TIME", value="true"),
                ],
                volume_mounts=[
                    k8s.V1VolumeMount(name="git-volume", mount_path="/opt/airflow/dags")
                ]
            )
        ],
        
        # 3. 메인 Spark 컨테이너 설정
        volume_mounts=[
            k8s.V1VolumeMount(name="git-volume", mount_path="/opt/airflow/dags")
        ],
        
        cmds=["/bin/bash", "-c"],
        arguments=[
            "/opt/spark/bin/spark-submit "
            "--master local[*] "
            "--conf spark.jars.ivy=/tmp/.ivy2 "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "--conf spark.hadoop.fs.s3a.endpoint=http://minio.airflow.svc.cluster.local:9000 "
            "--conf spark.hadoop.fs.s3a.access.key=admin "
            "--conf spark.hadoop.fs.s3a.secret.key=password123 "
            "--conf spark.hadoop.fs.s3a.path.style.access=true "
            "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            "--conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 "
            "--conf spark.sql.warehouse.dir=s3a://warehouse/ "
            # 이제 파일이 존재하므로 직접 경로를 지정합니다!
            "/opt/airflow/dags/repo/pyspark_test.py" 
        ],
        is_delete_operator_pod=False,
        get_logs=True,
    )