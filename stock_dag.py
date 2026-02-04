# stock_dag.py
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime

# ==========================================
# 사용자 설정 (본인 Git 주소로 변경 필수)
# ==========================================
GIT_REPO_URL = "https://github.com/jaeik714/airflow-dags.git"
GIT_BRANCH = "main"
SCRIPT_FILE_NAME = "stock_loader.py"
# ==========================================

with DAG(
    'load_stock_data',
    start_date=datetime(2026, 2, 1),
    schedule_interval='0 9 * * *', # 매일 아침 9시 실행 (UTC 기준 0시)
    catchup=False,
    tags=['spark', 'stock', 'finance']
) as dag:

    load_stock = KubernetesPodOperator(
        task_id='fetch_stock_prices',
        name='spark-stock-runner',
        namespace='airflow',
        image='apache/spark:3.5.1',
        startup_timeout_seconds=600,
        
        # 1. 코드를 담을 볼륨
        volumes=[
            k8s.V1Volume(name='code-storage', empty_dir=k8s.V1EmptyDirVolumeSource())
        ],
        
        # 2. 코드 다운로드 (Git-Sync)
        init_containers=[
            k8s.V1Container(
                name="fetch-code",
                image="registry.k8s.io/git-sync/git-sync:v3.6.5",
                env=[
                    k8s.V1EnvVar(name="GIT_SYNC_REPO", value=GIT_REPO_URL),
                    k8s.V1EnvVar(name="GIT_SYNC_BRANCH", value=GIT_BRANCH),
                    k8s.V1EnvVar(name="GIT_SYNC_ROOT", value="/tmp/code"),
                    k8s.V1EnvVar(name="GIT_SYNC_DEST", value="repo"),
                    k8s.V1EnvVar(name="GIT_SYNC_ONE_TIME", value="true"),
                ],
                volume_mounts=[
                    k8s.V1VolumeMount(name="code-storage", mount_path="/tmp/code")
                ]
            )
        ],
        
        volume_mounts=[
            k8s.V1VolumeMount(name="code-storage", mount_path="/tmp/code")
        ],

        cmds=["/bin/bash", "-c"],
        
        # 3. PySpark 실행 (한 줄로 작성)
        arguments=[
            f"""
            /opt/spark/bin/spark-submit \
            --master local[*] \
            --conf spark.jars.ivy=/tmp/.ivy2 \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio.airflow.svc.cluster.local:9000 \
            --conf spark.hadoop.fs.s3a.access.key=admin \
            --conf spark.hadoop.fs.s3a.secret.key=password123 \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
            --conf spark.sql.warehouse.dir=s3a://warehouse/ \
            /tmp/code/repo/{SCRIPT_FILE_NAME}
            """
        ],
        is_delete_operator_pod=False,
        get_logs=True,
    )