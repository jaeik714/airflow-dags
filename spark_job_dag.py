# spark_job_dag.py
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime

# ==========================================
# 사용자 설정 (여기를 꼭 수정하세요!)
# ==========================================
GIT_REPO_URL = "https://github.com/jaeik714/airflow-dags.git"  # <--- 본인 Git 주소
GIT_BRANCH = "main"
SCRIPT_FILE_NAME = "hive_batch.py"  # 실행할 파일명
# ==========================================

with DAG(
    'run_hive_batch_job',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'hive']
) as dag:

    run_spark = KubernetesPodOperator(
        task_id='execute_spark_script',
        name='spark-hive-runner',
        namespace='airflow',
        image='apache/spark:3.4.2',
        
        # 1. 파일을 담을 빈 그릇(볼륨) 준비
        volumes=[
            k8s.V1Volume(name='code-storage', emptyDir=k8s.V1EmptyDirVolumeSource())
        ],
        
        # 2. 실행 전, Git에서 코드를 다운로드 (Init Container)
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
        
        # 3. Spark 컨테이너가 코드를 볼 수 있게 마운트
        volume_mounts=[
            k8s.V1VolumeMount(name="code-storage", mount_path="/tmp/code")
        ],

        cmds=["/bin/bash", "-c"],
        arguments=[
            f"""
            # 다운로드된 파일이 있는지 확인 (디버깅용)
            echo "Checking file existence..."
            ls -al /tmp/code/repo/{SCRIPT_FILE_NAME}

            # Spark Submit 실행
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