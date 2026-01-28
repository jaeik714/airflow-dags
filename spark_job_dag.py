# spark_job_dag.py
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime

# ==========================================
# 사용자 설정 (본인 Git 주소로 변경 필수)
# ==========================================
GIT_REPO_URL = "https://github.com/사용자아이디/레포이름.git" 
GIT_BRANCH = "main"
SCRIPT_FILE_NAME = "hive_batch.py"
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
        
        # [수정됨] emptyDir -> empty_dir (파이썬 라이브러리 규격 준수)
        volumes=[
            k8s.V1Volume(name='code-storage', empty_dir=k8s.V1EmptyDirVolumeSource())
        ],
        
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
        arguments=[
            f"""
            echo "Checking file existence..."
            ls -al /tmp/code/repo/{SCRIPT_FILE_NAME}

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