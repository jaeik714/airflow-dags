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
    schedule_interval='0 9 * * *',
    catchup=False,
    tags=['python', 'stock', 'finance']
) as dag:

    load_stock = KubernetesPodOperator(
        task_id='fetch_stock_prices',
        name='stock-fetcher',
        namespace='airflow',
        
        # [핵심 변경] 무거운 Spark 대신 가볍고 호환성 완벽한 Python 3.9 이미지 사용
        image='python:3.9-slim',
        image_pull_policy='Always',
        
        # Git-Sync 볼륨 설정 (기존과 동일)
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

        # [핵심 변경] Spark Submit 대신 Python 실행
        cmds=["/bin/bash", "-c"],
        arguments=[
            f"""
            # 1. 필수 라이브러리 설치 (s3fs, pyarrow 추가)
            pip install --no-cache-dir yfinance pandas s3fs pyarrow

            # 2. 파이썬 스크립트 실행
            python /tmp/code/repo/{SCRIPT_FILE_NAME}
            """
        ],
        get_logs=True,
    )