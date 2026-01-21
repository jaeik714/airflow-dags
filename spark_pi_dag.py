from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta
from kubernetes.client import models as k8s  # [추가] K8s 모델 임포트

default_args = {
    'owner': 'jaeik',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '03_spark_pi_cluster_final',
    default_args=default_args,
    description='Spark Pi 분산 처리 (DNS 문제 해결 버전)',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['spark', 'final'],
) as dag:

    spark_pi_cluster = KubernetesPodOperator(
        task_id='run_spark_pi_cluster_final',
        name='spark-pi-driver',
        namespace='airflow',
        image='apache/spark:3.4.2',
        image_pull_policy='IfNotPresent',
        service_account_name='airflow-worker',
        
        # [핵심 1] 파드의 IP 주소를 환경변수(SPARK_DRIVER_HOST)로 가져옵니다.
        env_vars=[
            k8s.V1EnvVar(
                name="SPARK_DRIVER_HOST",
                value_from=k8s.V1EnvVarSource(
                    field_ref=k8s.V1ObjectFieldSelector(field_path="status.podIP")
                )
            )
        ],
        
        cmds=["/bin/bash", "-c"],
        
        arguments=[
            "/opt/spark/bin/spark-submit "
            "--master k8s://https://kubernetes.default.svc "
            "--deploy-mode client "
            
            # [핵심 2] 이름 대신 IP주소($SPARK_DRIVER_HOST)로 통신하라고 설정합니다.
            "--conf spark.driver.host=$SPARK_DRIVER_HOST "
            "--conf spark.driver.bindAddress=0.0.0.0 "
            
            "--conf spark.executor.instances=1 "
            "--conf spark.executor.memory=512m "
            "--conf spark.driver.memory=512m "
            "--conf spark.kubernetes.container.image=apache/spark:3.4.2 "
            "--conf spark.kubernetes.namespace=airflow "
            "--conf spark.kubernetes.authenticate.driver.serviceAccountName=airflow-worker "
            
            # 디버깅용 (로그 확인을 위해 파드 유지)
            "--conf spark.kubernetes.executor.deleteOnTermination=false "
            
            "--class org.apache.spark.examples.SparkPi "
            "/opt/spark/examples/jars/spark-examples_2.12-3.4.2.jar 1000"
        ],
        
        is_delete_operator_pod=False,
        get_logs=True,
    )