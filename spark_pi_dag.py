from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from airflow.utils.dates import days_ago

# 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

with DAG(
    'spark_pi_k8s',
    default_args=default_args,
    description='Spark Pi calculation on K8s (Native)',
    schedule_interval=None,
    tags=['spark', 'k8s'],
    catchup=False,
) as dag:

    # [수정] SparkOperator 대신 만능 도구인 KubernetesPodOperator 사용
    t1 = KubernetesPodOperator(
        task_id='spark_pi_task',
        name='spark-pi-job',
        namespace='spark-work',
        service_account_name='spark',  # 매우 중요: Spark 계정 권한 사용
        image='my-spark:3.5.0',
        image_pull_policy='Never',     # 로컬 이미지 사용 (Kind 환경)
        
        # 파드가 뜰 때 자기 자신의 IP를 알 수 있도록 환경변수 설정
        env_vars=[
            k8s.V1EnvVar(
                name="SPARK_LOCAL_IP",
                value_from=k8s.V1EnvVarSource(
                    field_ref=k8s.V1ObjectFieldSelector(field_path="status.podIP")
                )
            )
        ],
        
        # 실행할 명령어 (아까 YAML의 args 부분과 동일)
        arguments=[
            "/bin/bash", "-c",
            """
            /opt/spark/bin/spark-submit \
            --master k8s://https://kubernetes.default.svc \
            --deploy-mode client \
            --conf spark.kubernetes.container.image=my-spark:3.5.0 \
            --conf spark.kubernetes.container.image.pullPolicy=Never \
            --conf spark.kubernetes.namespace=spark-work \
            --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
            --conf spark.executor.instances=2 \
            --conf spark.driver.host=$SPARK_LOCAL_IP \
            --conf spark.driver.bindAddress=$SPARK_LOCAL_IP \
            --conf spark.executor.memory=512m \
            --conf spark.driver.memory=512m \
            --class org.apache.spark.examples.SparkPi \
            local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar 100
            """
        ],
        
        # 작업이 끝나면 파드를 삭제할지 여부 (디버깅 위해 False 추천, 완료 후엔 True)
        is_delete_operator_pod=False,
        get_logs=True,
    )