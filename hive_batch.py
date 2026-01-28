# hive_batch.py
from pyspark.sql import SparkSession
import os

def run_job():
    print(">>> [Start] Spark Hive Batch Job 시작")

    # 1. Spark 세션 생성 (Hive Metastore 및 MinIO 연동)
    spark = SparkSession.builder \
        .appName("Hive-Batch-Ingestion") \
        .enableHiveSupport() \
        .getOrCreate()

    # 2. 테스트 데이터 생성
    print(">>> 데이터 생성 중...")
    data = [
        ("Iron Man", "Engineering", 5000),
        ("Captain America", "HR", 4000),
        ("Thor", "Operations", 4500),
        ("Hulk", "Engineering", 4800)
    ]
    columns = ["name", "team", "salary"]
    df = spark.createDataFrame(data, schema=columns)

    # 3. Hive DB 생성 및 사용
    spark.sql("CREATE DATABASE IF NOT EXISTS my_data_warehouse")
    spark.sql("USE my_data_warehouse")

    # 4. 데이터 저장 (MinIO에 Parquet으로 저장 + Hive 테이블 등록)
    # saveAsTable은 메타스토어에 테이블 정보를 등록하고, 파일은 warehouse 경로에 저장합니다.
    print(">>> Hive 테이블(employees)에 데이터 저장 중...")
    df.write.mode("overwrite").saveAsTable("employees")

    # 5. 확인 조회
    print(">>> 저장된 데이터 조회:")
    spark.sql("SELECT * FROM employees").show()

    spark.stop()
    print(">>> [End] 작업 완료")

if __name__ == "__main__":
    run_job()