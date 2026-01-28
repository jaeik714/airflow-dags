from pyspark.sql import SparkSession
import os

# 1. Spark 세션 생성 (Hive 지원 활성화)
spark = SparkSession.builder \
    .appName("Spark-Hive-MinIO-Test") \
    .config("spark.sql.warehouse.dir", "s3a://warehouse/") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 2. 샘플 데이터 생성
data = [("James", "Sales", 3000), ("Michael", "Sales", 4600), ("Robert", "Sales", 4100)]
columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data, schema=columns)

# 3. Hive 데이터베이스 및 테이블 생성
spark.sql("CREATE DATABASE IF NOT EXISTS my_db")
spark.sql("USE my_db")

# 4. 데이터를 Hive 테이블로 저장 (MinIO에 parquet 형태로 저장됨)
df.write.mode("overwrite").saveAsTable("employees")

# 5. 저장된 데이터 확인
spark.sql("SELECT * FROM employees").show()

spark.stop()