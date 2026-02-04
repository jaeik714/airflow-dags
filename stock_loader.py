import os
import sys
import subprocess

# ==========================================
# 1. 라이브러리 설치 (버전 조정)
# ==========================================
def install_libraries():
    install_path = "/tmp/pylibs"
    os.makedirs(install_path, exist_ok=True)
    
    print(f">>> [Setup] Python Version: {sys.version}")
    print(f">>> [Setup] Installing libraries to {install_path}...")

    subprocess.check_call([
        sys.executable, "-m", "pip", "install",
        # 0.2.38: Python 3.8에서 동작하는 비교적 최신 버전
        "yfinance==0.2.38",
        "pandas<2.0.0",
        "lxml",  # yfinance가 html 파싱할 때 필요할 수 있음
        "--target", install_path,
        "--no-cache-dir"
    ])
    
    sys.path.insert(0, install_path)
    print(">>> [Setup] Library installation complete.")

install_libraries()

# ==========================================
# 2. Main Logic
# ==========================================
import yfinance as yf
import pandas as pd
from pyspark.sql import SparkSession
# [핵심] 스키마를 명시하기 위해 타입 임포트
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType

def run_job():
    print(">>> [Start] Spark Session 생성 중...")
    spark = SparkSession.builder \
        .appName("US-Stock-Loader") \
        .enableHiveSupport() \
        .getOrCreate()

    tickers = ["SPY", "QQQ", "NVDA", "AAPL", "TSLA"]
    print(f">>> [Extract] 수집 대상 종목: {tickers}")

    # 데이터 다운로드
    try:
        pdf = yf.download(tickers, period="1mo", group_by='ticker', auto_adjust=True)
    except Exception as e:
        print(f"!!! [Error] yfinance download failed: {e}")
        spark.stop()
        return

    # 데이터가 비어있는지 1차 확인
    if pdf.empty:
        print("!!! [Warning] 다운로드된 데이터가 없습니다. (Yahoo API 이슈 또는 종목명 오류)")
        spark.stop()
        return

    records = []
    print(">>> [Process] 데이터 변환 중...")
    
    for ticker in tickers:
        try:
            # 단일 종목/다중 종목 구조 차이 처리
            if len(tickers) > 1:
                if ticker not in pdf.columns.levels[0]:
                    print(f" - {ticker}: 데이터 없음 (Pass)")
                    continue
                df_ticker = pdf[ticker].copy()
            else:
                df_ticker = pdf.copy()
            
            df_ticker['ticker'] = ticker
            df_ticker.reset_index(inplace=True)
            df_ticker['Date'] = df_ticker['Date'].astype(str)
            
            for _, row in df_ticker.iterrows():
                if pd.isna(row['Close']): continue
                
                records.append((
                    row['Date'], 
                    row['ticker'], 
                    float(row['Close']), 
                    float(row['Open']), 
                    float(row['High']), 
                    float(row['Low']), 
                    int(row['Volume'])
                ))
        except Exception as e:
            print(f"!!! [Error] Processing {ticker}: {e}")

    print(f">>> [Process] 변환된 레코드 수: {len(records)} 건")

    # 3. 데이터가 없을 경우 안전하게 종료
    if len(records) == 0:
        print("!!! [Warning] 처리할 데이터가 0건입니다. 작업을 종료합니다.")
        spark.stop()
        return

    # 4. [핵심] 스키마 강제 지정 (데이터가 비어 있어도 에러 안 나게 함)
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("ticker", StringType(), True),
        StructField("close", FloatType(), True),
        StructField("open", FloatType(), True),
        StructField("high", FloatType(), True),
        StructField("low", FloatType(), True),
        StructField("volume", LongType(), True)
    ])

    spark_df = spark.createDataFrame(records, schema=schema)
    
    print(">>> [Preview] 데이터 미리보기:")
    spark_df.show(5)

    # 5. 저장
    print(">>> [Load] Hive/MinIO에 저장 시작...")
    spark.sql("CREATE DATABASE IF NOT EXISTS stock_db")
    spark.sql("USE stock_db")
    
    # 파티셔닝 저장
    spark_df.write.mode("overwrite").partitionBy("ticker").saveAsTable("daily_prices")
    
    print(">>> [Success] 모든 작업 완료!")
    spark.stop()

if __name__ == "__main__":
    run_job()