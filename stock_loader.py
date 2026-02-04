# stock_loader.py
import os
import sys

# 1. 런타임에 yfinance 라이브러리 강제 설치 (이 파드는 일회용이므로 괜찮습니다)
os.system(f"{sys.executable} -m pip install yfinance pandas")

import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def run_job():
    print(">>> [Start] 미국 주식 데이터 수집 시작")

    # 2. Spark 세션 생성 (S3 및 Hive 설정 포함)
    spark = SparkSession.builder \
        .appName("US-Stock-Loader") \
        .enableHiveSupport() \
        .getOrCreate()

    # 3. 수집할 종목 리스트 (원하는 종목을 추가하세요)
    tickers = ["VOO", "QQQ", "NVDA", "GOOGL", "TSLA", "AAPL", "MSFT"]
    print(f">>> 수집 대상: {tickers}")

    # 4. yfinance로 데이터 다운로드 (최근 1개월 데이터)
    # real-time에 가깝게 하려면 period='1d' 등을 쓰면 됩니다.
    pdf = yf.download(tickers, period="1mo", group_by='ticker', auto_adjust=True)
    
    # 5. 데이터 변환 (Pandas MultiIndex -> Flat Data)
    # yfinance 데이터 구조를 DB에 넣기 좋게 폅니다.
    records = []
    for ticker in tickers:
        try:
            # 종목별 데이터 추출
            df_ticker = pdf[ticker].copy()
            df_ticker['ticker'] = ticker
            df_ticker.reset_index(inplace=True)
            
            # 날짜를 문자열로 변환
            df_ticker['Date'] = df_ticker['Date'].astype(str)
            
            # 필요한 컬럼만 선택 및 이름 변경
            # yfinance 컬럼: Date, Open, High, Low, Close, Volume
            for _, row in df_ticker.iterrows():
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
            print(f"Error processing {ticker}: {e}")

    # 6. Spark DataFrame 생성
    schema = ["date", "ticker", "close", "open", "high", "low", "volume"]
    spark_df = spark.createDataFrame(records, schema=schema)
    
    print(">>> 수집된 데이터 미리보기:")
    spark_df.show(5)

    # 7. Hive/MinIO에 저장
    spark.sql("CREATE DATABASE IF NOT EXISTS stock_db")
    spark.sql("USE stock_db")
    
    # 파티셔닝: ticker별로 폴더를 나눠서 저장 (검색 속도 향상)
    print(">>> 데이터 저장 중 (Partitioned by ticker)...")
    spark_df.write.mode("overwrite").partitionBy("ticker").saveAsTable("daily_prices")
    
    print(">>> 저장 완료!")
    spark.stop()

if __name__ == "__main__":
    run_job()