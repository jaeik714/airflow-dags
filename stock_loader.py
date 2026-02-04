import os
import sys
import subprocess

# ==========================================
# [Fix] 권한 문제 해결을 위한 라이브러리 설치 로직
# ==========================================
def install_libraries():
    # 1. 누구나 쓸 수 있는 임시 경로 지정
    install_path = "/tmp/pylibs"
    os.makedirs(install_path, exist_ok=True)

    print(f">>> 라이브러리를 {install_path} 에 설치합니다...")
    
    # [디버깅] 현재 파이썬 버전 출력 (로그에서 확인용)
    print(f">>> Python Version: {sys.version}")

    # 2. pip install 실행 (버전 고정)
    # yfinance==0.2.33 : Python 3.8 호환 안정 버전
    # multitasking==0.0.9 : type[] 문법 에러 방지용 구버전
    subprocess.check_call([
        sys.executable, "-m", "pip", "install",
        "yfinance==0.2.33", 
        "multitasking==0.0.9",
        "pandas<2.0.0",   # Spark 3.x와 호환성 좋은 pandas 1.x 버전 유지
        "--target", install_path,
        "--no-cache-dir"
    ])

    # 3. 파이썬이 이 경로를 참조하도록 맨 앞에 추가
    sys.path.insert(0, install_path)
    print(">>> 라이브러리 설치 및 경로 등록 완료")

# 라이브러리 설치 함수 실행 (import yfinance 전에 실행되어야 함)
install_libraries()

# 이제 안전하게 import 가능
import yfinance as yf
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def run_job():
    print(">>> [Start] 미국 주식 데이터 수집 시작")

    # Spark 세션 생성
    spark = SparkSession.builder \
        .appName("US-Stock-Loader") \
        .enableHiveSupport() \
        .getOrCreate()

    # ... (이하 로직은 기존과 동일) ...
    tickers = ["SPY", "QQQ", "NVDA", "AAPL", "TSLA"]
    print(f">>> 수집 대상: {tickers}")

    # yfinance로 데이터 다운로드
    pdf = yf.download(tickers, period="1mo", group_by='ticker', auto_adjust=True)
    
    records = []
    for ticker in tickers:
        try:
            # 단일 종목일 경우와 다중 종목일 경우 구조가 다를 수 있음
            # 멀티 인덱스 처리를 위해 ticker 레벨 확인
            if len(tickers) > 1:
                df_ticker = pdf[ticker].copy()
            else:
                df_ticker = pdf.copy() # 종목이 하나면 바로 사용
            
            df_ticker['ticker'] = ticker
            df_ticker.reset_index(inplace=True)
            
            df_ticker['Date'] = df_ticker['Date'].astype(str)
            
            for _, row in df_ticker.iterrows():
                # yfinance 데이터 유효성 검사 (NaN 방지)
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
            print(f"Error processing {ticker}: {e}")

    schema = ["date", "ticker", "close", "open", "high", "low", "volume"]
    spark_df = spark.createDataFrame(records, schema=schema)
    
    print(">>> 수집된 데이터 미리보기:")
    spark_df.show(5)

    spark.sql("CREATE DATABASE IF NOT EXISTS stock_db")
    spark.sql("USE stock_db")
    
    # 파티셔닝 저장 (덮어쓰기 모드)
    print(">>> 데이터 저장 중 (Partitioned by ticker)...")
    spark_df.write.mode("overwrite").partitionBy("ticker").saveAsTable("daily_prices")
    
    print(">>> 저장 완료!")
    spark.stop()

if __name__ == "__main__":
    run_job()