import os
import yfinance as yf
import pandas as pd
import s3fs

def run_job():
    print(">>> [Start] 미국 주식 데이터 수집 (Python Native Mode)")

    tickers = ["VOO", "SPY", "QQQ", "NVDA", "AAPL", "TSLA"]
    print(f">>> 수집 대상: {tickers}")

    # 1. 데이터 다운로드 (최신 yfinance 사용 가능)
    try:
        pdf = yf.download(tickers, period="1mo", group_by='ticker', auto_adjust=True)
    except Exception as e:
        print(f"!!! Error downloading: {e}")
        return

    if pdf.empty:
        print("!!! No data found.")
        return

    # 2. 데이터 변환 (Spark DataFrame 대신 Pandas DataFrame 정리)
    records = []
    for ticker in tickers:
        try:
            if len(tickers) > 1:
                if ticker not in pdf.columns.levels[0]: continue
                df_ticker = pdf[ticker].copy()
            else:
                df_ticker = pdf.copy()
            
            df_ticker['ticker'] = ticker
            df_ticker.reset_index(inplace=True)
            df_ticker['Date'] = df_ticker['Date'].astype(str)
            
            # 필요한 컬럼만 남기기
            df_ticker = df_ticker[['Date', 'ticker', 'Close', 'Open', 'High', 'Low', 'Volume']]
            # 컬럼명 소문자로 통일 (Hive/Trino 호환성 위해)
            df_ticker.columns = ['date', 'ticker', 'close', 'open', 'high', 'low', 'volume']
            
            records.append(df_ticker)
        except Exception as e:
            print(f"Error processing {ticker}: {e}")

    if not records:
        print("!!! No records to save.")
        return

    # 모든 종목 데이터를 하나로 합치기
    final_df = pd.concat(records)
    print(f">>> 수집된 데이터: {len(final_df)} 건")
    print(final_df.head())

    # 3. MinIO에 Parquet으로 저장 (S3FS 사용)
    print(">>> MinIO 저장 시작...")
    
    # MinIO 접속 정보
    minio_endpoint = "http://minio.airflow.svc.cluster.local:9000"
    s3 = s3fs.S3FileSystem(
        key='admin',
        secret='password123',
        client_kwargs={'endpoint_url': minio_endpoint}
    )

    # 저장 경로: s3://warehouse/daily_prices
    save_path = "s3://warehouse/daily_prices"

    try:
        # partition_cols=['ticker']를 쓰면 폴더가 ticker=NVDA 식으로 예쁘게 생깁니다.
        final_df.to_parquet(
            save_path,
            filesystem=s3,
            partition_cols=['ticker'],
            index=False
        )
        print(">>> [Success] 저장 완료!")
        
    except Exception as e:
        print(f"!!! 저장 실패: {e}")
        # 혹시 버킷이 없어서 에러나면 알려주기 위함
        print("Tip: MinIO에 'warehouse' 버킷이 있는지 확인하세요.")

if __name__ == "__main__":
    run_job()