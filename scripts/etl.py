import os
import pandas as pd
from azure.storage.blob import BlobClient
from sqlalchemy import create_engine
from io import StringIO

# 環境変数から設定を取得
ST_URL = os.environ["ST_URL"]      # 例: https://pbistoragexxxx.blob.core.windows.net
SAS_TOKEN = os.environ["SAS_TOKEN"]    # SAS トークン（?sv=...）
SQL_URI = os.environ["SQL_URI"]        # 例: mssql+pyodbc://sqladmin:P%40ssw0rd!123@pbisqlsrvxxxx.database.windows.net/pbi_demo?driver=ODBC+Driver+18+for+SQL+Server
BLOB_PATH = os.environ["BLOB_PATH"]  # アップロード時に指定したパス

# 1. CSV 取得
blob = BlobClient(account_url=ST_URL, container_name="demo", blob_name=BLOB_PATH, credential=SAS_TOKEN)
csv_bytes = blob.download_blob().readall()
df = pd.read_csv(StringIO(csv_bytes.decode())) 

# 2. 税抜き価格カラムを追加（消費税10%として）
df["amount_excl_tax"] = (df["amount"] / 1.10).round(2)

# 3. Azure SQL Database へ INSERT（テーブル存在チェックも含む）
engine = create_engine(SQL_URI, fast_executemany=True)
with engine.begin() as conn:
    conn.execute(text("""
        IF OBJECT_ID('dbo.Payments','U') IS NULL
        CREATE TABLE dbo.Payments(
            payment_id INT PRIMARY KEY,
            user_id INT,
            amount DECIMAL(10,2),
            amount_excl_tax DECIMAL(10,2)
        )
    """))
    # ここでは新データを追加するため、すでに存在するレコードとの重複はエラーになる可能性に留意
    df.to_sql("Payments", conn, if_exists="append", index=False)
print("ETL 完了")
