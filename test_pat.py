from dotenv import load_dotenv
from pathlib import Path
import os
import snowflake.connector

load_dotenv(Path(__file__).parent / ".env")

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    authenticator="PROGRAMMATIC_ACCESS_TOKEN",
    token=os.getenv("SNOWFLAKE_PAT"),
)

cur = conn.cursor()

cur.execute("SELECT CURRENT_USER()")
print(cur.fetchone())

cur.execute("CREATE WAREHOUSE IF NOT EXISTS SIGMA_WH")
cur.execute("CREATE DATABASE IF NOT EXISTS CAPSTONE_DB")

cur.execute("USE WAREHOUSE SIGMA_WH")
cur.execute("USE DATABASE CAPSTONE_DB")
cur.execute("USE SCHEMA PUBLIC")

print("Everything works!")