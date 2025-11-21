import os
import sqlite3
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# ----------------------------
# Main process
# ----------------------------
def wrds_loader(sql_file: str, table_name: str, db_path: str):
    """
    sql_file : Path to the SQL file (e.g., models/dl_funda_a.sql)
    table_name : SQLite table name
    db_path : Path to SQLite database
    """
    sql_path = Path(sql_file)
    if not sql_path.exists():
        print(f"❌ SQL file not found: {sql_path}")
        return 0

    with open(sql_path, "r") as f:
        sql_query = f.read()

    # ----------------------------
    # Connect to WRDS
    # ----------------------------
    USER = os.getenv("WRDS_USER")
    PASSWORD = os.getenv("WRDS_PASS")
    engine = create_engine(
        f"postgresql://{USER}:{PASSWORD}@wrds-pgdata.wharton.upenn.edu:9737/wrds?sslmode=require"
    )

    try:
        df = pd.read_sql(sql_query, engine)
        print(f"📦 Queried WRDS: {sql_path.name} ({len(df)} rows)")
    except Exception as e:
        print(f"❌ WRDS query failed: {e}")
        return 0
    finally:
        engine.dispose()

    # ----------------------------
    # Save to SQLite
    # ----------------------------
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"✅ Saved {len(df)} rows to table '{table_name}' in {db_path}")

    return len(df)


# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    wrds_loader("models/download_funda_a.sql", "wrds_funda_a", "data/hongkong.db")
