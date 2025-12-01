
import os
import sqlite3
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def wrds_loader(sql_file: str, table_name: str, db_path: str, isin_list_file: str = None, batch_size: int = 1000):
    """
    sql_file : Path to the SQL file
    table_name : SQLite table name
    db_path : Path to SQLite database
    isin_list_file : Optional path to a text file with one ISIN per line
    batch_size : Number of ISINs per batch
    """
    sql_path = Path(sql_file)
    if not sql_path.exists():
        print(f"❌ SQL file not found: {sql_path}")
        return None

    with open(sql_path, "r") as f:
        sql_template = f.read().rstrip().rstrip(';')

    # Load ISIN list if provided
    isin_list = []
    if isin_list_file:
        isin_path = Path(isin_list_file)
        if not isin_path.exists():
            print(f"❌ ISIN list file not found: {isin_path}")
            return None
        with open(isin_path, "r") as f:
            isin_list = [line.strip() for line in f if line.strip()]
        if not isin_list:
            print(f"❌ ISIN list is empty: {isin_path}")
            return None
        print(f"ℹ️ Loaded {len(isin_list)} ISINs from {isin_path.name}")

    # Check WRDS credentials
    USER = os.getenv("WRDS_USER")
    PASSWORD = os.getenv("WRDS_PASS")
    if not USER or not PASSWORD:
        print("⚠️ WRDS credentials not found. Skipping WRDS loader.")
        return None

    try:
        engine = create_engine(
            f"postgresql://{USER}:{PASSWORD}@wrds-pgdata.wharton.upenn.edu:9737/wrds?sslmode=require"
        )
    except Exception as e:
        print(f"❌ Failed to create WRDS engine: {e}")
        return None

    all_dfs = []
    try:
        batches = [isin_list[i:i+batch_size] for i in range(0, len(isin_list), batch_size)] if isin_list and len(isin_list) > batch_size else [isin_list] if isin_list else [None]
        for idx, batch in enumerate(batches):
            if batch:
                isin_str = ",".join(f"'{s}'" for s in batch)
                sql_query = f"{sql_template} AND isin IN ({isin_str})" if "WHERE" in sql_template.upper() else f"{sql_template} WHERE isin IN ({isin_str})"
            else:
                sql_query = sql_template
            df = pd.read_sql(sql_query, engine)
            all_dfs.append(df)
            print(f"📦 Queried batch {idx+1}: {len(df)} rows" if batch else f"📦 Queried {len(df)} rows")
    except Exception as e:
        print(f"❌ WRDS query failed: {e}")
        return None
    finally:
        engine.dispose()

    final_df = pd.concat(all_dfs, ignore_index=True)
    if final_df.empty:
        print(f"⚠️ Query returned 0 rows for {sql_file}. Skipping.")
        return None

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        final_df.to_sql(table_name, conn, if_exists="replace", index=False)
    print(f"✅ Saved {len(final_df)} rows to '{table_name}'")
    return len(final_df)