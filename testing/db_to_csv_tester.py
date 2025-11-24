
#!/usr/bin/env python
"""
csv_tester.py

Run one or more SQL queries from a .sql file against a SQLite database.
Saves each query result into its own CSV file.
"""

import sys
from pathlib import Path
import os
import time
import sqlite3
import pandas as pd
from dotenv import load_dotenv

# ----------------------------
# Ensure project root is in sys.path (optional if needed for helpers)
# ----------------------------
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))

# ----------------------------
# Configuration
# ----------------------------
load_dotenv()  # load environment variables if needed

SQL_FILE = Path(__file__).parent / "db_to_csv_tester_query.sql"  # path to SQL file
OUTPUT_DIR = Path(__file__).parent / "db_to_csv_tester_results"  # directory for CSV outputs
DB_PATH = os.getenv("DB_PATH", "data/hongkong.db")  # SQLite database path

# ----------------------------
# Run CSV test
# ----------------------------
if __name__ == "__main__":
    print(f"\n🚀 Running CSV tester, output directory: '{OUTPUT_DIR}'\n")
    start = time.time()

    if not SQL_FILE.exists():
        print(f"❌ SQL file not found: {SQL_FILE}")
        exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    try:
        # Read and split queries by semicolon
        sql_text = SQL_FILE.read_text()
        queries = [q.strip() for q in sql_text.split(";") if q.strip()]

        if not queries:
            print("❌ No queries found in SQL file.")
            exit(1)

        print(f"ℹ️ Found {len(queries)} queries in {SQL_FILE.name}")

        # Connect and execute each query
        with sqlite3.connect(DB_PATH) as conn:
            for i, query in enumerate(queries, start=1):
                print(f"\n▶ Running query {i}/{len(queries)}...")
                try:
                    df = pd.read_sql(query, conn)
                    output_file = OUTPUT_DIR / f"query_{i}.csv"
                    df.to_csv(output_file, index=False)
                    print(f"✅ Saved {len(df)} rows to '{output_file.name}'")
                except Exception as e:
                    print(f"❌ Query {i} failed → {e}")

        elapsed = time.time() - start
        print(f"\n✅ Completed all queries in {elapsed:.1f}s")
    except Exception as e:
        elapsed = time.time() - start
        print(f"\n❌ Script failed after {elapsed:.1f}s → {e}")