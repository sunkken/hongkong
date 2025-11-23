#!/usr/bin/env python
"""
csv_tester.py

Run a custom SQL query for testing purposes.
Saves results into a local CSV file.
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

SQL_FILE = Path(__file__).parent / "csv_tester_query.sql"  # path to SQL file
OUTPUT_CSV = Path(__file__).parent / "csv_tester_output.csv"  # CSV output file
DB_PATH = "data/hongkong.db"  # SQLite database path

# ----------------------------
# Run CSV test
# ----------------------------
if __name__ == "__main__":
    print(f"\n🚀 Running CSV tester, output: '{OUTPUT_CSV}'\n")
    start = time.time()

    if not SQL_FILE.exists():
        print(f"❌ SQL file not found: {SQL_FILE}")
        exit(1)

    try:
        # read query
        sql_query = SQL_FILE.read_text()

        # connect and execute
        with sqlite3.connect(DB_PATH) as conn:
            df = pd.read_sql(sql_query, conn)

        # save to CSV
        df.to_csv(OUTPUT_CSV, index=False)

        elapsed = time.time() - start
        print(f"\n✅ Query successful. Saved {len(df)} rows to '{OUTPUT_CSV}' ({elapsed:.1f}s)")
    except Exception as e:
        elapsed = time.time() - start
        print(f"\n❌ Query failed after {elapsed:.1f}s → {e}")
