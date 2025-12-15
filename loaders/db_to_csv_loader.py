#!/usr/bin/env python
"""
loaders/db_to_csv_loader.py

Run a SQL file and save the single query result into both CSV and Excel files.
This is a minimal, project-local copy of `testing/db_to_csv_tester.py` adjusted
to export the `hkex_dataset` view result into `data/processed/hkex_dataset.csv` and
`data/processed/hkex_dataset.xlsx`.
"""

import sys
from pathlib import Path
import os
import time
import sqlite3
import pandas as pd
from dotenv import load_dotenv

# Ensure project root in sys.path (keeps behaviour similar to original)
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv()

# Use the dedicated select SQL we just added
SQL_FILE = PROJECT_ROOT / "models" / "db_init" / "select_hkex_dataset.sql"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"
DB_PATH = os.getenv("DB_PATH", "data/hongkong.db")

if __name__ == "__main__":
    print(f"\n🚀 Exporting view to CSV + Excel, output directory: '{OUTPUT_DIR}'\n")
    start = time.time()

    if not SQL_FILE.exists():
        print(f"❌ SQL file not found: {SQL_FILE}")
        exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    try:
        sql_text = SQL_FILE.read_text(encoding="utf-8")
        # split by semicolon but keep only non-empty queries
        queries = [q.strip() for q in sql_text.split(";") if q.strip()]

        if not queries:
            print("❌ No queries found in SQL file.")
            exit(1)

        with sqlite3.connect(DB_PATH) as conn:
            for i, query in enumerate(queries, start=1):
                print(f"\n▶ Running query {i}/{len(queries)}...")
                try:
                    df = pd.read_sql(query, conn)
                    # If only one query, use a friendly filename
                    if len(queries) == 1:
                        output_csv = OUTPUT_DIR / "hkex_dataset.csv"
                        output_xlsx = OUTPUT_DIR / "hkex_dataset.xlsx"
                    else:
                        output_csv = OUTPUT_DIR / f"query_{i}.csv"
                        output_xlsx = OUTPUT_DIR / f"query_{i}.xlsx"
                    # write both CSV and Excel
                    df.to_csv(output_csv, index=False)
                    try:
                        df.to_excel(output_xlsx, index=False)
                    except Exception:
                        # If Excel writer is not available, continue after saving CSV
                        print("⚠️ Could not write Excel file (missing engine). CSV saved.")
                    print(f"✅ Saved {len(df)} rows to '{output_csv.name}' and '{output_xlsx.name}'")
                except Exception as e:
                    print(f"❌ Query {i} failed → {e}")

        elapsed = time.time() - start
        print(f"\n✅ Completed export in {elapsed:.1f}s")
    except Exception as e:
        elapsed = time.time() - start
        print(f"\n❌ Script failed after {elapsed:.1f}s → {e}")
