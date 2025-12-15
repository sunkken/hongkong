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

# Require a SQL file path when called from CLI; also expose a function
def export_sql_file(sql_file, db_path=None, output_dir=None):
    """Execute the SQL file and export result(s) to CSV and Excel.

    sql_file: path to .sql file
    db_path: optional path to sqlite db (defaults to env DB_PATH or data/hongkong.db)
    output_dir: optional path to output folder (defaults to data/processed)
    """
    SQL_FILE = Path(sql_file)
    if not SQL_FILE.exists():
        raise FileNotFoundError(f"SQL file not found: {SQL_FILE}")

    DB_PATH_LOCAL = db_path or os.getenv("DB_PATH", "data/hongkong.db")
    OUTPUT_DIR_LOCAL = Path(output_dir) if output_dir else PROJECT_ROOT / "data" / "processed"

    OUTPUT_DIR_LOCAL.mkdir(parents=True, exist_ok=True)

    sql_text = SQL_FILE.read_text(encoding="utf-8")
    queries = [q.strip() for q in sql_text.split(";") if q.strip()]
    if not queries:
        raise ValueError("No queries found in SQL file.")

    sql_stem = SQL_FILE.stem
    if sql_stem.startswith("select_"):
        out_base = sql_stem.replace("select_", "")
    else:
        out_base = sql_stem

    with sqlite3.connect(DB_PATH_LOCAL) as conn:
        for i, query in enumerate(queries, start=1):
            df = pd.read_sql(query, conn)
            if len(queries) == 1:
                output_csv = OUTPUT_DIR_LOCAL / f"{out_base}.csv"
                output_xlsx = OUTPUT_DIR_LOCAL / f"{out_base}.xlsx"
            else:
                output_csv = OUTPUT_DIR_LOCAL / f"{out_base}_query_{i}.csv"
                output_xlsx = OUTPUT_DIR_LOCAL / f"{out_base}_query_{i}.xlsx"
            df.to_csv(output_csv, index=False)
            try:
                df.to_excel(output_xlsx, index=False)
            except Exception:
                # Excel not available; CSV remains
                pass
    return True
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"
DB_PATH = os.getenv("DB_PATH", "data/hongkong.db")

if __name__ == "__main__":
    # CLI entrypoint: require a SQL filepath argument
    if len(sys.argv) < 2:
        print("Usage: python db_to_csv_loader.py <path/to/select_xxx.sql>")
        sys.exit(2)
    sql_arg = sys.argv[1]
    print(f"\n🚀 Exporting view using '{sql_arg}' to CSV + Excel\n")
    start = time.time()
    try:
        export_sql_file(sql_arg)
        elapsed = time.time() - start
        print(f"\n✅ Completed export in {elapsed:.1f}s")
    except Exception as e:
        elapsed = time.time() - start
        print(f"\n❌ Export failed after {elapsed:.1f}s → {e}")
