#!/usr/bin/env python
"""
loaders/db_to_file_loader.py

Run a SQL file and save the single query result into CSV, Excel, or TXT files.
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
import re
from openpyxl import load_workbook
from dotenv import load_dotenv

# Ensure project root in sys.path (keeps behaviour similar to original)
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv()

# Require a SQL file path when called from CLI; also expose a function
def export_sql_file(sql_file, db_path=None, output_dir=None, output_format='csv', output_file=None):
    """Execute the SQL file and export result(s) to CSV, Excel, and/or TXT.

    sql_file: path to .sql file
    db_path: optional path to sqlite db (defaults to env DB_PATH or data/hongkong.db)
    output_dir: optional path to output folder (defaults to data/processed)
    output_format: 'csv', 'xlsx', or 'txt' (default: 'csv')
    output_file: optional specific output file path for txt format (overrides default naming)
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
                output_txt = Path(output_file) if output_file else OUTPUT_DIR_LOCAL / f"{out_base}.txt"
            else:
                output_csv = OUTPUT_DIR_LOCAL / f"{out_base}_query_{i}.csv"
                output_xlsx = OUTPUT_DIR_LOCAL / f"{out_base}_query_{i}.xlsx"
                output_txt = Path(output_file) if output_file else OUTPUT_DIR_LOCAL / f"{out_base}_query_{i}.txt"
            if output_format in ['csv']:
                df.to_csv(output_csv, index=False)
            if output_format in ['xlsx']:
                df.to_excel(output_xlsx, index=False, engine='openpyxl')
            if output_format in ['txt']:
                if df.shape[1] != 1:
                    raise ValueError("TXT export requires single-column query result.")
                values = sorted(df.iloc[:, 0].dropna().unique())
                with open(output_txt, "w") as f:
                    for value in values:
                        f.write(f"{value}\n")
    return True
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"
DB_PATH = os.getenv("DB_PATH", "data/hongkong.db")

if __name__ == "__main__":
    # CLI entrypoint: require a SQL filepath argument, optional format
    if len(sys.argv) < 2:
        print("Usage: python db_to_file_loader.py <path/to/select_xxx.sql> [csv|xlsx|txt]")
        sys.exit(2)
    sql_arg = sys.argv[1]
    output_format = 'csv'  # default
    if len(sys.argv) >= 3:
        output_format = sys.argv[2].lower()
        if output_format not in ['csv', 'xlsx', 'txt']:
            print("Invalid format. Use 'csv', 'xlsx', or 'txt'.")
            sys.exit(2)
    print(f"\n🚀 Exporting view using '{sql_arg}' to {output_format.upper()}\n")
    start = time.time()
    try:
        export_sql_file(sql_arg, output_format=output_format)
        elapsed = time.time() - start
        print(f"\n✅ Completed export in {elapsed:.1f}s")
    except Exception as e:
        elapsed = time.time() - start
        print(f"\n❌ Export failed after {elapsed:.1f}s → {e}")
