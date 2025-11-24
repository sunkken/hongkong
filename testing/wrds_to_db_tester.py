#!/usr/bin/env python
"""
wrds_tester.py

Run custom WRDS queries for testing purposes.
Saves results into a local SQLite database table (default: test_wrds_tmp).
"""

import sys
from pathlib import Path
import os
import time
from dotenv import load_dotenv

# ----------------------------
# Ensure project root is in sys.path so loaders import works
# ----------------------------
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))

from loaders.db_loader_wrds import wrds_loader

# ----------------------------
# Configuration
# ----------------------------
load_dotenv()  # make sure WRDS credentials are loaded

SQL_FILE = Path(__file__).parent / "wrds_to_db_tester_query.sql" # path to SQL file (from project root)
TABLE_NAME = "wrds_to_db_tester_tmp"                             # table name to save results
DB_PATH = "data/hongkong.db"                                     # SQLite database path
ISIN_LIST_FILE = "data/isin_list.txt"                            # optional ISIN list file, e.g., "data/isin_list.txt"
BATCH_SIZE = 1000                                                # batch size for large ISIN lists

# ----------------------------
# Run WRDS test
# ----------------------------
if __name__ == "__main__":
    print(f"\n🚀 Running WRDS tester for '{TABLE_NAME}'\n")
    start = time.time()

    try:
        rows_loaded = wrds_loader(
            sql_file=str(SQL_FILE),
            table_name=TABLE_NAME,
            db_path=DB_PATH,
            isin_list_file=ISIN_LIST_FILE,
            batch_size=BATCH_SIZE
        )
        elapsed = time.time() - start
        print(f"\n✅ Finished. Loaded {rows_loaded} rows into '{TABLE_NAME}' ({elapsed:.1f}s)")
    except Exception as e:
        elapsed = time.time() - start
        print(f"\n❌ Test query failed after {elapsed:.1f}s → {e}")