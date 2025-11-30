
# ============================================================
# press_main.py
#
# >>> IMPORTANT: Run this AFTER main.py has prepared listings <<<
#
# PRESS workflow:
#   1) (Optional) Run stock_id scraper to produce CSV
#   2) Load the produced CSV into SQLite table: stock_code_to_id
#   3) Export all stockIds from DB into a text file (one per line)
#
# Keep this file in the hongkong/ folder (project root).
# ============================================================

import os
import time
from dotenv import load_dotenv

# ------------------------------------------------------------
# Imports (project modules)
# ------------------------------------------------------------
from loaders.stock_id_api_scraper import run_scrape
from loaders.db_loader_csv import csv_loader
from helpers.export_stock_ids import export_stock_ids

# ------------------------------------------------------------
# Environment & configuration
# ------------------------------------------------------------
load_dotenv()
DB_PATH = os.getenv("DB_PATH", "data/hongkong.db")
CSV_PATH = os.getenv("PRESS_STOCK_MAPPING_CSV", "data/stock_mapping_filtered.csv")
TABLE_NAME = os.getenv("PRESS_STOCK_MAPPING_TABLE", "stock_code_to_id")
STOCK_IDS_FILE = os.getenv("PRESS_STOCK_IDS_FILE", "data/stock_ids.txt")

# ------------------------------------------------------------
# Step functions
# ------------------------------------------------------------
def step_1_scrape():
    """PRESS Step 1: Run scraper to produce mapping CSV."""
    return run_scrape()

def step_2_load():
    """PRESS Step 2: Load mapping CSV into SQLite."""
    rows_loaded = csv_loader(CSV_PATH, TABLE_NAME, db_path=DB_PATH)
    return rows_loaded

def step_3_export():
    """PRESS Step 3: Export all stockIds from DB into a text file."""
    sql_query = f"SELECT stockId FROM {TABLE_NAME};"
    count = export_stock_ids(DB_PATH, STOCK_IDS_FILE, sql_query)
    return count

# ------------------------------------------------------------
# Steps to run (comment/uncomment to run a subset)
# ------------------------------------------------------------
STEPS = [
    # ("Step 1: stock_id scraper", step_1_scrape),
    # ("Step 2: load CSV -> SQLite (stock_code_to_id)", step_2_load),
    ("Step 3: export stockIds -> text file", step_3_export),
]

# ------------------------------------------------------------
# Main run
# ------------------------------------------------------------
if __name__ == "__main__":
    print("\n🚀 Starting PRESS workflow...\n")
    start_total = time.time()
    results = []

    for name, func in STEPS:
        print(f"▶️ {name}")
        start = time.time()
        try:
            ret = func()
            elapsed = time.time() - start
            print(f"✅ {name} completed in {elapsed:.1f}s\n")
            results.append((name, True, elapsed, ret))
        except Exception as e:
            elapsed = time.time() - start
            print(f"❌ {name} failed after {elapsed:.1f}s → {e}\n")
            results.append((name, False, elapsed, None))

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------
    print("=" * 60)
    print("🏁 PRESS run summary")
    print("=" * 60)
    for name, ok, elapsed, ret in results:
        status = "✅ Success" if ok else "❌ Failed"
        extra = ""
        if name.startswith("Step 1") and ok:
            extra = f" | CSV: {ret}"
        elif name.startswith("Step 2") and ok:
            extra = f" | Rows loaded: {ret}"
        elif name.startswith("Step 3") and ok:
            extra = f" | StockIds exported: {ret}"
        print(f"{name} — {status} ({elapsed:.1f}s){extra}")

    print("-" * 60)
    print(f"⏱️ Total runtime: {time.time() - start_total:.1f}s")
    print("=" * 60 + "\n")

    if any(not ok for _, ok, _, _ in results):
        exit(1)
