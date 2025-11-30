
# ============================================================
# press_main.py
#
# >>> IMPORTANT: Run this AFTER main.py has prepared listings <<<
#
# PRESS workflow:
#   1) (Optional) Run stock_id scraper to produce CSV
#   2) Load the produced CSV into SQLite table: stock_code_to_id
#   3) Export all stockIds from DB into a text file (one per line)
#   4a) Build pending query list from full + ignore (skip existing CSVs)
#   4) Download press releases for pending IDs (auto-update ignore list)
#
# Keep this file in the hongkong/ folder (project root).
# ============================================================

import os
import time
from pathlib import Path
from dotenv import load_dotenv

# ------------------------------------------------------------
# Imports (project modules)
# ------------------------------------------------------------
from loaders.stock_id_api_scraper import run_scrape
from loaders.db_loader_csv import csv_loader
from helpers.export_stock_ids import export_stock_ids
from loaders import download_hkex_press_release as pressdl
from helpers.query_list_builder import build_query_list, update_ignore_from_summary

# ------------------------------------------------------------
# Environment & configuration
# ------------------------------------------------------------
load_dotenv()

DB_PATH          = os.getenv("DB_PATH", "data/hongkong.db")
CSV_PATH         = os.getenv("PRESS_STOCK_MAPPING_CSV", "data/stock_mapping_filtered.csv")
TABLE_NAME       = os.getenv("PRESS_STOCK_MAPPING_TABLE", "stock_code_to_id")

# ID list files (now in data/lists)
FULL_IDS_FILE    = os.getenv("PRESS_STOCK_IDS_FILE", "data/lists/stock_ids.txt")
IGNORE_IDS_FILE  = os.getenv("PRESS_IGNORE_IDS_FILE", "data/lists/ignore_ids.txt")
PENDING_IDS_FILE = os.getenv("PRESS_PENDING_IDS_FILE", "data/lists/pending_ids.txt")

# Press data folder and summary
SAVE_DIR         = os.getenv("PRESS_SAVE_DIR", "data/press")
SUMMARY_CSV      = os.getenv("PRESS_SUMMARY_CSV", "data/press/press_summary.csv")

# Concurrency control for Step 4
PRESS_MAX_WORKERS = os.getenv("PRESS_MAX_WORKERS")
if PRESS_MAX_WORKERS:
    try:
        pressdl.MAX_WORKERS = int(PRESS_MAX_WORKERS)
    except ValueError:
        print(f"⚠️ PRESS_MAX_WORKERS='{PRESS_MAX_WORKERS}' not integer; using {pressdl.MAX_WORKERS}")

# ------------------------------------------------------------
# Step functions
# ------------------------------------------------------------
def step_1_scrape():
    """Run scraper to produce mapping CSV."""
    return run_scrape()

def step_2_load():
    """Load mapping CSV into SQLite."""
    return csv_loader(CSV_PATH, TABLE_NAME, db_path=DB_PATH)

def step_3_export():
    """Export all stockIds from DB into a text file."""
    sql_query = f"SELECT stockId FROM {TABLE_NAME};"
    return export_stock_ids(DB_PATH, FULL_IDS_FILE, sql_query)

def step_4a_build_pending():
    """
    Build pending query list from FULL_IDS_FILE and IGNORE_IDS_FILE.
    Also skips IDs that already have per-ID CSVs in SAVE_DIR.
    """
    totals = build_query_list(
        full_list_file=FULL_IDS_FILE,
        ignore_list_file=IGNORE_IDS_FILE,
        output_pending_file=PENDING_IDS_FILE,
        skip_existing=True,
        existing_save_dir=SAVE_DIR,
        existing_pattern="press_releases_*.csv",
    )
    print(f"ℹ️ Build pending: full={totals[0]} ignore={totals[1]} existing={totals[2]} pending={totals[3]}")
    return totals[3]

def step_4_download_and_update():
    """
    Download press releases for IDs in PENDING_IDS_FILE, then automatically
    update the ignore list using the summary CSV.
    """
    txt_file = Path(PENDING_IDS_FILE)
    if not txt_file.exists():
        raise FileNotFoundError(f"Pending IDs file not found: {txt_file}")

    with txt_file.open("r", encoding="utf-8") as f:
        pending = [line.strip() for line in f if line.strip()]

    if not pending:
        print("✅ No pending IDs to download.")
        return 0

    print(f"➡️ Downloading for {len(pending)} pending IDs...")
    pressdl.run_parallel(pending)

    # Auto-update ignore list from summary (adds 'saved' IDs only)
    added = update_ignore_from_summary(IGNORE_IDS_FILE, SUMMARY_CSV)
    print(f"✅ Ignore list updated with {added} IDs (from summary)")
    return len(pending)

# ------------------------------------------------------------
# Steps to run (comment/uncomment to run a subset)
# ------------------------------------------------------------
STEPS = [
    # ("Step 1: stock_id scraper", step_1_scrape),
    # ("Step 2: load CSV -> SQLite", step_2_load),
    ("Step 3: export stockIds -> text file", step_3_export),
    ("Step 4a: build pending list", step_4a_build_pending),
    ("Step 4: download & auto-update ignore list", step_4_download_and_update),
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
        if name.startswith("Step 4a") and ok:
            extra = f" | Pending IDs: {ret}"
        elif name.startswith("Step 4") and ok:
            extra = f" | IDs processed: {ret}"
        print(f"{name} — {status} ({elapsed:.1f}s){extra}")

    print("-" * 60)
    print(f"⏱️ Total runtime: {time.time() - start_total:.1f}s")
    print("=" * 60 + "\n")

    if any(not ok for _, ok, _, _ in results):
        exit(1)