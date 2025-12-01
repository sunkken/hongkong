
# ============================================================
# main.py
#
# Orchestrates HKEX and WRDS data workflows:
# - Downloads and cleans HKEX data, pushes into SQLite database
# - Downloads WRDS data, pushes into SQLite database
# - Downloads WRDS data filtered by ISINs in HKEX, pushes into SQLite database
# ============================================================

import runpy
import time
import os
from contextlib import contextmanager, redirect_stdout, redirect_stderr
from io import StringIO
from dotenv import load_dotenv
from loaders.db_loader_csv import csv_loader
from loaders.db_loader_wrds import wrds_loader
from helpers.export_isins import export_unique_isins

# ------------------------------------------------------------
# Silent mode context manager
# ------------------------------------------------------------
SILENT_MODE = False  # set True to suppress output of loaders/scripts
@contextmanager
def suppress_output():
    if SILENT_MODE:
        with StringIO() as buf, redirect_stdout(buf), redirect_stderr(buf):
            yield
    else:
        yield

# ------------------------------------------------------------
# Load environment variables
# ------------------------------------------------------------
load_dotenv()
DB_PATH = os.getenv("DB_PATH", "data/hongkong.db")  # fallback if .env not found

# ------------------------------------------------------------
# Scripts to run (downloaders and data cleaners)
# ------------------------------------------------------------
scripts = [
    "loaders/download_hkex_isino.py",
    "loaders/download_hkex_listings.py",
    "loaders/download_hkex_auditor_reports.py",
    "loaders/download_hkex_auditor_pdfs.py",
    "modules/hkex_xlsx_converter.py",
    "modules/hkex_isino_bronze.py",
    "modules/hkex_isino_stock_types.py",
    "modules/hkex_isino_national_agencies.py",
    "modules/hkex_main_bronze.py",
    "modules/hkex_gem_bronze.py",
    "modules/hkex_main_silver.py",
    "modules/hkex_gem_silver.py",
]

# ------------------------------------------------------------
# Files and tables for CSV loader
# ------------------------------------------------------------
CSV_LOADERS = [
    {"csv_file": "data/silver/gem_silver.csv", "table_name": "hkex_gem"},
    {"csv_file": "data/silver/main_silver.csv", "table_name": "hkex_main"},
    {"csv_file": "data/bronze/isino_stock_types.csv", "table_name": "desc_hkex_stock_types"},
    {"csv_file": "data/bronze/isino_national_agencies.csv", "table_name": "desc_hkex_national_agencies"},
    {"csv_file": "data/raw/auditor_reports.csv", "table_name": "hkex_auditor_reports"},
]

# ------------------------------------------------------------
# ISIN export configuration
# ------------------------------------------------------------
ISIN_EXPORT_QUERY_FILE = "models/db_init/isin_export.sql"
ISIN_EXPORT_OUTPUT = "data/isin_list.txt"

# ------------------------------------------------------------
# WRDS loaders configuration
# ------------------------------------------------------------
WRDS_LOADERS = [
    {"sql_file": "models/db_init/dl_funda_a_170.sql", "table_name": "funda_a_170"},
    {"sql_file": "models/db_init/dl_funda_q_170.sql", "table_name": "funda_q_170"},
    {"sql_file": "models/db_init/dl_funda_a_isin.sql", "table_name": "funda_a_isin", "isin_list_file": "data/isin_list.txt"},
    {"sql_file": "models/db_init/dl_funda_q_isin.sql", "table_name": "funda_q_isin", "isin_list_file": "data/isin_list.txt"},
]

# ------------------------------------------------------------
# Stock Code export configuration
# ------------------------------------------------------------
STOCK_CODE_EXPORT_QUERY_FILE = "models/db_init/stock_code_export.sql"
STOCK_CODE_EXPORT_OUTPUT = "data/stock_code_list.txt"

# ------------------------------------------------------------
# Main run
# ------------------------------------------------------------
print("\n🚀 Starting main run sequence...\n")
results = []
loader_results = []
start_total = time.time()

# ------------------------------------------------------------
# Run scripts
# ------------------------------------------------------------
for script in scripts:
    print(f"▶️ {script}")
    start = time.time()
    try:
        with suppress_output():
            runpy.run_path(script, run_name="__main__")
        elapsed = time.time() - start
        print(f"✅ Done: {script} ({elapsed:.1f}s)")
        results.append((script, "✅ Success", elapsed))
    except Exception as e:
        elapsed = time.time() - start
        print(f"❌ Failed: {script} ({elapsed:.1f}s) → {e}")
        results.append((script, f"❌ Failed ({e})", elapsed))
    print("-" * 60)

# ------------------------------------------------------------
# Run CSV loaders
# ------------------------------------------------------------
print("\n📦 Starting CSV loaders...\n")
for loader in CSV_LOADERS:
    csv_file = loader["csv_file"]
    table_name = loader["table_name"]
    print(f"▶️ CSV Loader: {table_name}")
    start = time.time()
    try:
        with suppress_output():
            rows_loaded = csv_loader(csv_file, table_name, db_path=DB_PATH)
        elapsed = time.time() - start
        loader_results.append((f"CSV Loader: {table_name}", "✅ Success", rows_loaded, elapsed))
        print(f"✅ Loaded {rows_loaded} rows into table '{table_name}' ({elapsed:.1f}s)\n")
    except Exception as e:
        elapsed = time.time() - start
        loader_results.append((f"CSV Loader: {table_name}", f"❌ Failed ({e})", 0, elapsed))
        print(f"❌ Failed CSV loader '{table_name}': {e}\n")
    print("-" * 60)

# ------------------------------------------------------------
# Export ISIN list BEFORE WRDS loaders
# ------------------------------------------------------------
print("\n📄 Exporting unique ISINs...\n")
with open(ISIN_EXPORT_QUERY_FILE, "r") as f:
    isin_sql = f.read()
start = time.time()
try:
    with suppress_output():
        num_isins = export_unique_isins(DB_PATH, ISIN_EXPORT_OUTPUT, isin_sql)
    elapsed = time.time() - start
    loader_results.append(("ISIN Export", "✅ Success", num_isins, elapsed))
    print(f"✅ Exported {num_isins} ISINs to {ISIN_EXPORT_OUTPUT} ({elapsed:.1f}s)")
except Exception as e:
    elapsed = time.time() - start
    loader_results.append(("ISIN Export", f"❌ Failed ({e})", 0, elapsed))
    print(f"❌ ISIN export failed: {e}")

# ------------------------------------------------------------
# Export Stock Code list AFTER ISIN export
# ------------------------------------------------------------
print("\n📄 Exporting unique Stock Codes...\n")
with open(STOCK_CODE_EXPORT_QUERY_FILE, "r") as f:
    stock_code_sql = f.read()
start = time.time()
try:
    from helpers.export_stock_codes import export_unique_stock_codes
    with suppress_output():
        num_stock_codes = export_unique_stock_codes(DB_PATH, STOCK_CODE_EXPORT_OUTPUT, stock_code_sql)
    elapsed = time.time() - start
    loader_results.append(("Stock Code Export", "✅ Success", num_stock_codes, elapsed))
    print(f"✅ Exported {num_stock_codes} stock codes to {STOCK_CODE_EXPORT_OUTPUT} ({elapsed:.1f}s)")
except Exception as e:
    elapsed = time.time() - start
    loader_results.append(("Stock Code Export", f"❌ Failed ({e})", 0, elapsed))
    print(f"❌ Stock Code export failed: {e}")

# ------------------------------------------------------------
# Run WRDS loaders
# ------------------------------------------------------------
print("\n📦 Starting WRDS loaders...\n")
for loader in WRDS_LOADERS:
    sql_file = loader["sql_file"]
    table_name = loader["table_name"]
    isin_file = loader.get("isin_list_file")
    print(f"▶️ WRDS Loader: {table_name}")
    start = time.time()
    try:
        with suppress_output():
            rows_loaded = wrds_loader(sql_file, table_name, db_path=DB_PATH, isin_list_file=isin_file)
        elapsed = time.time() - start
        if rows_loaded is None:
            status = "❌ Failed"
            rows_loaded = 0
        else:
            status = "✅ Success"
        loader_results.append((f"WRDS Loader: {table_name}", status, rows_loaded, elapsed))
        print(f"{status} ({rows_loaded} rows, {elapsed:.1f}s)\n")
    except Exception as e:
        elapsed = time.time() - start
        loader_results.append((f"WRDS Loader: {table_name}", f"❌ Failed ({e})", 0, elapsed))
        print(f"❌ Failed WRDS loader '{table_name}': {e}\n")
    print("-" * 60)

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
print("\n" + "=" * 60)
print("🏁 Run summary")
print("=" * 60)
max_len_scripts = max(len(str(s)) for s, _, _ in results)
for script, status, _ in results:
    print(f"{str(script).ljust(max_len_scripts)} {status}")
if loader_results:
    print("-" * 60)
    max_len_loaders = max(len(s) for s, _, _, _ in loader_results)
    for loader_name, status, rows, elapsed in loader_results:
        print(f"{loader_name.ljust(max_len_loaders)} {status} ({rows} rows, {elapsed:.1f}s)")
success_count = sum(1 for _, status, _ in results if "✅" in status)
fail_count = len(results) - success_count
loader_success = sum(1 for _, status, _, _ in loader_results if "✅" in status)
loader_fail = len(loader_results) - loader_success
print("-" * 60)
print(f"✅ Scripts successful: {success_count} ❌ Failed: {fail_count}")
print(f"✅ Loaders successful: {loader_success} ❌ Failed: {loader_fail}")
print(f"⏱️ Total runtime: {time.time() - start_total:.1f}s")
print("=" * 60 + "\n")
if fail_count + loader_fail > 0:
    print("⚠️ Some scripts/loaders failed. Please check the log above.\n")