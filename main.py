
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
from pathlib import Path
from dotenv import load_dotenv
from loaders.db_loader_csv import csv_loader
from loaders.db_loader_wrds import wrds_loader
from loaders.db_run_sql import run_sql_file
from loaders.db_to_file_loader import export_sql_file

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
OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "xlsx")  # csv, xlsx, or txt

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
    "modules/auditor_opinion_flags.py",
]

# ------------------------------------------------------------
# Files and tables for CSV loader (pushes clean csv-files into SQLite Database)
# ------------------------------------------------------------
CSV_LOADERS = [
    {"csv_file": "data/silver/gem_silver.csv", "table_name": "hkex_gem"},
    {"csv_file": "data/silver/main_silver.csv", "table_name": "hkex_main"},
    {"csv_file": "data/bronze/isino_bronze.csv", "table_name": "hkex_isin"},
    {"csv_file": "data/bronze/isino_stock_types.csv", "table_name": "desc_hkex_stock_types"},
    {"csv_file": "data/bronze/isino_national_agencies.csv", "table_name": "desc_hkex_national_agencies"},
    {"csv_file": "data/raw/auditor_reports.csv", "table_name": "hkex_auditor_reports"},
    {"csv_file": "data/processed/auditor_opinion_flags.csv", "table_name": "auditor_opinion_flags"},
]

# ------------------------------------------------------------
# Code export configuration (export unique ISINs and Stock Codes from database to text files)
# ------------------------------------------------------------
ISIN_EXPORT_QUERY_FILE = "models/db_export/select_union_hkex_isin.sql"
ISIN_EXPORT_OUTPUT = "data/isin_list.txt"
STOCK_CODE_EXPORT_QUERY_FILE = "models/db_export/select_union_hkex_stock_code.sql"
STOCK_CODE_EXPORT_OUTPUT = "data/stock_code_list.txt"

# ------------------------------------------------------------
# WRDS loaders configuration (downloads data from WRDS and pushes into SQLite Database)
# ------------------------------------------------------------
WRDS_LOADERS = [
    {"sql_file": "models/db_init/dl_funda_a_170.sql", "table_name": "funda_a_170"},
    {"sql_file": "models/db_init/dl_funda_q_170.sql", "table_name": "funda_q_170"},
    {"sql_file": "models/db_init/dl_funda_a_isin.sql", "table_name": "funda_a_isin", "isin_list_file": "data/isin_list.txt"},
    {"sql_file": "models/db_init/dl_funda_q_isin.sql", "table_name": "funda_q_isin", "isin_list_file": "data/isin_list.txt"},
]

# ------------------------------------------------------------
# Database queries (table and view creation)
# ------------------------------------------------------------
# Order matters: create `hkex_all_stock_code_isin` first, then `hkex_dataset` and `hkex_document_dataset` which depend on it
DB_QUERIES = [
    "models/db_init/cv_hkex_all_stock_code_isin.sql",
    "models/db_init/cv_hkex_dataset.sql",
    "models/db_init/cv_hkex_document_dataset.sql",
]

# ------------------------------------------------------------
# Export Queries (export views/tables into folder data/processed. OUTPUT_FORMAT from environment variables)
# ------------------------------------------------------------
EXPORT_SQLS = [
    "models/db_export/select_hkex_dataset.sql",
    "models/db_export/select_hkex_document_dataset.sql",
    "models/db_export/select_hkex_dataset_hkex_document_exists.sql",
    "models/db_export/select_hkex_isin.sql",
    "models/db_export/select_funda_q_170.sql",
]

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
start = time.time()
try:
    with suppress_output():
        export_sql_file(ISIN_EXPORT_QUERY_FILE, db_path=DB_PATH, output_format='txt', output_file=ISIN_EXPORT_OUTPUT)
    elapsed = time.time() - start
    # Count lines in the file
    with open(ISIN_EXPORT_OUTPUT, 'r') as f:
        num_isins = len(f.readlines())
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
start = time.time()
try:
    with suppress_output():
        export_sql_file(STOCK_CODE_EXPORT_QUERY_FILE, db_path=DB_PATH, output_format='txt', output_file=STOCK_CODE_EXPORT_OUTPUT)
    elapsed = time.time() - start
    # Count lines in the file
    with open(STOCK_CODE_EXPORT_OUTPUT, 'r') as f:
        num_stock_codes = len(f.readlines())
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
# Run database queries
# ------------------------------------------------------------
if DB_QUERIES:
    print("\n📚 Running database queries...\n")
    for sql_file in DB_QUERIES:
        print(f"▶️ {sql_file}")
        start = time.time()
        with suppress_output():
            success = run_sql_file(sql_file, db_path=DB_PATH)
        elapsed = time.time() - start
        status = "✅ Success" if success else "❌ Failed"
        loader_results.append((f"Query: {Path(sql_file).name}", status, 0, elapsed))
        print(f"{status} ({elapsed:.1f}s)\n")
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
query_success = sum(1 for loader_name, status, _, _ in loader_results if "Query:" in str(loader_name) and "✅" in str(status))
query_fail = sum(1 for loader_name, status, _, _ in loader_results if "Query:" in str(loader_name) and "❌" in str(status))
print("-" * 60)
print(f"✅ Scripts successful: {success_count} ❌ Failed: {fail_count}")
print(f"✅ Loaders successful: {loader_success} ❌ Failed: {loader_fail}")
if query_success + query_fail > 0:
    print(f"✅ Queries successful: {query_success} ❌ Failed: {query_fail}")
print(f"⏱️ Total runtime: {time.time() - start_total:.1f}s")
print("=" * 60 + "\n")
if fail_count + loader_fail > 0:
    print("⚠️ Some scripts/loaders failed. Please check the log above.\n")

# ------------------------------------------------------------
# Export views to CSV/XLSX
# ------------------------------------------------------------

print("\n📤 Exporting views to CSV/XLSX...\n")
for sql_file in EXPORT_SQLS:
    print(f"▶ Export: {sql_file}")
    start = time.time()
    try:
        with suppress_output():
            export_sql_file(sql_file, db_path=DB_PATH, output_dir=Path("data/processed"), output_format=OUTPUT_FORMAT)
        elapsed = time.time() - start
        print(f"✅ Export completed ({elapsed:.1f}s)\n")
    except Exception as e:
        elapsed = time.time() - start
        print(f"❌ Export failed for {sql_file}: {e} ({elapsed:.1f}s)\n")