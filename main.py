import runpy
import time
from loaders.db_loader_csv import csv_loader
from loaders.db_loader_wrds import wrds_loader

# ----------------------------
# Configuration
# ----------------------------
DB_PATH = "data/hongkong.db"  # SQLite database location

# Files and tables for CSV loader
CSV_LOADERS = [
    {"csv_file": "data/silver/gem_silver.csv", "table_name": "hkex_gem"},
    {"csv_file": "data/silver/main_silver.csv", "table_name": "hkex_main"},
    # Add more CSV loaders here as needed
]

# Files and tables for WRDS loader
WRDS_LOADERS = [
    {"sql_file": "models/download_funda_a.sql", "table_name": "funda_a"},
    {"sql_file": "models/download_funda_q.sql", "table_name": "funda_q"},
    # Add more WRDS loaders here as needed
]

# ----------------------------
# Scripts
# ----------------------------
scripts = [
    # Downloaders (scrapers)
    "loaders/download_hkex_isino.py",
    "loaders/download_hkex_listings.py",

    # XLSX converter
    "modules/hkex_xlsx_converter.py",

    # Modules ISINO
    "modules/hkex_isino_bronze.py",
    "modules/hkex_isino_stock_types.py",
    "modules/hkex_isino_national_agencies.py",

    # Modules Bronze
    "modules/hkex_main_bronze.py",
    "modules/hkex_gem_bronze.py",

    # Modules Silver
    "modules/hkex_main_silver.py",
    "modules/hkex_gem_silver.py",
]

# ----------------------------
# Main run
# ----------------------------
print("\n🚀 Starting main run sequence...\n")
results = []
loader_results = []
start_total = time.time()

# Run scripts
for script in scripts:
    print(f"▶️  {script}")
    start = time.time()
    try:
        runpy.run_path(script, run_name="__main__")
        elapsed = time.time() - start
        print(f"✅ Done: {script} ({elapsed:.1f}s)")
        results.append((script, "✅ Success", elapsed))
    except Exception as e:
        elapsed = time.time() - start
        print(f"❌ Failed: {script} ({elapsed:.1f}s) → {e}")
        results.append((script, f"❌ Failed ({e})", elapsed))
    print("-" * 60)

# ----------------------------
# Run loaders
# ----------------------------
print("\n📦 Starting database loaders...\n")

# Run CSV loaders
for loader in CSV_LOADERS:
    csv_file = loader["csv_file"]
    table_name = loader["table_name"]
    print(f"▶️  CSV Loader: {table_name}")
    try:
        rows_loaded = csv_loader(csv_file, table_name, db_path=DB_PATH)
        loader_results.append((f"CSV Loader: {table_name}", "✅ Success", rows_loaded))
        print(f"✅ Loaded {rows_loaded} rows into table '{table_name}'\n")
    except Exception as e:
        loader_results.append((f"CSV Loader: {table_name}", f"❌ Failed ({e})", 0))
        print(f"❌ Failed CSV loader '{table_name}': {e}\n")
    print("-" * 60)

# Run WRDS loaders
for loader in WRDS_LOADERS:
    sql_file = loader["sql_file"]
    table_name = loader["table_name"]
    print(f"▶️  WRDS Loader: {table_name}")
    try:
        rows_loaded = wrds_loader(sql_file, table_name, db_path=DB_PATH)
        loader_results.append((f"WRDS Loader: {table_name}", "✅ Success", rows_loaded))
        print(f"✅ Loaded {rows_loaded} rows into table '{table_name}'\n")
    except Exception as e:
        loader_results.append((f"WRDS Loader: {table_name}", f"❌ Failed ({e})", 0))
        print(f"❌ Failed WRDS loader '{table_name}': {e}\n")
    print("-" * 60)

# ----------------------------
# Summary
# ----------------------------
print("\n" + "=" * 60)
print("🏁 Run summary")
print("=" * 60)

# Scripts
max_len_scripts = max(len(str(s)) for s, _, _ in results)
for script, status, _ in results:
    print(f"{str(script).ljust(max_len_scripts)}  {status}")

# Loaders
if loader_results:
    print("-" * 60)
    max_len_loaders = max(len(s) for s, _, _ in loader_results)
    for loader_name, status, rows in loader_results:
        print(f"{loader_name.ljust(max_len_loaders)}  {status} ({rows} rows)")

success_count = sum(1 for _, status, _ in results if "✅" in status)
fail_count = len(results) - success_count
loader_success = sum(1 for _, status, _ in loader_results if "✅" in status)
loader_fail = len(loader_results) - loader_success

print("-" * 60)
print(f"✅ Scripts successful: {success_count}   ❌ Failed: {fail_count}")
print(f"✅ Loaders successful: {loader_success}   ❌ Failed: {loader_fail}")
print(f"⏱️  Total runtime: {time.time() - start_total:.1f}s")
print("=" * 60 + "\n")

if fail_count + loader_fail > 0:
    print("⚠️  Some scripts/loaders failed. Please check the log above.\n")
    exit(1)