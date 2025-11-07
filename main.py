import runpy
import time

scripts = [
    ## Scrapers
    "scrapers/download_hkex_isino.py",
    "scrapers/download_hkex_listings.py",

    ## xlsx converter
    "modules/hkex_xlsx_converter.py",

    ## Modules isino
    "modules/hkex_isino_main.py",
    "modules/hkex_isino_stock_types.py",
    "modules/hkex_isino_national_agencies.py",

    ## Modules bronze
    "modules/hkex_main_bronze.py",
    "modules/hkex_gem_bronze.py",

    ## Modules silver
    "modules/hkex_main_silver.py",
    "modules/hkex_gem_silver.py",
]

print("\n🚀 Starting main run sequence...\n")

for script in scripts:
    print("=" * 60)
    print(f"▶️  Running: {script}")
    print("=" * 60)
    start = time.time()

    try:
        runpy.run_path(script, run_name="__main__")
        print(f"\n✅ Finished: {script} in {time.time() - start:.2f}s\n")
    except Exception as e:
        print(f"\n❌ Error running {script}: {e}\n")

print("🏁 All scripts finished.\n")
