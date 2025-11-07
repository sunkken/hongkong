import runpy
import time

scripts = [
    # Scrapers
    "scrapers/download_hkex_isino.py",
    "scrapers/download_hkex_listings.py",

    # XLSX converter
    "modules/hkex_xlsx_converter.py",

    # Modules ISINO
    "modules/hkex_isino_main.py",
    "modules/hkex_isino_stock_types.py",
    "modules/hkex_isino_national_agencies.py",

    # Modules Bronze
    "modules/hkex_main_bronze.py",
    "modules/hkex_gem_bronze.py",

    # Modules Silver
    "modules/hkex_main_silver.py",
    "modules/hkex_gem_silver.py",
]

print("\n🚀 Starting main run sequence...\n")

results = []
start_total = time.time()

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

    print("-" * 60)  # separator between scripts

# ----------------------------
# Summary
# ----------------------------
print("\n" + "=" * 60)
print("🏁 Run summary")
print("=" * 60)

max_len = max(len(s) for s, _, _ in results)
for script, status, _ in results:
    print(f"{script.ljust(max_len)}  {status}")

success_count = sum(1 for _, status, _ in results if "✅" in status)
fail_count = len(results) - success_count
print("-" * 60)
print(f"✅ Successful: {success_count}   ❌ Failed: {fail_count}")
print(f"⏱️  Total runtime: {time.time() - start_total:.1f}s")
print("=" * 60 + "\n")

if fail_count > 0:
    print("⚠️  Some scripts failed. Please check the log above.\n")
    exit(1)
