import time
import importlib

# ----------------------------------------------------
# Define pipeline stages
# ----------------------------------------------------

SCRAPER_TASKS = [
    "scrapers.download_hkex_isino",
    "scrapers.download_hkex_listings",
]

NORMALIZATION_TASKS = [
    "modules.hkex_xlsx_converter",
]

BRONZE_TASKS = [
    "modules.hkex_isino_bronze",
    "modules.hkex_isino_stock_types",
    "modules.hkex_isino_national_agencies",
    "modules.hkex_main_bronze",
    "modules.hkex_gem_bronze",
]

SILVER_TASKS = [
    "modules.hkex_main_silver",
    "modules.hkex_gem_silver",
]

# ----------------------------------------------------
# Utility to run a task module
# ----------------------------------------------------
def run_task(module_path: str):
    print(f"▶️  Running: {module_path}")

    start = time.time()
    try:
        module = importlib.import_module(module_path)
        if hasattr(module, "main"):
            module.main()  # recommended entry point
        elif hasattr(module, "__main__"):
            module.__main__()  # fallback
        else:
            # Last fallback: if script only runs on import
            pass

        elapsed = time.time() - start
        print(f"✅ Success: {module_path} ({elapsed:.1f}s)")
        return True

    except Exception as e:
        elapsed = time.time() - start
        print(f"❌ ERROR in {module_path} ({elapsed:.1f}s)\n   → {e}\n")
        return False


# ----------------------------------------------------
# Orchestrate all pipeline stages
# ----------------------------------------------------
def run_pipeline():
    start = time.time()
    results = []

    print("\n🚀 Starting full HKEX pipeline...\n")

    PIPELINE = [
        ("Scrapers", SCRAPER_TASKS),
        ("Normalization", NORMALIZATION_TASKS),
        ("Bronze", BRONZE_TASKS),
        ("Silver", SILVER_TASKS),
    ]

    for label, task_list in PIPELINE:
        print(f"\n=== 📦 {label} Stage ===")

        for task in task_list:
            ok = run_task(task)
            results.append((task, ok))

    # Summary
    print("\n" + "=" * 60)
    print("🏁 Pipeline Summary")
    print("=" * 60)

    max_len = max(len(t) for t, _ in results)

    success_count = 0
    for task, ok in results:
        status = "✅ OK" if ok else "❌ FAIL"
        if ok:
            success_count += 1
        print(f"{task.ljust(max_len)}  {status}")

    fail_count = len(results) - success_count
    print("-" * 60)
    print(f"✔ Successful: {success_count}")
    print(f"✖ Failed:     {fail_count}")
    print(f"⏱ Total time: {time.time() - start:.1f}s")
    print("=" * 60)

    if fail_count > 0:
        print("\n⚠️  Some tasks failed — please review log.\n")
        exit(1)


# ----------------------------------------------------
# Entry point
# ----------------------------------------------------
if __name__ == "__main__":
    run_pipeline()
