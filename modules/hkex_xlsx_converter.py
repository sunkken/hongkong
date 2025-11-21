from pathlib import Path
import pandas as pd

# ----------------------------
# Configuration
# ----------------------------
RAW_DIR = Path("./data/raw")
OUT_DIR = Path("./data/normalized")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Helper functions
# ----------------------------
def read_excel_file(file_path: Path):
    """Read Excel file using openpyxl first, fallback to xlrd for old .xls."""
    try:
        return pd.read_excel(file_path, header=None, engine="openpyxl")
    except Exception:
        try:
            return pd.read_excel(file_path, header=None, engine="xlrd")
        except Exception:
            return None

# ----------------------------
# Main process
# ----------------------------
def normalize_excel_files():
    """Convert all Excel files in raw_dir into clean .xlsx files with summary output."""
    files = sorted(list(RAW_DIR.glob("*.xls")) + list(RAW_DIR.glob("*.xlsx")))
    if not files:
        print("⚠️  No Excel files found in ./data/raw — skipping normalization.")
        return

    summary = {"converted": 0, "cached": 0, "failed": 0, "skipped": 0}

    for f in files:
        if f.name.startswith("~$"):
            summary["skipped"] += 1
            continue

        target = OUT_DIR / (f.stem + ".xlsx")
        if target.exists():
            summary["cached"] += 1
            continue

        df = read_excel_file(f)
        if df is None:
            summary["failed"] += 1
            continue

        try:
            df.to_excel(target, index=False, header=False)
            summary["converted"] += 1
        except Exception:
            summary["failed"] += 1

    # ----------------------------
    # Final Summary
    # ----------------------------
    print(
        f"\n📊 Normalization Summary: ✅ Converted: {summary['converted']}, "
        f"🗂️ Cached: {summary['cached']}, ⚠️ Skipped: {summary['skipped']}, "
        f"❌ Failed: {summary['failed']}"
    )

    if summary["failed"] > 0:
        raise RuntimeError(f"{summary['failed']} Excel files failed to normalize.")

    print("✅ Normalization complete.\n")

# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    try:
        normalize_excel_files()
    except Exception as e:
        print(f"\n❌ Normalization process failed: {e}")
        raise
