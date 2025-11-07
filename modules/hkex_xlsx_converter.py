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
def read_excel_file(file_path):
    """Read Excel file using openpyxl or xlrd as fallback."""
    try:
        # Try OOXML first (works for true .xlsx and many mislabeled .xls)
        return pd.read_excel(file_path, header=None, engine="openpyxl")
    except Exception as e_openpyxl:
        try:
            # Fallback for genuine legacy .xls binaries
            return pd.read_excel(file_path, header=None, engine="xlrd")
        except Exception as e_xlrd:
            print(f"[WARN] Could not read {file_path.name}: openpyxl={e_openpyxl}; xlrd={e_xlrd}")
            return None


def normalize_excel_files():
    """Normalize all Excel files from raw_dir into .xlsx files."""
    files = sorted(list(RAW_DIR.glob("*.xls")) + list(RAW_DIR.glob("*.xlsx")))
    print(f"Found {len(files)} files to normalize.")

    for f in files:
        if f.name.startswith("~$"):  # skip temp/lock files
            continue

        target = OUT_DIR / (f.stem + ".xlsx")
        print(f"Normalizing {f.name} → {target.name}")

        df = read_excel_file(f)
        if df is not None:
            # Unified write (values only)
            df.to_excel(target, index=False, header=False)


# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    normalize_excel_files()
