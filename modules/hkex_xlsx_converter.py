
from pathlib import Path
import pandas as pd

raw_dir = Path("./data/raw")
out_dir = Path("./data/normalized")
out_dir.mkdir(parents=True, exist_ok=True)

files = sorted(list(raw_dir.glob("*.xls")) + list(raw_dir.glob("*.xlsx")))
print(f"Found {len(files)} files to normalize.")

for f in files:
    if f.name.startswith("~$"):  # skip temp/lock files
        continue

    target = out_dir / (f.stem + ".xlsx")
    print(f"Normalizing {f.name} → {target.name}")

    try:
        # Try OOXML first (works for true .xlsx and many mislabeled .xls)
        df = pd.read_excel(f, header=None, engine="openpyxl")
    except Exception as e_openpyxl:
        try:
            # Fallback for genuine legacy .xls binaries
            df = pd.read_excel(f, header=None, engine="xlrd")
        except Exception as e_xlrd:
            print(f"[WARN] Could not read {f.name}: openpyxl={e_openpyxl}; xlrd={e_xlrd}")
            continue

    # Unified write (values only)
    df.to_excel(target, index=False, header=False)
