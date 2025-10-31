from pathlib import Path
import pandas as pd
import shutil

raw_dir = Path("data/raw")
out_dir = Path("data/normalized")
out_dir.mkdir(parents=True, exist_ok=True)

# Process all GEM files (xls and xlsx)
for f in sorted(list(raw_dir.glob("GEM_*.xls")) + list(raw_dir.glob("GEM_*.xlsx"))):
    target = out_dir / (f.stem + ".xlsx")

    if f.suffix.lower() == ".xlsx":
        # Copy existing xlsx directly
        print(f"Copying {f.name} → {target.name}")
        shutil.copy2(f, target)
        continue

    # Convert .xls to .xlsx
    try:
        print(f"Converting {f.name} → {target.name}")
        df = pd.read_excel(f, header=None)
        df.to_excel(target, index=False, header=False)
    except Exception as e:
        print(f"[WARN] Could not read {f.name}: {e}")
