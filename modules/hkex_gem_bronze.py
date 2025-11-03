
from pathlib import Path
import pandas as pd

# Define output columns
COLUMNS = [
    "source_file", "listing_date", "stock_code", "company", "offer_price", "subscription_ratio",
    "funds_raised", "shrout_at_listing", "mcap_at_listing", "industry",
    "place_of_incorporation", "listing_method", "sponsors", "reporting_accountant",
]

# Initialize empty DataFrame and skip flag
combined = pd.DataFrame(columns=COLUMNS)
any_skipped = False

# Define base directory
base_dir = Path("./data/normalized")
files = sorted(base_dir.glob("GEM_*.xlsx"))

print(f"🔍 Found {len(files)} files to process.")

# Process each file
for f in files:
    try:
        df_raw = pd.read_excel(f, header=None)

        # Flatten multi-line cells by replacing line breaks with spaces
        df_raw = df_raw.replace(r'[\r\n]+', ' ', regex=True)

        header_row = df_raw.iloc[9].astype(str).str.strip()
        valid_cols = [i for i, val in enumerate(header_row) if val and val.lower() != "nan"]

        df = df_raw.iloc[11:, valid_cols].reset_index(drop=True)
        df.columns = header_row[valid_cols]

        # Stop at 'Total'
        first_col = df.columns[0]
        total_mask = df[first_col].astype(str).str.strip().str.lower() == "total"
        if total_mask.any():
            df = df.iloc[:total_mask.idxmax(), :]

        # Drop rows where second column is NaN or empty
        second_col = df.columns[1]
        df = df[df[second_col].notna() & (df[second_col].astype(str).str.strip() != "")]

        # Build aligned output and append
        df_out = df.iloc[:, :13].copy()
        df_out.insert(0, "source_file", f.name)
        df_out.columns = COLUMNS

        combined = pd.concat([combined, df_out], ignore_index=True)
    except Exception:
        any_skipped = True

# Output results
print("\n📊 Combined Data Preview:")
print(combined.head())
print(f"📈 Total rows combined: {len(combined)}")

if any_skipped:
    print("\n⚠️ One or more files were skipped due to errors.")
else:
    print("\n✅ All files processed successfully.")

# Ensure output directory exists
Path("data/bronze").mkdir(parents=True, exist_ok=True)

# Save to CSV
combined.to_csv("data/bronze/gem_bronze.csv", index=False)
print("\n📁 Saved combined data to data/bronze/gem_bronze.csv")
