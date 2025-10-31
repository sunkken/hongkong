from pathlib import Path
import pandas as pd

COLUMNS = [
    "source_file", "listing_date", "stock_code", "company", "offer_price", "subscription_ratio",
    "funds_raised", "shrout_at_listing", "mcap_at_listing", "industry",
    "place_of_incorporation", "listing_method", "sponsors", "reporting_accountant",
]

combined = pd.DataFrame(columns=COLUMNS)

# Pick up every Excel file whose name starts with GEM_
base_dir = Path("./data/normalized")
files = sorted(base_dir.glob("GEM_*.xlsx"))

print(f"Found {len(files)} files to process.")

for f in files:
    df_raw = pd.read_excel(f, header=None)

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

print("\nCombined preview:")
print(combined.head())

combined.to_csv("data/bronze/gem_bronze.csv", index=False)

print(f"Saved {len(combined)} rows to ./data/bronze/gem_bronze.csv")