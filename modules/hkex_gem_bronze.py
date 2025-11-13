from pathlib import Path
import pandas as pd


# ----------------------------
# Configuration
# ----------------------------
BASE_DIR = Path("./data/normalized")
OUTPUT_PATH = Path("./data/bronze/gem_bronze.csv")
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

COLUMNS = [
    "source_file", "listing_date", "stock_code", "company", "offer_price", "subscription_ratio",
    "funds_raised", "shrout_at_listing", "mcap_at_listing", "industry",
    "place_of_incorporation", "listing_method", "sponsors", "reporting_accountant",
]


# ----------------------------
# Helper functions
# ----------------------------
def clean_and_transform(file_path: Path) -> pd.DataFrame:
    """Clean and standardize a single GEM listing Excel file."""
    df_raw = pd.read_excel(file_path, engine="openpyxl", header=None)
    df_raw = df_raw.replace(r'[\r\n]+', ' ', regex=True)

    # Locate header row and columns
    header_row = df_raw.iloc[9].astype(str).str.strip()
    valid_cols = [i for i, val in enumerate(header_row) if val and val.lower() != "nan"]

    # Extract data rows
    df = df_raw.iloc[11:, valid_cols].reset_index(drop=True)
    df.columns = header_row[valid_cols]

    # Stop before the 'Total' row
    first_col = df.columns[0]
    total_mask = df[first_col].astype(str).str.strip().str.lower() == "total"
    if total_mask.any():
        df = df.iloc[:total_mask.idxmax(), :]

    # Drop rows with blank stock_code
    second_col = df.columns[1]
    df = df[df[second_col].notna() & (df[second_col].astype(str).str.strip() != "")]

    # Align and standardize
    df_out = df.iloc[:, :13].copy()
    df_out.insert(0, "source_file", file_path.name)
    df_out.columns = COLUMNS

    # Convert stock_code to int
    df_out["stock_code"] = pd.to_numeric(df_out["stock_code"], errors="coerce")
    df_out = df_out.dropna(subset=["stock_code"])
    df_out["stock_code"] = df_out["stock_code"].astype(int)

    return df_out


def process_gem_bronze():
    """Combine all GEM normalized Excel files into one bronze CSV."""
    combined = pd.DataFrame(columns=COLUMNS)
    any_skipped = False

    files = sorted(BASE_DIR.glob("GEM_*.xlsx"))
    print(f"🔍 Found {len(files)} GEM files to process.")

    for file in files:
        try:
            df_cleaned = clean_and_transform(file)
            combined = pd.concat([combined, df_cleaned], ignore_index=True)
        except Exception:
            any_skipped = True

    combined.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Combined {len(combined)} rows → {OUTPUT_PATH}")

    if any_skipped:
        print("⚠️  One or more GEM files were skipped due to errors.")


# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    process_gem_bronze()
