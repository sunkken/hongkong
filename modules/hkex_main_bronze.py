from pathlib import Path
import pandas as pd
import numpy as np


# ----------------------------
# Configuration
# ----------------------------
BASE_DIR = Path("./data/normalized")
OUTPUT_PATH = Path("./data/bronze/main_bronze.csv")
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

COLUMNS = [
    "source_file", "stock_code", "company", "prospectus_date", "listing_date",
    "sponsors", "reporting_accountant", "valuers", "funds_raised",
    "subscription_price", "offer_location"
]

# Silence FutureWarning from pandas replace downcasting
pd.set_option('future.no_silent_downcasting', True)


# ----------------------------
# Helper functions
# ----------------------------
def clean_and_transform(file_path: Path) -> pd.DataFrame:
    """Clean and standardize a single Main Board listing file."""
    df = pd.read_excel(file_path, engine="openpyxl", header=None, skiprows=1)
    df = df.iloc[:, :11]
    df = df.iloc[1:]  # Trim away the first row

    # Flatten multi-line cells
    df = df.replace(r'[\r\n]+', ' ', regex=True)

    # Normalize excessive spacing in company names
    if df.shape[1] > 2:
        df.iloc[:, 2] = df.iloc[:, 2].astype(str).str.replace(r' {2,}', ' ', regex=True)

    # Drop rows where stock_code is missing
    df = df[df.iloc[:, 1].notna()]

    # Stop at first fully empty row
    empty_row_index = df[df.isnull().all(axis=1)].index.min()
    if pd.notna(empty_row_index):
        df = df.iloc[:empty_row_index]

    # Clean quotes, infer types
    df = df.replace(r'^\"+$', np.nan, regex=True).infer_objects(copy=False)

    # Fill stock_code for continuation rows (offer types b/c)
    if df.shape[1] >= 11:
        for i in range(len(df)):
            offer_type = str(df.iloc[i, -1]).strip()
            prev_offer = str(df.iloc[i - 1, -1]).strip() if i > 0 else ""
            if offer_type in ("(b)", "(c)") and i > 0:
                if prev_offer == "(a)":
                    df.iat[i, 1] = df.iat[i - 1, 1]
                elif offer_type == "(c)" and prev_offer == "(b)" and i > 1:
                    df.iat[i, 1] = df.iat[i - 2, 1]

    # Drop first column (index) and insert source_file
    if df.shape[1] > 1:
        df = df.iloc[:, 1:]
    df.insert(0, "source_file", file_path.name)

    # Pad columns if missing
    while df.shape[1] < len(COLUMNS):
        df[df.shape[1]] = np.nan
    df.columns = COLUMNS

    # Convert stock_code to int safely
    df["stock_code"] = pd.to_numeric(df["stock_code"], errors="coerce")
    df = df.dropna(subset=["stock_code"]).copy()
    df.loc[:, "stock_code"] = df["stock_code"].astype(int)

    return df


def process_main_bronze():
    """Combine all Main Board normalized Excel files into one bronze CSV."""
    combined = None
    any_skipped = False

    files = sorted(BASE_DIR.glob("Main_*.xlsx"))
    print(f"🔍 Found {len(files)} files to process.")

    for file in files:
        try:
            df_cleaned = clean_and_transform(file)

            # Drop all-NaN columns defensively
            df_cleaned = df_cleaned.dropna(axis=1, how="all")

            # ✅ Avoid concat with empty DataFrame (prevents FutureWarning)
            if combined is None:
                combined = df_cleaned
            else:
                combined = pd.concat([combined, df_cleaned], ignore_index=True)
        except Exception:
            any_skipped = True

    # Handle case where no files were successfully processed
    if combined is None or combined.empty:
        print("⚠️ No valid data processed; no file saved.")
        return

    combined.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Combined {len(combined)} rows → {OUTPUT_PATH}")

    if any_skipped:
        print("⚠️  One or more files were skipped due to errors.")


# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    process_main_bronze()
