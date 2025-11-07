from pathlib import Path
import pandas as pd
import numpy as np

# ----------------------------
# Configuration
# ----------------------------
pd.set_option("future.no_silent_downcasting", True)

BASE_DIR = Path("./data/normalized")
FILE_PATH = BASE_DIR / "isino.xlsx"
OUTPUT_PATH = Path("./data/bronze/isino_bronze.csv")

COLUMNS = [
    "company", "isin_code", "stock_code", "stock_type",
    "place_of_incorporation", "national_agency",
]


# ----------------------------
# Helper functions
# ----------------------------
def clean_and_transform(file_path):
    """Clean and transform the Excel file into a normalized DataFrame."""
    df_raw = pd.read_excel(file_path, engine="openpyxl", header=None)

    # Detect the first row with multiple non-empty columns
    start_row = None
    for i, row in df_raw.iterrows():
        if row.notna().sum() >= 3:
            start_row = i
            break

    if start_row is None:
        raise ValueError("No valid data row found in the file.")

    # Skip the header row and read from the next row
    df_data = df_raw.iloc[start_row + 1:].copy()

    # Stop at the first blank value in the first column
    stop_index = df_data[df_data.iloc[:, 0].isna()].index.min()
    if pd.notna(stop_index):
        df_data = df_data.loc[:stop_index - 1]

    # Trim to first 6 columns
    df_data = df_data.iloc[:, :6]

    # Replace newlines and excessive quotes
    df_data = df_data.replace(r"[\r\n]+", " ", regex=True)
    df_data = df_data.replace(r'^\"+$', np.nan, regex=True).infer_objects(copy=False)

    # Assign column names
    df_data.columns = COLUMNS

    # Convert stock_code to numeric and clean rows
    df_data["stock_code"] = pd.to_numeric(df_data["stock_code"], errors="coerce")
    df_data = df_data.dropna(subset=["stock_code"])
    df_data["stock_code"] = df_data["stock_code"].astype(int)

    return df_data


# ----------------------------
# Main logic
# ----------------------------
def convert_isino():
    """Load, clean, and export ISINO Excel data."""
    print("📘 Processing ISINO Excel file...")

    try:
        df_cleaned = clean_and_transform(FILE_PATH)
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        df_cleaned.to_csv(OUTPUT_PATH, index=False)
        print(f"✅ Processed {len(df_cleaned)} rows → {OUTPUT_PATH}")
    except Exception as e:
        print(f"❌ Failed to process ISINO file: {e}")
        raise  # allow main.py to catch and log the failure


# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    convert_isino()
