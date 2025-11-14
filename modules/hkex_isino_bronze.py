from pathlib import Path
import pandas as pd
import numpy as np

# ----------------------------
# Configuration
# ----------------------------
pd.set_option("future.no_silent_downcasting", True)

BASE_DIR = Path("./data/normalized")
FILE_PATH = BASE_DIR / "isino.xlsx"
FILE_PATH_SEHK = BASE_DIR / "isinsehk.xlsx"
SEC_FILE_PATH = BASE_DIR / "secstkorder.xlsx"
OUTPUT_PATH = Path("./data/bronze/isino_bronze.csv")

COLUMNS = [
    "company", "isin_code", "stock_code", "stock_type",
    "place_of_incorporation", "national_agency",
]

SEC_COLUMNS = ["stock_code", "hkex_co_name"]


# ----------------------------
# Shared cleaning function
# ----------------------------
def clean_and_transform_df(df_raw):
    """
    Clean and transform a DataFrame containing ONE table.
    df_raw should already be trimmed down so the first table
    begins at its first header row.
    """

    # Detect first header row (>=3 non-empty cells)
    start_row = None
    for i, row in df_raw.iterrows():
        if row.notna().sum() >= 3:
            start_row = i
            break

    if start_row is None:
        raise ValueError("No valid data table found.")

    # Skip the header row
    df_data = df_raw.iloc[start_row + 1:].copy()

    # Stop at first blank in col 0
    stop_index = df_data[df_data.iloc[:, 0].isna()].index.min()
    if pd.notna(stop_index):
        df_data = df_data.loc[:stop_index - 1]

    # Trim to available columns (max 6)
    df_data = df_data.iloc[:, :6]

    # Replace newlines/quotes and normalize
    df_data = df_data.replace(r"[\r\n]+", " ", regex=True)
    df_data = df_data.replace(r'^\"+$', np.nan, regex=True).infer_objects(copy=False)

    # Add missing columns if table has <6 columns
    missing_cols = len(COLUMNS) - df_data.shape[1]
    if missing_cols > 0:
        for i in range(missing_cols):
            df_data[f"extra_col_{i}"] = np.nan

    # Assign final column names
    df_data.columns = COLUMNS

    df_data["stock_code"] = pd.to_numeric(df_data["stock_code"], errors="coerce")
    df_data = df_data.dropna(subset=["stock_code"])
    df_data["stock_code"] = df_data["stock_code"].astype(int)

    return df_data


# ----------------------------
# ISINO file (normal)
# ----------------------------
def clean_and_transform(file_path):
    df_raw = pd.read_excel(file_path, engine="openpyxl", header=None)
    return clean_and_transform_df(df_raw)


# ----------------------------
# SEHK file (skip first table)
# ----------------------------
def preprocess_sehk_and_clean(file_path):
    """
    1. Load raw Excel
    2. Find Table 1 header
    3. Skip everything until blank row after table 1
    4. Pass remaining DataFrame into shared clean function
    """
    df_raw = pd.read_excel(file_path, engine="openpyxl", header=None)

    # Find header of table 1
    first_header = None
    for i, row in df_raw.iterrows():
        if row.notna().sum() >= 3:
            first_header = i
            break

    if first_header is None:
        raise ValueError("ISINSEHK: Could not find first table header.")

    # Find first blank after table 1
    df_after_first = df_raw.iloc[first_header + 1:].copy()
    blank_after_first = df_after_first[df_after_first.iloc[:, 0].isna()].index.min()

    if pd.isna(blank_after_first):
        raise ValueError("ISINSEHK: Could not find end of first table.")

    # Slice everything AFTER the blank row
    df_second_table_candidate = df_after_first.loc[blank_after_first + 1:].copy()

    # Process as normal table
    return clean_and_transform_df(df_second_table_candidate)


# ----------------------------
# SEC stock names
# ----------------------------
def clean_and_transform_sec(file_path):
    """
    Clean the SEC stock name file (2-column table)
    """
    df_raw = pd.read_excel(file_path, engine="openpyxl", header=None)

    # Detect first row with >=2 non-empty cells
    start_row = None
    for i, row in df_raw.iterrows():
        if row.notna().sum() >= 2:
            start_row = i
            break

    if start_row is None:
        raise ValueError("SEC table not found.")

    # Skip header row
    df_data = df_raw.iloc[start_row + 1:].copy()

    # Stop at first blank row in column 0
    stop_index = df_data[df_data.iloc[:, 0].isna()].index.min()
    if pd.notna(stop_index):
        df_data = df_data.loc[:stop_index - 1]

    # Take only first 2 columns
    df_data = df_data.iloc[:, :2]

    # Replace newlines/quotes and normalize
    df_data = df_data.replace(r"[\r\n]+", " ", regex=True)
    df_data = df_data.replace(r'^\"+$', np.nan, regex=True).infer_objects(copy=False)

    df_data.columns = SEC_COLUMNS

    # Ensure stock_code is numeric
    df_data["stock_code"] = pd.to_numeric(df_data["stock_code"], errors="coerce")
    df_data = df_data.dropna(subset=["stock_code"])
    df_data["stock_code"] = df_data["stock_code"].astype(int)

    return df_data


# ----------------------------
# Main logic
# ----------------------------
def convert_isino():
    """Load, clean, append ISINO + SEHK, enrich with SEC names, export."""
    print("📘 Processing ISINO, ISINSEHK, and SEC stock names...")

    try:
        # Step 1: main + SEHK
        df_main = clean_and_transform(FILE_PATH)
        df_sehk = preprocess_sehk_and_clean(FILE_PATH_SEHK)
        df_combined = pd.concat([df_main, df_sehk], ignore_index=True)

        # Step 2: SEC stock names
        df_sec = clean_and_transform_sec(SEC_FILE_PATH)

        # Step 3: Left join on stock_code
        df_combined = df_combined.merge(df_sec, on="stock_code", how="left")

        # Step 4: Export
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        df_combined.to_csv(OUTPUT_PATH, index=False)

        print(f"✅ ISINO: {len(df_main)} rows")
        print(f"✅ ISINSEHK (2nd table): {len(df_sehk)} rows")
        print(f"✅ Combined total before SEC: {len(df_main)+len(df_sehk)} rows")
        print(f"✅ Enriched with SEC stock names → {OUTPUT_PATH}")

    except Exception as e:
        print(f"❌ Failed to process: {e}")
        raise


# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    convert_isino()
