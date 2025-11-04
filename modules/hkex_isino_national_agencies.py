
from pathlib import Path
import pandas as pd
import numpy as np

# Silence FutureWarning from pandas replace downcasting
pd.set_option('future.no_silent_downcasting', True)

# Define base directory and file path
base_dir = Path("./data/normalized")
file_path = base_dir / "isino.xlsx"

# Output path
output_path = Path("./data/bronze/isino_national_agencies.csv")
output_path.parent.mkdir(parents=True, exist_ok=True)

def parse_national_agencies(file_path: Path) -> pd.DataFrame:
    df_raw = pd.read_excel(file_path, engine="openpyxl", header=None)

    # --- Find the main table start (first row with >= 3 non-empty cells) ---
    start_row = None
    for i, row in df_raw.iterrows():
        if row.notna().sum() >= 3:
            start_row = i
            break
    if start_row is None:
        raise ValueError("Could not detect the start of the main table.")

    # --- Find the blank row after the main data (scan first column for NaN) ---
    df_after_header = df_raw.iloc[start_row + 1:]  # main data starts after header row
    stop_index = df_after_header[df_after_header.iloc[:, 0].isna()].index.min()
    if pd.isna(stop_index):
        raise ValueError("Could not find the blank row after the main data.")

    # --- Move past the blank row(s), then skip title row and header row ---
    pos = int(stop_index) + 1  # first row after the blank row
    # Skip any additional fully blank rows (defensive)
    while pos < len(df_raw) and df_raw.iloc[pos].isna().all():
        pos += 1

    title_idx = pos                      # Title row
    header_idx = title_idx + 1           # Header row (column names for the second table)
    data_start = header_idx + 1          # Actual data starts here

    if header_idx >= len(df_raw) or data_start >= len(df_raw):
        raise ValueError("Second table header/data region not found where expected.")

    # --- Pull first two columns and stop when column 2 (index 1) is blank ---
    data_block = df_raw.iloc[data_start:, :2].copy()
    first_blank_col2 = data_block[data_block.iloc[:, 1].isna()].index.min()
    if pd.notna(first_blank_col2):
        data_block = data_block.loc[:first_blank_col2 - 1]

    # Clean up: drop rows that are entirely empty across the first two columns
    data_block = data_block.dropna(how="all", subset=[0, 1])

    # Replace newlines; trim whitespace; normalize NaNs that got stringified
    data_block = data_block.replace(r'[\r\n]+', ' ', regex=True)
    for col in [0, 1]:
        data_block[col] = data_block[col].astype(str).str.strip()
        data_block[col] = data_block[col].replace({'nan': np.nan})

    # Assign final column names
    data_block.columns = ["national_agency", "description"]

    return data_block

# Process the file and save
try:
    df_out = parse_national_agencies(file_path)
    df_out.to_csv(output_path, index=False)
    print(f"✅ Parsed {len(df_out)} national agency entries.")
    print(f"📁 Saved national agency definitions to {output_path}")
except Exception as e:
    print(f"⚠️ Failed to parse national agencies: {e}")
