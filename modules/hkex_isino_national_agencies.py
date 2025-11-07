from pathlib import Path
import pandas as pd
import numpy as np


# ----------------------------
# Configuration
# ----------------------------
BASE_DIR = Path("./data/normalized")
FILE_PATH = BASE_DIR / "isino.xlsx"

OUTPUT_PATH = Path("./data/bronze/isino_national_agencies.csv")
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

# Silence FutureWarning from pandas replace downcasting
pd.set_option('future.no_silent_downcasting', True)


# ----------------------------
# Main logic
# ----------------------------
def parse_national_agencies(file_path: Path) -> pd.DataFrame:
    """Extract national agency definitions from the ISINO Excel file."""
    df_raw = pd.read_excel(file_path, engine="openpyxl", header=None)

    # Find start of main table
    start_row = next((i for i, row in df_raw.iterrows() if row.notna().sum() >= 3), None)
    if start_row is None:
        raise ValueError("Could not detect the start of the main table.")

    # Find blank row after main data
    df_after_header = df_raw.iloc[start_row + 1:]
    stop_index = df_after_header[df_after_header.iloc[:, 0].isna()].index.min()
    if pd.isna(stop_index):
        raise ValueError("Could not find the blank row after the main data.")

    # Skip blank rows to locate second table
    pos = int(stop_index) + 1
    while pos < len(df_raw) and df_raw.iloc[pos].isna().all():
        pos += 1

    header_idx = pos + 1
    data_start = header_idx + 1
    if header_idx >= len(df_raw) or data_start >= len(df_raw):
        raise ValueError("Second table header/data region not found where expected.")

    # Pull first two columns and stop when col 2 is blank
    data_block = df_raw.iloc[data_start:, :2].copy()
    first_blank_col2 = data_block[data_block.iloc[:, 1].isna()].index.min()
    if pd.notna(first_blank_col2):
        data_block = data_block.loc[:first_blank_col2 - 1]

    # Clean up
    data_block = data_block.dropna(how="all", subset=[0, 1])
    data_block = data_block.replace(r'[\r\n]+', ' ', regex=True)
    for col in [0, 1]:
        data_block[col] = data_block[col].astype(str).str.strip()
        data_block[col] = data_block[col].replace({'nan': np.nan})

    data_block.columns = ["national_agency", "description"]
    return data_block


def extract_national_agencies():
    """Wrapper: parse and save national agency definitions."""
    df_out = parse_national_agencies(FILE_PATH)
    df_out.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Parsed {len(df_out)} national agency entries → {OUTPUT_PATH}")


# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    extract_national_agencies()
