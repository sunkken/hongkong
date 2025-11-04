
from pathlib import Path
import pandas as pd
import numpy as np

# Silence FutureWarning from pandas replace downcasting
pd.set_option('future.no_silent_downcasting', True)

# Define base directory and file path
base_dir = Path("./data/normalized")
file_path = base_dir / "isino.xlsx"

# Define output columns
COLUMNS = [
    "company", "isin_code", "stock_code", "stock_type",
    "place_of_incorporation", "national_agency"
]

# Initialize empty DataFrame
combined = pd.DataFrame(columns=COLUMNS)

# Function to clean and transform the file
def clean_and_transform(file_path):
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
    df_data = df_data.replace(r'[\r\n]+', ' ', regex=True)
    df_data = df_data.replace(r'^\"+$', np.nan, regex=True).infer_objects(copy=False)

    # Assign column names
    df_data.columns = COLUMNS

    # Convert stock_code to numeric
    df_data["stock_code"] = pd.to_numeric(df_data["stock_code"], errors="coerce")
    df_data = df_data.dropna(subset=["stock_code"])
    df_data["stock_code"] = df_data["stock_code"].astype(int)

    return df_data

# Process the file
try:
    df_cleaned = clean_and_transform(file_path)
    combined = pd.concat([combined, df_cleaned], ignore_index=True)
    print("✅ File processed successfully.")
except Exception as e:
    print(f"⚠️ Failed to process file: {e}")

# Output results
print("\n📊 Data Preview:")
print(combined.head())
print(f"📈 Total rows processed: {len(combined)}")

# Ensure output directory exists
output_path = Path("./data/bronze/isino_bronze.csv")
output_path.parent.mkdir(parents=True, exist_ok=True)

# Save to CSV
combined.to_csv(output_path, index=False)
print(f"\n📁 Saved cleaned data to {output_path}")
