from pathlib import Path
import pandas as pd
import numpy as np

# Silence FutureWarning from pandas replace downcasting
pd.set_option('future.no_silent_downcasting', True)

# Define base directory
base_dir = Path("./data/normalized")

# Define output columns
COLUMNS = [
    "source_file", "stock_code", "company", "prospectus_date", "listing_date",
    "sponsors", "reporting_accountant", "valuers", "funds_raised",
    "subscription_price", "offer_location"
]

# Initialize empty DataFrame and skip flag
combined = pd.DataFrame(columns=COLUMNS)
any_skipped = False

# Function to clean and transform each file
def clean_and_transform(file_path):
    df = pd.read_excel(file_path, engine="openpyxl", header=None, skiprows=1)
    df = df.iloc[:, :11]
    df = df.iloc[1:]  # Trim away the first row

    # Flatten multi-line cells
    df = df.replace(r'[\r\n]+', ' ', regex=True)

    # Normalize excessive spacing in company names only
    if df.shape[1] > 2:
        df.iloc[:, 2] = df.iloc[:, 2].astype(str).str.replace(r' {2,}', ' ', regex=True)

    # Drop rows where stock_code is missing
    df = df[df.iloc[:, 1].notna()]

    empty_row_index = df[df.isnull().all(axis=1)].index.min()
    if pd.notna(empty_row_index):
        df = df.iloc[:empty_row_index]

    df = df.replace(r'^\"+$', np.nan, regex=True).infer_objects(copy=False)

    if df.shape[1] >= 11:
        for i in range(len(df)):
            offer_type = str(df.iloc[i, -1]).strip()
            prev_offer = str(df.iloc[i - 1, -1]).strip() if i > 0 else ""

            if offer_type in ("(b)", "(c)") and i > 0:
                if prev_offer == "(a)":
                    df.iat[i, 1] = df.iat[i - 1, 1]
                elif offer_type == "(c)" and prev_offer == "(b)" and i > 1:
                    df.iat[i, 1] = df.iat[i - 2, 1]

    if df.shape[1] > 1:
        df = df.iloc[:, 1:]

    df.insert(0, "source_file", file_path.name)

    while df.shape[1] < len(COLUMNS):
        df[df.shape[1]] = np.nan

    df.columns = COLUMNS

    # Convert stock_code to numeric
    df["stock_code"] = pd.to_numeric(df["stock_code"], errors="coerce")
    df = df.dropna(subset=["stock_code"])
    df["stock_code"] = df["stock_code"].astype(int)

    return df

# Loop through all matching files
files = sorted(base_dir.glob("Main_*.xlsx"))
print(f"🔍 Found {len(files)} files to process.")

for file in files:
    try:
        df_cleaned = clean_and_transform(file)
        combined = pd.concat([combined, df_cleaned], ignore_index=True)
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
Path("./data/bronze").mkdir(parents=True, exist_ok=True)

# Save to CSV
combined.to_csv("data/bronze/main_bronze.csv", index=False)
print("\n📁 Saved combined data to data/bronze/main_bronze.csv")
