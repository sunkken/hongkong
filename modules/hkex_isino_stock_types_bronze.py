
from pathlib import Path
import pandas as pd

# Define base directory and file path
base_dir = Path("./data/normalized")
file_path = base_dir / "isino.xlsx"

# Define output path
output_path = Path("./data/bronze/isino_stock_types.csv")
output_path.parent.mkdir(parents=True, exist_ok=True)

# Read the specific row containing stock type definitions
df_raw = pd.read_excel(file_path, engine="openpyxl", header=None)
stock_type_row = str(df_raw.iloc[16, 0]).strip()  # Row 17 is index 16

# 🔧 NEW: Take only the content between the first '(' and the last ')'
start = stock_type_row.find("(")
end = stock_type_row.rfind(")")
if start != -1 and end != -1 and end > start:
    stock_type_row = stock_type_row[start + 1 : end]

# 🔧 (Optional but useful): normalize en dash to hyphen so splitting works uniformly
stock_type_row = stock_type_row.replace(" – ", " - ")

# Split into individual entries
entries = [entry.strip() for entry in stock_type_row.split(";") if entry.strip()]

# Split each entry into stock_type and description
parsed = []
for entry in entries:
    if " - " in entry:
        stock_type, description = entry.split(" - ", 1)
        parsed.append((stock_type.strip(), description.strip()))

# Create DataFrame
df_types = pd.DataFrame(parsed, columns=["stock_type", "description"])

# Save to CSV
df_types.to_csv(output_path, index=False)

# Console output
print(f"✅ Parsed {len(df_types)} stock type entries.")
print(f"📁 Saved stock type definitions to {output_path}")
