from pathlib import Path
import pandas as pd


# ----------------------------
# Configuration
# ----------------------------
BASE_DIR = Path("./data/normalized")
FILE_PATH = BASE_DIR / "isino.xlsx"

OUTPUT_PATH = Path("./data/bronze/isino_stock_types.csv")
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)


# ----------------------------
# Main logic
# ----------------------------
def extract_stock_types():
    """Extract stock type definitions from the ISINO Excel file."""
    df_raw = pd.read_excel(FILE_PATH, engine="openpyxl", header=None)

    # Row 17 (index 16) contains the stock type definitions
    stock_type_row = str(df_raw.iloc[16, 0]).strip()

    # Take only the content between the first '(' and the last ')'
    start = stock_type_row.find("(")
    end = stock_type_row.rfind(")")
    if start != -1 and end != -1 and end > start:
        stock_type_row = stock_type_row[start + 1 : end]

    # Normalize en dash to hyphen for consistent splitting
    stock_type_row = stock_type_row.replace(" – ", " - ")

    # Split into entries
    entries = [entry.strip() for entry in stock_type_row.split(";") if entry.strip()]

    # Parse into tuples (stock_type, description)
    parsed = []
    for entry in entries:
        if " - " in entry:
            stock_type, description = entry.split(" - ", 1)
            parsed.append((stock_type.strip(), description.strip()))

    # Create DataFrame
    df_types = pd.DataFrame(parsed, columns=["stock_type", "description"])

    # Save to CSV
    df_types.to_csv(OUTPUT_PATH, index=False)

    print(f"✅ Parsed {len(df_types)} stock type entries → {OUTPUT_PATH}")


# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    extract_stock_types()
