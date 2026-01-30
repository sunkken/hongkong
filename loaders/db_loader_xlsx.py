import sqlite3
import pandas as pd
from pathlib import Path

# ----------------------------
# Main process
# ----------------------------
def xlsx_loader(xlsx_file: str, table_name: str, db_path: str):
    """
    xlsx_file : Path to the XLSX file
    table_name : SQLite table name
    db_path : Path to SQLite database
    """
    xlsx_path = Path(xlsx_file)
    if not xlsx_path.exists():
        print(f"❌ XLSX file not found: {xlsx_path}")
        return 0

    df = pd.read_excel(xlsx_path)
    print(f"📦 Loaded XLSX: {xlsx_path.name} ({len(df)} rows)")

    # ----------------------------
    # Save to SQLite
    # ----------------------------
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)

    return len(df)


# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    xlsx_loader("data/processed/hkex_dataset_hkex_document_exists_20251223.xlsx", "hkex_auditor_reports_classified", "data/hongkong.db")
