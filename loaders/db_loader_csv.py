import sqlite3
import pandas as pd
from pathlib import Path

# ----------------------------
# Main process
# ----------------------------
def csv_loader(csv_file: str, table_name: str, db_path: str):
    """
    csv_file : Path to the CSV file (can include folder, e.g., data/silver/file.csv)
    table_name : SQLite table name
    db_path : Path to SQLite database
    """
    csv_path = Path(csv_file)
    if not csv_path.exists():
        print(f"❌ CSV file not found: {csv_path}")
        return 0

    df = pd.read_csv(csv_path)
    print(f"📦 Loaded CSV: {csv_path.name} ({len(df)} rows)")

    # ----------------------------
    # Save to SQLite
    # ----------------------------
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"✅ Saved {len(df)} rows to table '{table_name}' in {db_path}")

    return len(df)


# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    csv_loader("data/silver/gem_silver.csv", "gem_silver", "data/hongkong.db")
