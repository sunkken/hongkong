
import sqlite3
from pathlib import Path

# ----------------------------
# Helper function
# ----------------------------
def export_stock_ids(db_path: str, output_file: str, sql_query: str):
    """
    Extracts all stockIds from a SQLite database using the provided SQL query
    and saves them to a text file (one stockId per line).

    db_path : Path to SQLite database
    output_file : Path to save the stockId list
    sql_query : SQL query that returns a single column 'stockId'
    """
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    conn.close()

    # Extract stockIds and skip nulls
    all_ids = [r[0] for r in rows if r[0]]

    # Ensure output directory exists
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    # Save to file
    with open(output_file, "w") as f:
        for sid in sorted(all_ids):
            f.write(f"{sid}\n")

    return len(all_ids)

# ----------------------------
# Example usage
# ----------------------------
if __name__ == "__main__":
    DB_PATH = "data/hongkong.db"
    OUTPUT_FILE = "data/lists/stock_ids.txt"
    SQL_QUERY = "SELECT stockId FROM stock_code_to_id;"  # adjust if needed

    print(f"\n🚀 Exporting stockIds from {DB_PATH}...\n")
    count = export_stock_ids(DB_PATH, OUTPUT_FILE, SQL_QUERY)
    print(f"✅ Exported {count} stockIds to {OUTPUT_FILE}")