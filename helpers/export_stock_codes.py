
import sqlite3
from pathlib import Path

# ----------------------------
# Helper function
# ----------------------------
def export_unique_stock_codes(db_path: str, output_file: str, sql_query: str):
    """
    Extracts unique stock codes from a SQLite database using the provided SQL query
    and saves them to a text file (one stock code per line).

    db_path : Path to SQLite database
    output_file : Path to save the stock code list
    sql_query : SQL query that returns a single column 'stock_code'
    """
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    conn.close()

    # Extract stock codes and skip nulls
    all_stock_codes = [r[0] for r in rows if r[0]]

    # Ensure output directory exists
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    # Save to file
    with open(output_file, "w") as f:
        for stock_code in sorted(all_stock_codes):
            f.write(f"{stock_code}\n")

    return len(all_stock_codes)