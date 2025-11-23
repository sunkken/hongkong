import sqlite3
from pathlib import Path

# ----------------------------
# Helper function
# ----------------------------
def export_unique_isins(db_path: str, output_file: str, sql_query: str):
    """
    Extracts unique ISINs from a SQLite database using the provided SQL query
    and saves them to a text file (one ISIN per line).

    db_path : Path to SQLite database
    output_file : Path to save the ISIN list
    sql_query : SQL query that returns a single column 'isin'
    """
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    conn.close()

    # Extract ISINs and skip nulls
    all_isins = [r[0] for r in rows if r[0]]

    # Ensure output directory exists
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    # Save to file
    with open(output_file, "w") as f:
        for isin in sorted(all_isins):
            f.write(f"{isin}\n")

    return len(all_isins)