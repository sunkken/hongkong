import sqlite3
from pathlib import Path

# ----------------------------
# Main process
# ----------------------------
def run_sql_file(sql_file: str, db_path: str = "data/hongkong.db") -> bool:
    """
    Execute SQL file against the database (DDL or SELECT queries).
    sql_file : Path to the SQL file
    db_path : Path to SQLite database
    
    Returns True on success, False on failure.
    """
    sql_path = Path(sql_file)
    if not sql_path.exists():
        print(f"❌ SQL file not found: {sql_path}")
        return False

    if not Path(db_path).exists():
        print(f"❌ Database not found: {db_path}")
        return False

    sql_text = sql_path.read_text(encoding="utf-8").strip()
    if not sql_text:
        print(f"❌ SQL file is empty: {sql_path}")
        return False

    try:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(db_path) as conn:
            # Use executescript to allow running files with multiple SQL statements
            conn.executescript(sql_text)
            conn.commit()
        print(f"✅ Query executed: {sql_path.name}")
        return True
    except Exception as e:
        print(f"❌ Failed to execute query: {e}")
        return False


# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    run_sql_file("models/db_init/isin_stock_code_export.sql", "data/hongkong.db")
