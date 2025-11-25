import sqlite3
from pathlib import Path

# ----------------------------
# Helper function
# ----------------------------
def export_unique_stock_codes(db_path: str, output_file: str, sql_query: str):
    """
    Extracts unique stock codes from a SQLite database using the provided SQL query
    and saves them to a text file (one stock code per line).
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    conn.close()

    stock_codes = [r[0] for r in rows if r[0]]

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w") as f:
        for code in sorted(stock_codes):
            f.write(f"{code}\n")

    return len(stock_codes)


# ----------------------------
# New: two separate exports
# ----------------------------
def export_main_and_gem(db_path: str, output_dir: str):
    output_dir = Path(output_dir)

    # MAIN board
    sql_main = "SELECT stock_code FROM hkex_main;"
    main_file = output_dir / "stock_code_list_main.txt"
    main_count = export_unique_stock_codes(db_path, main_file, sql_main)

    # GEM board
    sql_gem = "SELECT stock_code FROM hkex_gem;"
    gem_file = output_dir / "stock_code_list_gem.txt"
    gem_count = export_unique_stock_codes(db_path, gem_file, sql_gem)

    print(f"MAIN: {main_count} codes exported to {main_file}")
    print(f"GEM : {gem_count} codes exported to {gem_file}")


if __name__ == "__main__":
    # Example:
    export_main_and_gem(
        db_path="data/hongkong.db",
        output_dir="data/"
    )
