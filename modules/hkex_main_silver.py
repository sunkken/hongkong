from pathlib import Path
import pandas as pd
import duckdb

# ----------------------------
# Configuration
# ----------------------------
BRONZE_MAIN = Path("data/bronze/main_bronze.parquet")
BRONZE_ISINO = Path("data/bronze/isino_bronze.parquet")

OUTPUT_PARQUET = Path("data/silver/main_silver.parquet")
OUTPUT_CSV = Path("data/silver/main_silver.csv")

DB_PATH = Path("data/database/hkex.duckdb")
TABLE_NAME = "main_silver"

OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)


# ----------------------------
# Saving helper (clean + universal)
# ----------------------------
def save_output(df: pd.DataFrame, parquet_path: Path, csv_path: Path, db_path: Path, table: str):
    """Save to Parquet, CSV, and DuckDB in a clean, consistent way."""
    # Parquet
    df.to_parquet(parquet_path, index=False)
    print(f"💾 Saved Parquet → {parquet_path}")

    # CSV
    df.to_csv(csv_path, index=False)
    print(f"💾 Saved CSV → {csv_path}")

    # DuckDB
    conn = duckdb.connect(str(db_path))
    conn.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM df")
    conn.close()
    print(f"💾 Saved DuckDB table '{table}' → {db_path}")


# ----------------------------
# ETL processing
# ----------------------------
def load_bronze_data() -> pd.DataFrame:
    df = pd.read_parquet(BRONZE_MAIN)
    print(f"📦 Loaded {BRONZE_MAIN.name}: {len(df)} rows")
    return df


def merge_offer_rows(df: pd.DataFrame) -> pd.DataFrame:
    df["funds_raised_hk"] = None
    df["funds_raised_intl"] = None
    df["funds_raised_sg"] = None

    rows_to_drop = []

    for i in range(len(df)):
        location = str(df.loc[i, "offer_location"]).strip()
        funds = df.loc[i, "funds_raised"]

        if location in ("(b)", "(c)"):
            for j in range(i - 1, -1, -1):
                if df.loc[j, "stock_code"] == df.loc[i, "stock_code"]:
                    if location == "(b)":
                        df.at[j, "funds_raised_intl"] = funds
                    else:
                        df.at[j, "funds_raised_sg"] = funds
                    rows_to_drop.append(i)
                    break
        else:
            df.at[i, "funds_raised_hk"] = funds

    df = df.drop(rows_to_drop).drop(columns=["funds_raised", "offer_location"])
    print(f"🧹 Merged offer rows → {len(df)} remaining")
    return df


def join_with_isino(df: pd.DataFrame) -> pd.DataFrame:
    df_isino = pd.read_parquet(BRONZE_ISINO)
    print(f"📦 Loaded {BRONZE_ISINO.name}: {len(df_isino)} rows")

    df["stock_code"] = pd.to_numeric(df["stock_code"], errors="coerce").astype("Int64")
    df_isino["stock_code"] = pd.to_numeric(df_isino["stock_code"], errors="coerce").astype("Int64")

    df_joined = df.merge(df_isino, on="stock_code", how="inner")
    print(f"🔗 Joined: {len(df_joined)} rows")
    return df_joined


# ----------------------------
# Main process
# ----------------------------
def process_main_silver():
    df_main = load_bronze_data()
    df_cleaned = merge_offer_rows(df_main)
    df_joined = join_with_isino(df_cleaned)

    save_output(
        df_joined,
        OUTPUT_PARQUET,
        OUTPUT_CSV,
        DB_PATH,
        TABLE_NAME
    )


if __name__ == "__main__":
    process_main_silver()
