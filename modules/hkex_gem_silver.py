from pathlib import Path
import pandas as pd
import duckdb

# ----------------------------
# Configuration
# ----------------------------
BRONZE_GEM = Path("data/bronze/gem_bronze.parquet")
BRONZE_ISINO = Path("data/bronze/isino_bronze.parquet")

OUTPUT_PARQUET = Path("data/silver/gem_silver.parquet")
OUTPUT_CSV = Path("data/silver/gem_silver.csv")

DB_PATH = Path("data/database/hkex.duckdb")
TABLE_NAME = "gem_silver"

OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)


# ----------------------------
# Universal saving helper
# ----------------------------
def save_output(df: pd.DataFrame, parquet_path: Path, csv_path: Path, db_path: Path, table: str):
    """Save to Parquet, CSV, and DuckDB."""
    df.to_parquet(parquet_path, index=False)
    print(f"💾 Saved Parquet → {parquet_path}")

    df.to_csv(csv_path, index=False)
    print(f"💾 Saved CSV → {csv_path}")

    conn = duckdb.connect(str(db_path))
    conn.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM df")
    conn.close()
    print(f"💾 Saved DuckDB table '{table}' → {db_path}")


# ----------------------------
# ETL processing
# ----------------------------
def load_bronze_data():
    df_gem = pd.read_parquet(BRONZE_GEM)
    df_isino = pd.read_parquet(BRONZE_ISINO)

    print(f"📦 Loaded {BRONZE_GEM.name}: {len(df_gem)} rows")
    print(f"📦 Loaded {BRONZE_ISINO.name}: {len(df_isino)} rows")

    return df_gem, df_isino


def prepare_for_join(df_gem, df_isino):
    df_gem["stock_code"] = pd.to_numeric(df_gem["stock_code"], errors="coerce").astype("Int64")
    df_isino["stock_code"] = pd.to_numeric(df_isino["stock_code"], errors="coerce").astype("Int64")
    return df_gem, df_isino


def join_datasets(df_gem, df_isino):
    df_joined = df_gem.merge(df_isino, on="stock_code", how="inner")
    print(f"🔗 Joined: {len(df_joined)} rows")
    return df_joined


# ----------------------------
# Main
# ----------------------------
def process_gem_silver():
    df_gem, df_isino = load_bronze_data()
    df_gem, df_isino = prepare_for_join(df_gem, df_isino)
    df_joined = join_datasets(df_gem, df_isino)

    save_output(
        df_joined,
        OUTPUT_PARQUET,
        OUTPUT_CSV,
        DB_PATH,
        TABLE_NAME
    )


if __name__ == "__main__":
    process_gem_silver()
