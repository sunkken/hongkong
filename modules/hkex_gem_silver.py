from pathlib import Path
import pandas as pd


# ----------------------------
# Configuration
# ----------------------------
BRONZE_GEM = Path("data/bronze/gem_bronze.csv")
BRONZE_ISINO = Path("data/bronze/isino_bronze.csv")
OUTPUT_PATH = Path("data/silver/gem_silver.csv")
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)


# ----------------------------
# Helper functions
# ----------------------------
def load_bronze_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load GEM and ISINO bronze datasets."""
    df_gem = pd.read_csv(BRONZE_GEM)
    df_isino = pd.read_csv(BRONZE_ISINO)
    print(f"📦 Loaded {BRONZE_GEM.name}: {len(df_gem)} rows")
    print(f"📦 Loaded {BRONZE_ISINO.name}: {len(df_isino)} rows")
    return df_gem, df_isino


def prepare_for_join(df_gem: pd.DataFrame, df_isino: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Ensure stock_code columns are numeric in both datasets."""
    df_gem["stock_code"] = pd.to_numeric(df_gem["stock_code"], errors="coerce").astype("Int64")
    df_isino["stock_code"] = pd.to_numeric(df_isino["stock_code"], errors="coerce").astype("Int64")
    return df_gem, df_isino


def join_datasets(df_gem: pd.DataFrame, df_isino: pd.DataFrame) -> pd.DataFrame:
    """Inner join GEM bronze with ISINO bronze on stock_code."""
    df_joined = df_gem.merge(df_isino, on="stock_code", how="inner", suffixes=("", "_isino"))
    print(f"🔗 Joined datasets: {len(df_joined)} rows (−{len(df_gem) - len(df_joined)} lost)")
    return df_joined


def save_to_silver(df: pd.DataFrame):
    """Save joined dataset to silver layer."""
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Saved to {OUTPUT_PATH}")


# ----------------------------
# Main process
# ----------------------------
def process_gem_silver():
    df_gem, df_isino = load_bronze_data()
    df_gem, df_isino = prepare_for_join(df_gem, df_isino)
    df_joined = join_datasets(df_gem, df_isino)
    save_to_silver(df_joined)


# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    process_gem_silver()
