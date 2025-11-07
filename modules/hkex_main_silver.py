from pathlib import Path
import pandas as pd


# ----------------------------
# Configuration
# ----------------------------
BRONZE_MAIN = Path("data/bronze/main_bronze.csv")
BRONZE_ISINO = Path("data/bronze/isino_bronze.csv")
OUTPUT_PATH = Path("data/silver/main_silver.csv")
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)


# ----------------------------
# Helper functions
# ----------------------------
def load_bronze_data() -> pd.DataFrame:
    """Load the main bronze dataset."""
    df = pd.read_csv(BRONZE_MAIN)
    print(f"📦 Loaded {BRONZE_MAIN.name}: {len(df)} rows")
    return df


def merge_offer_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Merge (b) and (c) offer rows into their corresponding (a) parent rows."""
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
                    elif location == "(c)":
                        df.at[j, "funds_raised_sg"] = funds
                    rows_to_drop.append(i)
                    break
        else:
            df.at[i, "funds_raised_hk"] = funds

    df = df.drop(rows_to_drop).drop(columns=["funds_raised", "offer_location"])
    print(f"🧹 Merged offer rows → {len(df)} remaining")
    return df


def join_with_isino(df: pd.DataFrame) -> pd.DataFrame:
    """Join main bronze data with ISINO bronze data."""
    df_isino = pd.read_csv(BRONZE_ISINO)
    print(f"📦 Loaded {BRONZE_ISINO.name}: {len(df_isino)} rows")

    # Ensure numeric stock_code
    df["stock_code"] = pd.to_numeric(df["stock_code"], errors="coerce").astype("Int64")
    df_isino["stock_code"] = pd.to_numeric(df_isino["stock_code"], errors="coerce").astype("Int64")

    df_joined = df.merge(df_isino, on="stock_code", how="inner", suffixes=("", "_isino"))
    print(f"🔗 Joined datasets: {len(df_joined)} rows (−{len(df) - len(df_joined)} lost)")
    return df_joined


def save_to_silver(df: pd.DataFrame):
    """Save final cleaned and joined dataset to silver layer."""
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Saved to {OUTPUT_PATH}")


# ----------------------------
# Main process
# ----------------------------
def process_main_silver():
    df_main = load_bronze_data()
    df_cleaned = merge_offer_rows(df_main)
    df_joined = join_with_isino(df_cleaned)
    save_to_silver(df_joined)


# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    process_main_silver()
