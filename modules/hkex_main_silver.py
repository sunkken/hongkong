from pathlib import Path
import pandas as pd

# Load bronze data
df = pd.read_csv("data/bronze/main_bronze.csv")

print(f"📦 Loaded main_bronze.csv rows: {len(df)}")

# --- Step 1: Merge (b) and (c) rows BEFORE joining ---
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

# Count offers by type before dropping
offer_counts = df["offer_location"].value_counts(dropna=False)
print("\n📊 Offer location breakdown before cleanup:")
print(offer_counts)

# Drop merged rows and redundant columns
df = df.drop(rows_to_drop).drop(columns=["funds_raised", "offer_location"])

print(f"\n🧹 Cleaned main_bronze rows: {len(df)} "
      f"(dropped {len(rows_to_drop)} pivoted offer_location rows)")

# --- Step 2: Join with ISINO data ---
df_isino = pd.read_csv("data/bronze/isino_bronze.csv")
print(f"📦 Loaded isino_bronze.csv rows: {len(df_isino)}")

# Ensure stock_code is numeric in both datasets
df["stock_code"] = pd.to_numeric(df["stock_code"], errors="coerce").astype("Int64")
df_isino["stock_code"] = pd.to_numeric(df_isino["stock_code"], errors="coerce").astype("Int64")

# Inner join
df_joined = df.merge(df_isino, on="stock_code", how="inner", suffixes=("", "_isino"))
print(f"🔗 Joined dataset rows: {len(df_joined)} (inner join on stock_code)")
print(f"❌ Rows excluded during join: {len(df) - len(df_joined)}")

# --- Step 3: Save final result ---
Path("data/silver").mkdir(parents=True, exist_ok=True)
df_joined.to_csv("data/silver/main_silver.csv", index=False)

print("\n✅ Saved cleaned and joined data to data/silver/main_silver.csv")
