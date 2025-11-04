from pathlib import Path
import pandas as pd

# --- Step 1: Load datasets ---
df_gem = pd.read_csv("data/bronze/gem_bronze.csv")
df_isino = pd.read_csv("data/bronze/isino_bronze.csv")

print(f"📦 Loaded gem_bronze.csv rows: {len(df_gem)}")
print(f"📦 Loaded isino_bronze.csv rows: {len(df_isino)}")

# --- Step 2: Ensure stock_code is numeric for clean join ---
df_gem["stock_code"] = pd.to_numeric(df_gem["stock_code"], errors="coerce").astype("Int64")
df_isino["stock_code"] = pd.to_numeric(df_isino["stock_code"], errors="coerce").astype("Int64")

# --- Step 3: Perform inner join ---
df_joined = df_gem.merge(df_isino, on="stock_code", how="inner", suffixes=("", "_isino"))

# --- Step 4: Console output on row counts ---
print(f"\n🔗 Joined dataset rows: {len(df_joined)} (inner join on stock_code)")
print(f"❌ Rows excluded during join: {len(df_gem) - len(df_joined)}")

# Optional peek at data
print("\n📊 Joined Data Preview:")
print(df_joined.head())

# --- Step 5: Save to silver layer ---
Path("data/silver").mkdir(parents=True, exist_ok=True)
df_joined.to_csv("data/silver/gem_silver.csv", index=False)

print("\n✅ Saved joined GEM data to data/silver/gem_silver.csv")
