from pathlib import Path
import pandas as pd

# Load bronze data
df = pd.read_csv("data/bronze/main_bronze.csv")

# Create new columns
df["funds_raised_hk"] = None
df["funds_raised_intl"] = None
df["funds_raised_sg"] = None

# Track rows to drop
rows_to_drop = []

# Merge (b) and (c) rows into previous (a) or blank row
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

# Drop merged rows and original columns
df = df.drop(rows_to_drop).drop(columns=["funds_raised", "offer_location"])

# Ensure output directory exists
Path("data/silver").mkdir(parents=True, exist_ok=True)

# Save to CSV
df.to_csv("data/silver/main_silver.csv", index=False)

print("✅ Saved cleaned data to data/silver/main_silver.csv")