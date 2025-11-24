#!/usr/bin/env python
"""
wrds_fields.py

Print all column names for a WRDS table using a direct Postgres connection.
"""

import os
import sys
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# ----------------------------
# Load WRDS credentials from .env
# ----------------------------
load_dotenv()

WRDS_USER = os.getenv("WRDS_USER")
WRDS_PASS = os.getenv("WRDS_PASS")
WRDS_HOST = "wrds-pgdata.wharton.upenn.edu"
WRDS_PORT = 9737
WRDS_DB = "wrds"

if not WRDS_USER or not WRDS_PASS:
    print("❌ Missing WRDS_USER or WRDS_PASS in .env")
    sys.exit(1)

# ----------------------------
# Parse command-line arguments
# ----------------------------
if len(sys.argv) < 2:
    print("Usage: python wrds_fields.py <table_name> [schema]")
    print("Example: python wrds_fields.py g_funda comp")
    sys.exit(1)

table_name = sys.argv[1]
schema = sys.argv[2] if len(sys.argv) > 2 else "comp"

# ----------------------------
# Connect to WRDS Postgres
# ----------------------------
conn = psycopg2.connect(
    dbname=WRDS_DB,
    user=WRDS_USER,
    password=WRDS_PASS,
    host=WRDS_HOST,
    port=WRDS_PORT,
    sslmode="require"
)

# ----------------------------
# Query information_schema.columns
# ----------------------------
query = f"""
SELECT column_name
FROM information_schema.columns
WHERE table_schema = '{schema}'
  AND table_name = '{table_name}'
ORDER BY ordinal_position;
"""

df = pd.read_sql(query, conn)
conn.close()

# ----------------------------
# Print field names
# ----------------------------
print(f"\nColumns in {schema}.{table_name}:\n")
for col in df['column_name']:
    print(col)
print(f"\n✅ Total columns: {len(df)}\n")
